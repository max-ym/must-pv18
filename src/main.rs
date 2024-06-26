use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::fmt::Display;
use std::time::{Duration, SystemTime};
use strum::IntoEnumIterator;
use strum_macros::{EnumIter, FromRepr};
use tokio::time::sleep;

use log::*;
use rodbus::client::{Channel, RequestParam, WriteMultiple};
use rodbus::{
    AddressRange, DataBits, DecodeLevel, FlowControl, Parity, RequestError, RetryStrategy,
    SerialSettings, StopBits, UnitId,
};

const BAUD: u32 = 19200;
const PATH: &str = "/dev/ttyUSB0";
const SLAVE: UnitId = UnitId { value: 4 };
const TIMEOUT: Duration = Duration::from_millis(200);
const AFTER_IO_HALT: Duration = Duration::from_millis((512.0 / BAUD as f32 * 1000.0) as u64);

static mut OVERNIGHT_CHARGING_START: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;
static mut OVERNIGHT_CHARGING_END: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

fn is_overnight_charging_init() -> bool {
    let start = unsafe { OVERNIGHT_CHARGING_START };
    let end = unsafe { OVERNIGHT_CHARGING_END };

    start != end
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(LevelFilter::Debug)
        .chain(std::io::stdout())
        .apply()
        .unwrap();
    debug!("AFTER_READ_HALT {} ms", AFTER_IO_HALT.as_millis());

    struct MyRetryStrategy;
    impl RetryStrategy for MyRetryStrategy {
        fn reset(&mut self) {
            debug!("RetryStrategy - Resetting connection");
        }

        fn after_failed_connect(&mut self) -> Duration {
            debug!("RetryStrategy - Failed to connect");
            TIMEOUT
        }

        fn after_disconnect(&mut self) -> Duration {
            debug!("RetryStrategy - Disconnected");
            TIMEOUT
        }
    }

    let mut channel = rodbus::client::spawn_rtu_client_task(
        PATH,
        SerialSettings {
            baud_rate: BAUD,
            data_bits: DataBits::Eight,
            flow_control: FlowControl::None,
            stop_bits: StopBits::One,
            parity: Parity::None,
        },
        1,
        Box::new(MyRetryStrategy),
        DecodeLevel::default(),
        None,
    );
    channel.enable().await?;

    // Setup overnight charging timer.
    // It will enable charging at 3 AM and disable after specified time.
    let overnight_charging = std::env::args().position(|v| v == "overnight_charging");
    let overnight_charging = overnight_charging.map(|pos| {
        let mut args = std::env::args().skip(pos + 1);
        let duration_minutes = args
            .next()
            .expect("overnight charge duration is not provided")
            .parse::<u32>()
            .expect("overnight charge duration is not a number");
        Duration::from_secs(duration_minutes as u64 * 60)
    });
    if let Some(overnight_duration) = overnight_charging {
        let start = chrono::Local::now()
            .date_naive()
            .and_hms_opt(3, 14, 15)
            .unwrap();
        info!("Overnight charging will start at {}", start);
        let start_utc = start.and_local_timezone(chrono::Local).unwrap().into();
        unsafe { OVERNIGHT_CHARGING_START = start_utc };

        let end = start + overnight_duration;
        info!("Overnight charging will end at {}", end);
        let end_utc = end.and_local_timezone(chrono::Local).unwrap().into();
        unsafe { OVERNIGHT_CHARGING_END = end_utc };
    }

    if std::env::args().any(|v| v == "db") {
        return snapshot_acc(&connect_db().await?, &mut channel).await;
    } else if std::env::args().any(|v| v == "load") {
        return snapshot_load(&connect_db().await?, &mut channel).await;
    } else if std::env::args().any(|v| v == "track") {
        let cli = connect_db().await?;
        let mut last_acc = SystemTime::UNIX_EPOCH;
        loop {
            if last_acc.elapsed().unwrap() >= Duration::from_secs(60 * 15) {
                snapshot_acc(&cli, &mut channel).await?;
                last_acc = SystemTime::now();
            }

            snapshot_load(&cli, &mut channel).await?;
            charger_update(&mut channel).await;
            sleep(Duration::from_secs(2)).await;
        }
    } else if std::env::args().any(|v| v == "track_with_grid") {
        let cli = connect_db().await?;
        let mut last_acc = SystemTime::UNIX_EPOCH;
        loop {
            if last_acc.elapsed().unwrap() >= Duration::from_secs(60 * 15) {
                snapshot_acc(&cli, &mut channel).await?;
                last_acc = SystemTime::now();
            }

            snapshot_grid(&connect_db().await?, &mut channel).await?;
            snapshot_load(&cli, &mut channel).await?;
            charger_update(&mut channel).await;
            sleep(Duration::from_secs(5)).await;
        }
    } else if std::env::args().any(|v| v == "volt_cal_coef") {
        let val = Reg16::BatteryVoltageCalibrationCoef
            .read(&mut channel)
            .await?;
        println!("{val}");
        let new_val: i16 = 16384 - 200;
        Reg16::BatteryVoltageCalibrationCoef
            .write(new_val as u16, &mut channel)
            .await?;
        sleep(Duration::from_secs(1)).await;
        let val = Reg16::BatteryVoltageCalibrationCoef
            .read(&mut channel)
            .await?;
        println!("New {val}");
        return Ok(());
    }

    for group in Groups16::new(Reg16::iter().map(|v| v as u16)).expect("Reg16 enum is not empty") {
        let result = read_many(group.clone(), &mut channel).await;
        match result {
            Ok(val) => {
                for (index, val) in val.into_iter().enumerate() {
                    let addr = group.start + index as u16;
                    println!(
                        "{}",
                        Reg16Val {
                            val,
                            var: Reg16::from_repr(addr)
                                .expect("range was created from valid values"),
                        }
                    );
                }
            }
            Err(e) => println!("Error reading {:?}: {}", group, e),
        }
    }
    for group in Groups32::new(Reg32::iter().map(|v| v as u16)).expect("Reg32 enum is not empty") {
        let result = read_many(group.clone(), &mut channel).await;
        match result {
            Ok(val) => {
                for (index, val) in val.chunks(2).enumerate() {
                    let addr = group.start + index as u16 * 2;
                    println!(
                        "{}",
                        Reg32Val {
                            val: [val[0], val[1]],
                            var: Reg32::from_repr(addr)
                                .expect("range was created from valid values"),
                        }
                    );
                }
            }
            Err(e) => println!("Error reading {:?}: {}", group, e),
        }
    }

    Ok(())
}

// Update charger state according to OVERNIGHT_CHARGING_END
async fn charger_update(channel: &mut Channel) {
    use ChargerSourcePriority::*;

    if !is_overnight_charging_init() {
        return;
    }

    let start = unsafe { OVERNIGHT_CHARGING_START };
    let end = unsafe { OVERNIGHT_CHARGING_END };
    let now = chrono::Local::now();

    let charger_source_priority = Reg16::ChargetSourcePriority.read(channel).await.unwrap();
    debug!("Current charger source priority: {charger_source_priority}");
    let expected_source_priority = if now < start || now > end {
        OnlySolar
    } else {
        SolarOrElseGrid
    };
    debug!("Expected charger source priority: {expected_source_priority}");

    if charger_source_priority.val != expected_source_priority.into_value16() {
        Reg16::ChargetSourcePriority
            .write(expected_source_priority, channel)
            .await
            .unwrap();
    }
}

async fn connect_db() -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let tls = native_tls::TlsConnector::builder()
        .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
        .danger_accept_invalid_certs(true)
        .build()
        .expect("check the parameters above, those should pass!");
    let tls = postgres_native_tls::MakeTlsConnector::new(tls);

    let (cli, conn) = tokio_postgres::Config::new()
        .user("must_solar")
        .password("must_solar")
        .host("localhost")
        .port(5432)
        .dbname("must_solar")
        .connect_timeout(Duration::from_secs(5))
        .connect(tls)
        .await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {e}");
        }
    });

    Ok(cli)
}

async fn snapshot_acc(cli: &tokio_postgres::Client, channel: &mut Channel) -> Result<(), Error> {
    debug!("Making snapshot to store to DB");

    let pv_pow = Reg32::AccumulatedPvPower.read(channel).await?;
    info!("{pv_pow}");
    let load_pow = Reg32::AccumulatedLoadPower.read(channel).await?;
    info!("{load_pow}");

    let sql = format!(
        "INSERT INTO accumulated_power (pv, load) VALUES ({}, {})",
        pv_pow.val(),
        load_pow.val()
    );
    let count = cli.simple_query(&sql).await?.len();
    if count == 1 {
        debug!("Snapshot stored to DB");
    } else {
        warn!("Unexpected statement result: {count}");
    }
    Ok(())
}

async fn snapshot_grid(cli: &tokio_postgres::Client, channel: &mut Channel) -> Result<(), Error> {
    debug!("Making grid snapshot to store to DB");

    let grid_voltage = Reg16::GridVoltage.read(channel).await?;
    let grid_freq = Reg16::GridFrequency.read(channel).await?;

    let sql = format!(
        "INSERT INTO grid_stat (voltage, freq) VALUES ({}, {})",
        f32::from(grid_voltage.val) / 10.0,
        f32::from(grid_freq.val) / 100.0
    );
    let count = cli.simple_query(&sql).await?.len();
    if count == 1 {
        debug!("Grid snapshot stored to DB");
    } else {
        warn!("Unexpected insert count: {count}");
    }

    Ok(())
}

async fn snapshot_load(cli: &tokio_postgres::Client, channel: &mut Channel) -> Result<(), Error> {
    debug!("Making load snapshot to store to DB");

    let regs_to_read = [
        Reg16::PvVoltage,
        Reg16::PvBatteryVoltage,
        Reg16::PvChargerCurrent,
        Reg16::PvChargerPower,
        Reg16::LoadCurrent,
        Reg16::PInverter,
        Reg16::SInverter,
        Reg16::BatteryPower,
        Reg16::BatteryCurrent,
    ];
    // Hardcode to make only two requests to controller to save time.
    let range1 = Reg16::PvVoltage as u16..(Reg16::PvChargerPower as u16 + 1);
    let range2 = Reg16::LoadCurrent as u16..(Reg16::SInverter as u16 + 1);
    let range3 = Reg16::BatteryPower as u16..(Reg16::BatteryCurrent as u16 + 1);

    let ranges = [&range1, &range2, &range3];

    // Check that we've made no mistake
    for reg in regs_to_read {
        assert!(ranges.iter().any(|r| r.contains(&(reg as u16))));
    }

    let mut results = Vec::with_capacity(ranges.iter().map(|r| r.len()).sum());

    debug!("reading regs");
    for grp in ranges {
        debug!("{:?}", grp);
        let result = read_many(grp.clone(), channel).await;
        match result {
            Ok(val) => {
                for (index, val) in val.into_iter().enumerate() {
                    let addr = grp.start + index as u16;
                    results.push(Reg16Val {
                        val,
                        var: Reg16::from_repr(addr).expect("range was created from valid values"),
                    });
                }
            }
            Err(e) => println!("Error reading {:?}: {}", grp, e),
        }
    }

    macro_rules! find {
        ($var:ident, f01) => {
            results
                .iter()
                .find(|v| v.var == Reg16::$var)
                .map(|v| f32::from(v.val))
                .unwrap()
                / 10.0
        };
        ($var:ident) => {
            results
                .iter()
                .find(|v| v.var == Reg16::$var)
                .map(|v| v.val)
                .unwrap()
        };
        ($var:ident, ival) => {
            results
                .iter()
                .find(|v| v.var == Reg16::$var)
                .map(|v| v.val as i16)
                .unwrap()
        };
        ($var:ident, -ival) => {
            results
                .iter()
                .find(|v| v.var == Reg16::$var)
                .map(|v| -(v.val as i16))
                .unwrap()
        };
    }

    debug!("Preparing SQL");
    let sql = format!(
        "INSERT INTO current_load (pv_voltage, pv_charger_current, pv_charger_power, battery_voltage, battery_current, battery_power, load_current, p_inverter, s_inverter) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})",
        find!(PvVoltage, f01),
        find!(PvChargerCurrent, f01),
        find!(PvChargerPower),
        find!(PvBatteryVoltage, f01),
        find!(BatteryCurrent, -ival),
        find!(BatteryPower, -ival),
        find!(LoadCurrent, f01),
        find!(PInverter, ival),
        find!(SInverter),
    );
    debug!("{sql}");
    cli.simple_query(&sql).await?;

    debug!("Stored load snapshot to DB");
    Ok(())
}

struct Groups16<I: Iterator<Item = u16>> {
    iter: I,
    next_start: u16,
}

impl<I: Iterator<Item = u16>> Groups16<I> {
    fn new(mut iter: I) -> Option<Self> {
        let next_start = iter.next()?;
        Some(Self { iter, next_start })
    }
}

impl<I: Iterator<Item = u16>> Iterator for Groups16<I> {
    type Item = std::ops::Range<u16>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.next_start;
        let mut end = start;

        let mut executed = false;
        for next in self.iter.by_ref() {
            executed = true;
            if next == end + 1 {
                end = next;
            } else {
                self.next_start = next;
                break;
            }
        }

        if executed {
            Some(start..end + 1)
        } else {
            None // no iteration was made → end of iterator
        }
    }
}

struct Groups32<I: Iterator<Item = u16>> {
    iter: I,
    next_start: u16,
}

impl<I: Iterator<Item = u16>> Groups32<I> {
    fn new(mut iter: I) -> Option<Self> {
        let next_start = iter.next()?;
        Some(Self { iter, next_start })
    }
}

impl<I: Iterator<Item = u16>> Iterator for Groups32<I> {
    type Item = std::ops::Range<u16>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.next_start;
        let mut end = start;

        let mut executed = false;
        for next in self.iter.by_ref() {
            executed = true;

            if next == end + 2 {
                end = next;
            } else {
                self.next_start = next;
                break;
            }
        }

        if executed {
            Some(start..end + 2)
        } else {
            None // no iteration was made → end of iterator
        }
    }
}

#[derive(Debug, Clone, Copy, EnumIter, FromRepr, PartialEq, Eq)]
#[repr(u16)]
enum Reg16 {
    MachineType = 10001,

    // PvBatteryVoltageCalibrationCoef = 10007,
    ChargerWorkstate = 15201,
    MpptState = 15202,
    ChargingState = 15203,
    // Reserved = 15204,
    PvVoltage = 15205,
    PvBatteryVoltage = 15206,
    PvChargerCurrent = 15207,
    PvChargerPower = 15208,
    PvRadiatorTemperature = 15209,
    PvExternalTemperature = 15210,
    PvBatteryRelay = 15211,
    PvRelay = 15212,
    PvErrorMessage = 15213,
    PvWarningMessage = 15214,
    PvBattVolGrade = 15215,
    PvRatedCurrent = 15216,
    // AccumulatedPVPowerHigh = 15217,
    // AccumulatedPVPowerLow = 15218,
    PvAccumulatedDay = 15219,
    PvAccumulatedHour = 15220,
    PvAccumulatedMinute = 15221,

    BatteryVoltageCalibrationCoef = 20009,

    ChargetSourcePriority = 20143,

    AcVoltageGrade = 25202,
    RatedPowerVa = 25203,
    // Reserved1 = 25204,
    BatteryVoltage = 25205,
    InverterVoltage = 25206,
    GridVoltage = 25207,
    BusVoltage = 25208,
    ControlCurrent = 25209,
    InverterCurrent = 25210,
    GridCurrent = 25211,
    LoadCurrent = 25212,
    PInverter = 25213,
    PGrid = 25214,
    PLoad = 25215,
    LoadPercent = 25216,
    SInverter = 25217,
    SGrid = 25218,
    SLoad = 25219,
    // Reserved2 = 25220,
    QInverter = 25221,
    QGrid = 25222,
    QLoad = 25223,
    // Reserved3 = 25224,
    InverterFrequency = 25225,
    GridFrequency = 25226,
    // Reserved4 = 25227,
    // Reserved5 = 25228,
    InverterMaxNumber = 25229,
    CombineType = 25230,
    InverterNumber = 25231,
    // Reserved6 = 25232,
    AcRadiatorTemperature = 25233,
    TransformerTemperature = 25234,
    DCRadiatorTemperature = 25235,
    // Reserved7 = 25236,
    InverterRelayState = 25237,
    GridRelayState = 25238,
    LoadRelayState = 25239,
    NLineRelayState = 25240,
    DCRelayState = 25241,
    EarthRelayState = 25242,
    // Reserved8 = 25243,
    // Reserved9 = 25244,
    // AccumulatedChargerPowerHigh = 25245,
    // AccumulatedChargerPowerLow = 25246,
    // AccumulatedDischargerPowerHigh = 25247,
    // AccumulatedDischargerPowerLow = 25248,
    // AccumulatedBuyPowerHigh = 25249,
    // AccumulatedBuyPowerLow = 25250,
    // AccumulatedSellPowerHigh = 25251,
    // AccumulatedSellPowerLow = 25252,
    // AccumulatedLoadPowerHigh = 25253,
    // AccumulatedLoadPowerLow = 25254,
    // AccumulatedSelfUsePowerHigh = 25255,
    // AccumulatedSelfUsePowerLow = 25256,
    // AccumulatedPVSellPowerHigh = 25257,
    // AccumulatedPVSellPowerLow = 25258,
    // AccumulatedGridChargerPowerHigh = 25259,
    // AccumulatedGridChargerPowerLow = 25260,
    ErrorMessage1 = 25261,
    ErrorMessage2 = 25262,
    ErrorMessage3 = 25263,
    // Reserved10 = 25264,
    WarningMessage1 = 25265,
    WarningMessage2 = 25266,
    // Reserved11 = 25267,
    // Reserved12 = 25268,
    // SerialNumberHigh = 25269,
    // SerialNumberLow = 25270,
    HardwareVersion = 25271,
    SoftwareVersion = 25272,
    BatteryPower = 25273,
    BatteryCurrent = 25274,
    BatteryVoltageGrade = 25275,
    // Reserved13 = 25276,
    RatedPowerW = 25277,
}

#[derive(Debug, Clone, Copy)]
struct Reg16Val {
    val: u16,
    var: Reg16,
}

#[derive(Debug, Clone, Copy)]
struct Reg32Val {
    val: [u16; 2],
    var: Reg32,
}

impl Reg32Val {
    fn val(&self) -> f32 {
        f32::from(self.val[0]) * 1000.0 + f32::from(self.val[1]) * 0.1
    }
}

#[derive(Debug)]
enum ConnectionState {
    Disconnected,
    Connected,
}

impl From<u16> for ConnectionState {
    fn from(val: u16) -> Self {
        match val {
            0 => ConnectionState::Disconnected,
            1 => ConnectionState::Connected,
            _ => unreachable!("Invalid connection state: {}", val),
        }
    }
}

impl Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "Disconnected"),
            ConnectionState::Connected => write!(f, "Connected"),
        }
    }
}

#[derive(Debug)]
enum ChargerWorkstate {
    Init,
    SelfTest,
    Work,
    Stop,
}

impl From<u16> for ChargerWorkstate {
    fn from(val: u16) -> Self {
        match val {
            0 => ChargerWorkstate::Init,
            1 => ChargerWorkstate::SelfTest,
            2 => ChargerWorkstate::Work,
            3 => ChargerWorkstate::Stop,
            _ => unreachable!("Invalid charger workstate: {}", val),
        }
    }
}

impl Display for ChargerWorkstate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChargerWorkstate::Init => write!(f, "Initializing"),
            ChargerWorkstate::SelfTest => write!(f, "Self Test"),
            ChargerWorkstate::Work => write!(f, "Work Mode"),
            ChargerWorkstate::Stop => write!(f, "Stop Mode"),
        }
    }
}

#[derive(Debug)]
enum MpptState {
    Stop,
    Mppt,
    CurrentLimited,
}

impl From<u16> for MpptState {
    fn from(val: u16) -> Self {
        match val {
            0 => MpptState::Stop,
            1 => MpptState::Mppt,
            2 => MpptState::CurrentLimited,
            _ => unreachable!("Invalid MPPT state: {}", val),
        }
    }
}

impl Display for MpptState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MpptState::Stop => write!(f, "Stop"),
            MpptState::Mppt => write!(f, "MPPT"),
            MpptState::CurrentLimited => write!(f, "Current Limited"),
        }
    }
}

#[derive(Debug)]
enum ChargingState {
    Stop,
    Absorb,
    Float,
    End,
}

impl From<u16> for ChargingState {
    fn from(val: u16) -> Self {
        match val {
            0 => ChargingState::Stop,
            1 => ChargingState::Absorb,
            2 => ChargingState::Float,
            3 => ChargingState::End,
            _ => unreachable!("Invalid charging state: {}", val),
        }
    }
}

impl Display for ChargingState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChargingState::Stop => write!(f, "Stop"),
            ChargingState::Absorb => write!(f, "Absorb charging"),
            ChargingState::Float => write!(f, "Float charging"),
            ChargingState::End => write!(f, "End of charging"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ChargerSourcePriority {
    SolarOrElseGrid,
    SolarAndGrid,
    OnlySolar,
}

impl IntoValue16 for ChargerSourcePriority {
    fn into_value16(self) -> u16 {
        match self {
            ChargerSourcePriority::SolarOrElseGrid => 0,
            ChargerSourcePriority::SolarAndGrid => 2,
            ChargerSourcePriority::OnlySolar => 3,
        }
    }
}

impl From<u16> for ChargerSourcePriority {
    fn from(val: u16) -> Self {
        match val {
            0 => ChargerSourcePriority::SolarOrElseGrid,
            2 => ChargerSourcePriority::SolarAndGrid,
            3 => ChargerSourcePriority::OnlySolar,
            _ => unreachable!("Invalid charger source priority: {val}"),
        }
    }
}

impl Display for ChargerSourcePriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChargerSourcePriority::SolarOrElseGrid => write!(f, "Solar when available, else Grid"),
            ChargerSourcePriority::SolarAndGrid => write!(f, "Solar and Grid"),
            ChargerSourcePriority::OnlySolar => write!(f, "Only Solar"),
        }
    }
}

impl Display for Reg16Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Reg16::*;
        let fval = f32::from(self.val);
        let ival = self.val as i16;
        let f01 = fval / 10.0;
        let f001 = fval / 100.0;

        macro_rules! conn {
            () => {
                ConnectionState::from(self.val)
            };
        }

        match self.var {
            MachineType => write!(f, "Machine Type: {}", self.val),

            ChargerWorkstate => write!(
                f,
                "Charger Workstate: {}",
                crate::ChargerWorkstate::from(self.val)
            ),
            MpptState => write!(f, "MPPT State: {}", crate::MpptState::from(self.val)),
            ChargingState => write!(
                f,
                "Charging State: {}",
                crate::ChargingState::from(self.val)
            ),
            // PvBatteryVoltageCalibrationCoef => write!(f, "PV Battery Voltage Calibration Coefficient: {}", ival),
            PvVoltage => write!(f, "PV Voltage: {} V", f01),
            PvBatteryVoltage => write!(f, "PV Battery Voltage: {} V", f01),
            PvChargerCurrent => write!(f, "PV Charger Current: {} A", f01),
            PvChargerPower => write!(f, "PV Charger Power: {} W", self.val),
            PvRadiatorTemperature => write!(f, "PV Radiator Temperature: {} °C", ival),
            PvExternalTemperature => write!(f, "PV External Temperature: {} °C", ival),
            PvBatteryRelay => write!(f, "PV Battery Relay: {}", conn!()),
            PvRelay => write!(f, "PV Relay: {}", conn!()),
            PvErrorMessage => write!(f, "PV Error Message: {}", self.val),
            PvWarningMessage => write!(f, "PV Warning Message: {}", self.val),
            PvBattVolGrade => write!(f, "PV Battery Voltage Grade: {} V", self.val),
            PvRatedCurrent => write!(f, "PV Rated Current: {} A", f01),
            PvAccumulatedDay => write!(f, "PV Accumulated Day: {}", self.val),
            PvAccumulatedHour => write!(f, "PV Accumulated Hour: {}", self.val),
            PvAccumulatedMinute => write!(f, "PV Accumulated Minute: {}", self.val),

            BatteryVoltageCalibrationCoef => {
                write!(f, "Battery Voltage Calibration Coefficient: {}", ival)
            }

            ChargetSourcePriority => write!(
                f,
                "Charger Source: {}",
                crate::ChargerSourcePriority::from(self.val)
            ),

            AcVoltageGrade => write!(f, "AC Voltage Grade: {} V", self.val),
            RatedPowerVa => write!(f, "Rated Power (VA): {} VA", self.val),
            BatteryVoltage => write!(f, "Battery Voltage: {} V", f01),
            InverterVoltage => write!(f, "Inverter Voltage: {} V", f01),
            GridVoltage => write!(f, "Grid Voltage: {} V", f01),
            BusVoltage => write!(f, "Bus Voltage: {} V", f01),
            ControlCurrent => write!(f, "Control Current: {} A", f01),
            InverterCurrent => write!(f, "Inverter Current: {} A", f01),
            GridCurrent => write!(f, "Grid Current: {} A", f01),
            LoadCurrent => write!(f, "Load Current: {} A", f01),
            PInverter => write!(f, "P Inverter: {} W", ival),
            PGrid => write!(f, "P Grid: {} W", self.val),
            PLoad => write!(f, "P Load: {} W", self.val),
            LoadPercent => write!(f, "Load Percent: {} %", self.val),
            SInverter => write!(f, "S Inverter: {} VA", self.val),
            SGrid => write!(f, "S Grid: {} VA", self.val),
            SLoad => write!(f, "S Load: {} VA", self.val),
            QInverter => write!(f, "Q Inverter: {} var", self.val),
            QGrid => write!(f, "Q Grid: {} var", self.val),
            QLoad => write!(f, "Q Load: {} var", self.val),
            InverterFrequency => write!(f, "Inverter Frequency: {} Hz", f001),
            GridFrequency => write!(f, "Grid Frequency: {} Hz", f001),
            InverterMaxNumber => write!(f, "Inverter Max Number: {}", self.val),
            CombineType => write!(f, "Combine Type: {}", self.val),
            InverterNumber => write!(f, "Inverter Number: {}", self.val),
            AcRadiatorTemperature => write!(f, "AC Radiator Temperature: {} °C", ival),
            TransformerTemperature => write!(f, "Transformer Temperature: {} °C", ival),
            DCRadiatorTemperature => write!(f, "DC Radiator Temperature: {} °C", ival),
            InverterRelayState => write!(f, "Inverter Relay State: {}", conn!()),
            GridRelayState => write!(f, "Grid Relay State: {}", conn!()),
            LoadRelayState => write!(f, "Load Relay State: {}", conn!()),
            NLineRelayState => write!(f, "N Line Relay State: {}", conn!()),
            DCRelayState => write!(f, "DC Relay State: {}", conn!()),
            EarthRelayState => write!(f, "Earth Relay State: {}", conn!()),
            ErrorMessage1 => write!(f, "Error Message 1: {}", self.val),
            ErrorMessage2 => write!(f, "Error Message 2: {}", self.val),
            ErrorMessage3 => write!(f, "Error Message 3: {}", self.val),
            WarningMessage1 => write!(f, "Warning Message 1: {}", self.val),
            WarningMessage2 => write!(f, "Warning Message 2: {}", self.val),
            HardwareVersion => write!(f, "Hardware Version: {}", self.val),
            SoftwareVersion => write!(f, "Software Version: {}", self.val),
            BatteryPower => write!(f, "Battery Power: {} W", ival),
            BatteryCurrent => write!(f, "Battery Current: {} A", ival),
            BatteryVoltageGrade => write!(f, "Battery Voltage Grade: {} V", self.val),
            RatedPowerW => write!(f, "Rated Power (W): {} W", self.val),
        }
    }
}

impl Display for Reg32Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Reg32::*;
        let sum = self.val();

        match self.var {
            AccumulatedPvPower => write!(f, "Accumulated PV Power: {} kWh", sum),
            AccumulatedChargerPower => write!(f, "Accumulated Charger Power: {} kWh", sum),
            AccumulatedDischargerPower => write!(f, "Accumulated Discharger Power: {} kWh", sum),
            AccumulatedBuyPower => write!(f, "Accumulated Buy Power: {} kWh", sum),
            AccumulatedSellPower => write!(f, "Accumulated Sell Power: {} kWh", sum),
            AccumulatedLoadPower => write!(f, "Accumulated Load Power: {} kWh", sum),
            AccumulatedSelfUsePower => write!(f, "Accumulated Self Use Power: {} kWh", sum),
            AccumulatedPvSellPower => write!(f, "Accumulated PV Sell Power: {} kWh", sum),
            AccumulatedGridChargerPower => write!(f, "Accumulated Grid Charger Power: {} kWh", sum),
            SerialNumber => write!(f, "Serial Number: {}{}", self.val[0], self.val[1]),
        }
    }
}

#[derive(Debug, Clone, Copy, EnumIter, FromRepr)]
#[repr(u16)]
enum Reg32 {
    AccumulatedPvPower = 15217,

    AccumulatedChargerPower = 25245,
    AccumulatedDischargerPower = 25247,
    AccumulatedBuyPower = 25249,
    AccumulatedSellPower = 25251,
    AccumulatedLoadPower = 25253,
    AccumulatedSelfUsePower = 25255,
    AccumulatedPvSellPower = 25257,
    AccumulatedGridChargerPower = 25259,
    SerialNumber = 25269,
}

type Error = Box<dyn std::error::Error>;

#[async_trait]
trait SerialRead {
    type Item: Display;

    async fn read(&self, channel: &mut Channel) -> Result<Self::Item, Error>;
}

#[async_trait]
impl SerialRead for Reg16 {
    type Item = Reg16Val;

    async fn read(&self, channel: &mut Channel) -> Result<Self::Item, Error> {
        let addr = *self as u16;
        let val = ctx_read(addr, channel).await?;

        Ok(Reg16Val { val, var: *self })
    }
}

#[async_trait]
trait SerialWrite {
    async fn write(&self, val: impl IntoValue16, channel: &mut Channel) -> Result<(), Error>;
}

trait IntoValue16: Send {
    fn into_value16(self) -> u16;
}

impl IntoValue16 for u16 {
    fn into_value16(self) -> u16 {
        self
    }
}

#[async_trait]
impl SerialWrite for Reg16 {
    async fn write(&self, val: impl IntoValue16, channel: &mut Channel) -> Result<(), Error> {
        let addr = *self as u16;
        reg_write(addr, val.into_value16(), channel).await?;
        Ok(())
    }
}

#[async_trait]
impl SerialRead for Reg32 {
    type Item = Reg32Val;

    #[allow(clippy::identity_op)]
    async fn read(&self, channel: &mut Channel) -> Result<Self::Item, Error> {
        let addr = *self as u16;
        let val0 = ctx_read(addr + 0, channel).await?;
        let val1 = ctx_read(addr + 1, channel).await?;

        Ok(Reg32Val {
            val: [val0, val1],
            var: *self,
        })
    }
}

async fn read_many(
    addr: std::ops::Range<u16>,
    channel: &mut Channel,
) -> Result<Vec<u16>, RequestError> {
    let now = std::time::Instant::now();

    let val = channel
        .read_holding_registers(
            RequestParam {
                id: SLAVE,
                response_timeout: TIMEOUT,
            },
            AddressRange {
                start: addr.start,
                count: addr.clone().count() as u16,
            },
        )
        .await
        .map(|v| v.iter().map(|v| v.value).collect())?;

    let elapsed = now.elapsed();
    debug!("Read {:?}: {} ms", addr, elapsed.as_millis());
    sleep(AFTER_IO_HALT).await; // needed to prevent errors on too fast reads

    Ok(val)
}

async fn ctx_read(addr: u16, channel: &mut Channel) -> Result<u16, RequestError> {
    let now = std::time::Instant::now();

    let val = channel
        .read_holding_registers(
            RequestParam {
                id: SLAVE,
                response_timeout: TIMEOUT,
            },
            AddressRange {
                start: addr,
                count: 1,
            },
        )
        .await
        .map(|v| v[0].value)?;

    let elapsed = now.elapsed();
    debug!("Read {}: {} ms", addr, elapsed.as_millis());
    sleep(AFTER_IO_HALT).await; // needed to prevent errors on too fast IO

    Ok(val)
}

async fn reg_write(addr: u16, val: u16, channel: &mut Channel) -> Result<(), RequestError> {
    let now = std::time::Instant::now();

    channel
        .write_multiple_registers(
            RequestParam {
                id: SLAVE,
                response_timeout: TIMEOUT,
            },
            WriteMultiple::from(addr, vec![val]).unwrap(),
        )
        .await?;

    let elapsed = now.elapsed();
    debug!("Write {}: {} ms", addr, elapsed.as_millis());
    sleep(AFTER_IO_HALT).await; // needed to prevent errors on too fast IO

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registers are required to be listed in sorted order, so that they can be loaded
    /// in groups.
    #[test]
    fn reg_sorted() {
        let mut last = 0;
        for reg in Reg16::iter().map(|v| v as u16) {
            assert!(reg > last);
            last = reg;
        }

        let mut last = 0;
        for reg in Reg32::iter().map(|v| v as u16) {
            assert!(reg > last);
            last = reg;
        }
    }
}
