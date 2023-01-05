use chrono::{DateTime, TimeZone, Utc};
use serde::{de, Deserialize, Serialize, Serializer};
use std::str::FromStr;

pub struct Message {
    pub sequence: u64,
    pub version: String,
    pub kind: MessageKind,
}

pub enum MessageKind {
    Command(Command),
    Query(Query),
    Event(Event),
}

#[derive(Debug)]
pub enum Command {
    PlaceOrder(Order),
    CancelOrder(Order),
    CancelOpenOrders(Instrument, Vec<Order>),
    CancelAllOpenOrders,
    ExitAllPositions,
    Shutdown,
}

pub enum Query {
    FetchOpenOrders(Instrument),
    FetchOpenPositions(Instrument),
    FetchHistoricalTrades(Instrument),
    FetchHistoricalPrices(Instrument),
}

#[derive(Debug)]
pub enum Event {
    MarketDataReceived(MarketData),
    OrderCreated(Order),
    OrderCancelled(Order),
    OrderFilled(Order),
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Order {
    pub direction: Option<OrderDirection>,
    pub id: Option<String>,
    pub kind: OrderKind,
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub status: OrderStatus,
    pub symbol: String,
    pub creation_time: DateTime<Utc>,
    pub venue: Venue,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub enum OrderDirection {
    Entry,
    Exit,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub enum OrderStatus {
    New,
    Cancelled,
    PartiallyFilled,
    Filled,
    Rejected,
    Expired,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub enum OrderKind {
    #[serde(alias = "l")]
    Limit,

    #[serde(alias = "m")]
    Market,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub enum Side {
    #[serde(alias = "buy", alias = "b")]
    Buy,

    #[serde(alias = "sell", alias = "s")]
    Sell,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize, Hash)]
pub enum Venue {
    BinanceFuturesUsd,
    BinanceSpot,
    Coinbase,
    GateIO,
    Huobi,
    Kraken,
    Okx,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct MarketData {
    pub venue: Venue,
    pub instrument: Instrument,
    pub venue_time: DateTime<Utc>,
    pub received_time: DateTime<Utc>,
    pub kind: MarketDataKind,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize, Hash)]
pub struct Instrument {
    pub base: String,
    pub quote: String,
    pub kind: InstrumentKind,
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize, Hash)]
pub enum InstrumentKind {
    Spot,
    FuturesPerpetual,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub id: Option<String>,
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OrderBookLevel {
    #[serde(deserialize_with = "from_str")]
    pub price: f64,

    #[serde(deserialize_with = "from_str")]
    pub quantity: f64,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OrderBook {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub enum MarketDataKind {
    Trade(Trade),
    L2Snapshot(OrderBook),
    L2Update(OrderBook)
}

pub fn from_str<'de, D, T>(value: D) -> Result<T, D::Error>
where
    D: de::Deserializer<'de>,
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let str: String = de::Deserialize::deserialize(value)?;
    str.parse::<T>().map_err(de::Error::custom)
}

pub fn from_str_unix_epoch_sec<'de, D>(value: D) -> Result<DateTime<Utc>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let str: String = de::Deserialize::deserialize(value)?;
    let result = str.parse::<i64>();

    let timestamp = match result {
        Ok(x) => x,
        Err(_) => str
            .parse::<f64>()
            .expect("unable to parse str as i64 or f64") as i64,
    };

    Ok(Utc.timestamp_opt(timestamp, 0).unwrap())
}

pub fn from_unix_epoch_ms<'de, D>(value: D) -> Result<DateTime<Utc>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let timestamp = de::Deserialize::deserialize(value)?;
    Ok(Utc.timestamp_millis_opt(timestamp).unwrap())
}

pub fn from_str_unix_epoch_ms<'de, D>(value: D) -> Result<DateTime<Utc>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let str: String = de::Deserialize::deserialize(value)?;
    let result = str.parse::<i64>();

    let timestamp = match result {
        Ok(x) => x,
        Err(_) => str
            .parse::<f64>()
            .expect("unable to parse str as i64 or f64") as i64,
    };

    Ok(Utc.timestamp_millis_opt(timestamp).unwrap())
}

pub fn is_buyer_maker<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    serde::de::Deserialize::deserialize(deserializer).map(|buyer_is_maker| {
        if buyer_is_maker {
            Side::Sell
        } else {
            Side::Buy
        }
    })
}

pub fn to_unix_epoch_ns<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_i64(value.timestamp_nanos())
}
