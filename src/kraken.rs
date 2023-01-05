use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_str, from_str_unix_epoch_sec, Instrument, MarketData, MarketDataKind, OrderBook,
    OrderBookLevel, OrderKind, Side, Trade, Venue,
};
use crate::websocket::{
    Websocket, WebsocketClient, WebsocketSubscriber, WebsocketSubscription,
    WebsocketSubscriptionKind,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tungstenite::Message;

pub struct Kraken {
    stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl Kraken {
    pub const BASE_WS_URL: &'static str = "wss://ws.kraken.com/";

    pub const TRADE_CHANNEL: &'static str = "trade";
    pub const L2_QUOTE_CHANNEL: &'static str = "book";

    pub fn new() -> Self {
        Self {
            stream_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn build_stream_name(sub: &WebsocketSubscription) -> Result<(String, &str), String> {
        let market = format!(
            "{}/{}",
            sub.instrument.base.to_uppercase(),
            sub.instrument.quote.to_uppercase()
        );

        let channel = match sub.kind {
            WebsocketSubscriptionKind::Trade => Self::TRADE_CHANNEL,
            WebsocketSubscriptionKind::L2Quote => Self::L2_QUOTE_CHANNEL,
            // _ => panic!("WebsocketSubscriptionKind not supported for exchange"),
        };

        Ok((market, channel))
    }

    async fn handle_messages(&self, mut socket: Websocket) -> UnboundedReceiverStream<MarketData> {
        let (tx, rx) = mpsc::unbounded_channel();
        let subscriptions = self.stream_subscriptions.clone();

        tokio::spawn(async move {
            loop {
                if let Some(msg) = Self::read_message(&mut socket, &subscriptions) {
                    tx.send(msg).unwrap();
                }
            }
        });

        UnboundedReceiverStream::new(rx)
    }

    fn read_message<T: Read + Write>(
        socket: &mut tungstenite::protocol::WebSocket<T>,
        stream_subscriptions: &Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
    ) -> Option<MarketData> {
        match socket.read_message().expect("Error reading message") {
            Message::Text(json) => {
                // println!("{}", json);
                let map = stream_subscriptions.lock().unwrap();

                match serde_json::from_str(&json).unwrap() {
                    KrakenMessage::Heartbeat(_) => None,
                    KrakenMessage::SubscriptionResponse(_) => None,
                    KrakenMessage::Trade(trade) => {
                        let sub = map
                            .get(&format!("{}|{}", trade.symbol, trade.channel_name))
                            .expect("unable to find matching subscription");

                        // TODO: yield each individual trade, rather than aggregating them into one
                        let agg_trade = KrakenTradeData {
                            amount: trade.trades.iter().map(|x| x.amount).sum(),
                            ..trade.trades[0].clone()
                        };

                        return Some(MarketData::from((sub.instrument.clone(), agg_trade)));
                    }
                    KrakenMessage::Snapshot(snapshot) => {
                        let sub = map
                            .get(&format!("{}|{}", snapshot.symbol, snapshot.channel_name))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), snapshot)));
                    }
                    KrakenMessage::L2UpdateSingle(update) => {
                        let sub = map
                            .get(&format!("{}|{}", update.symbol, update.channel_name))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), update)));
                    }
                    KrakenMessage::L2UpdateDouble(update) => {
                        // println!("{:?}", quote);
                        let sub = map
                            .get(&format!("{}|{}", update.symbol, update.channel_name))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), update)));
                    }
                }
            }
            x => {
                println!("other: {:?}", x);
                None
            }
        }
    }
}

#[async_trait]
impl WebsocketSubscriber for Kraken {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        for sub in subscriptions {
            if sub.venue == Venue::Kraken {
                let (market, channel) = Self::build_stream_name(&sub).unwrap();

                // update subscription map so we can match against it on each exchange message
                let sub_channel;
                match channel {
                    Self::TRADE_CHANNEL => sub_channel = "trade",
                    Self::L2_QUOTE_CHANNEL => sub_channel = "book-10",

                    _ => panic!("Unexpected channel"),
                }
                self.stream_subscriptions
                    .lock()
                    .unwrap()
                    .insert(format!("{}|{}", market, &sub_channel), sub.clone());

                // subscribe to market / channel
                let _ = socket.write_message(Message::Text(
                    json!({
                        "event": "subscribe",
                        "pair": [market],
                        "subscription": {
                            "name": channel
                        }
                    })
                    .to_string(),
                ));

                if let Message::Text(json) = socket.read_message().expect("Error reading message") {
                    println!("{:?}", json);
                }
            }
        }

        let exchange_rx = self.handle_messages(socket).await;

        Ok(exchange_rx)
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum KrakenMessage {
    SubscriptionResponse(KrakenSubscriptionResponse),

    Heartbeat(KrakenHeartbeat),

    Trade(KrakenTrade),

    Snapshot(KrakenSnapshot),

    L2UpdateSingle(KrakenL2UpdateSingle),

    L2UpdateDouble(KrakenL2UpdateDouble),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KrakenSubscriptionResponse {
    #[serde(rename = "channelID")]
    pub channel_id: i64,

    pub channel_name: String,

    pub event: String,

    #[serde(rename = "pair")]
    pub symbol: String,

    pub status: String,

    pub subscription: KrakenSubscription,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KrakenSubscription {
    pub name: String,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenHeartbeat {
    pub event: String,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenTrade {
    pub channel_id: u64,

    pub trades: Vec<KrakenTradeData>,

    pub channel_name: String,

    pub symbol: String,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenTradeData {
    #[serde(deserialize_with = "from_str")]
    pub price: f64,

    #[serde(deserialize_with = "from_str")]
    pub amount: f64,

    #[serde(deserialize_with = "from_str_unix_epoch_sec")]
    pub timestamp: DateTime<Utc>,

    pub side: Side,

    pub order_type: OrderKind,

    pub misc: String,
}

impl From<(Instrument, KrakenTradeData)> for MarketData {
    fn from((instrument, trade): (Instrument, KrakenTradeData)) -> Self {
        Self {
            venue: Venue::Kraken,
            instrument,
            venue_time: trade.timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: None,
                price: trade.price,
                quantity: trade.amount,
                side: trade.side,
            }),
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenSnapshot {
    pub channel_id: u64,

    pub data: KrakenSnapshotData,

    pub channel_name: String,

    pub symbol: String,
}

impl From<(Instrument, KrakenSnapshot)> for MarketData {
    fn from((instrument, quote): (Instrument, KrakenSnapshot)) -> Self {
        let bids = quote
            .data
            .bids
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let asks = quote
            .data
            .asks
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        Self {
            venue: Venue::Kraken,
            instrument,
            venue_time: Utc::now(), // TODO: make this optional
            received_time: Utc::now(),
            kind: MarketDataKind::L2Snapshot(OrderBook { bids, asks }),
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenSnapshotData {
    #[serde(rename = "bs")]
    pub bids: Vec<KrakenLevel>,

    #[serde(rename = "as")]
    pub asks: Vec<KrakenLevel>,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenLevel {
    #[serde(deserialize_with = "from_str")]
    pub price: f64,

    #[serde(deserialize_with = "from_str")]
    pub quantity: f64,

    #[serde(deserialize_with = "from_str_unix_epoch_sec")]
    pub timestamp: DateTime<Utc>,

    #[serde(default, rename = "r")]
    pub republished: Option<String>,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenL2UpdateSingle {
    pub channel_id: u64,

    pub data: KrakenL2Data,

    pub channel_name: String,

    pub symbol: String,
}

impl From<(Instrument, KrakenL2UpdateSingle)> for MarketData {
    fn from((instrument, quote): (Instrument, KrakenL2UpdateSingle)) -> Self {
        let bids = quote
            .data
            .bids
            .iter()
            .flatten()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let asks = quote
            .data
            .asks
            .iter()
            .flatten()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        Self {
            venue: Venue::Kraken,
            instrument,
            venue_time: Utc::now(), // TODO: make this optional
            received_time: Utc::now(),
            kind: MarketDataKind::L2Snapshot(OrderBook { bids, asks }),
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenL2UpdateDouble {
    pub channel_id: u64,

    pub data1: KrakenL2Data,

    pub data2: KrakenL2Data,

    pub channel_name: String,

    pub symbol: String,
}

impl From<(Instrument, KrakenL2UpdateDouble)> for MarketData {
    fn from((instrument, quote): (Instrument, KrakenL2UpdateDouble)) -> Self {
        // TODO: MERGE THESE TOGETHER
        let _bids1: Vec<OrderBookLevel> = quote
            .data1
            .bids
            .iter()
            .flatten()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let _bids2: Vec<OrderBookLevel> = quote
            .data2
            .bids
            .iter()
            .flatten()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let _asks1: Vec<OrderBookLevel> = quote
            .data1
            .asks
            .iter()
            .flatten()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let _asks2: Vec<OrderBookLevel> = quote
            .data2
            .asks
            .iter()
            .flatten()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        Self {
            venue: Venue::Kraken,
            instrument,
            venue_time: Utc::now(), // TODO: make this optional
            received_time: Utc::now(),
            kind: MarketDataKind::L2Snapshot(OrderBook {
                bids: vec![],
                asks: vec![],
            }),
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct KrakenL2Data {
    #[serde(rename = "b")]
    pub bids: Option<Vec<KrakenLevel>>,

    #[serde(rename = "a")]
    pub asks: Option<Vec<KrakenLevel>>,

    #[serde(default, rename = "c")]
    pub checksum: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"[337,[["16820.40000","0.00002191","1672839975.703671","s","m",""],["16820.40000","0.01095178","1672839975.703792","b","l",""]],"trade","XBT/USD"]"#;

        assert_eq!(
            serde_json::from_str::<KrakenMessage>(input).expect("failed to deserialise"),
            KrakenMessage::Trade(KrakenTrade {
                channel_id: 337,
                trades: vec![
                    KrakenTradeData {
                        price: 16820.40000,
                        amount: 0.00002191,
                        timestamp: DateTime::parse_from_rfc3339("2023-01-04T13:46:15Z")
                            .unwrap()
                            .with_timezone(&Utc),
                        side: Side::Sell,
                        order_type: OrderKind::Market,
                        misc: "".to_string()
                    },
                    KrakenTradeData {
                        price: 16820.40000,
                        amount: 0.01095178,
                        timestamp: DateTime::parse_from_rfc3339("2023-01-04T13:46:15Z")
                            .unwrap()
                            .with_timezone(&Utc),
                        side: Side::Buy,
                        order_type: OrderKind::Limit,
                        misc: "".to_string()
                    }
                ],
                channel_name: "trade".to_string(),
                symbol: "XBT/USD".to_string()
            })
        );
    }

    #[test]
    fn deserialise_snapshot_json_to_l2_quote() {
        let input = r#"[336,{"as":[["16835.00000","0.16686808","1672847524.720055"],["16838.30000","0.47317063","1672847522.867256"]],"bs":[["16834.90000","5.36202364","1672847529.231729"],["16834.40000","4.39497549","1672847529.170574"]]},"book-10","XBT/USD"]"#;

        assert_eq!(
            serde_json::from_str::<KrakenMessage>(input).expect("failed to deserialise"),
            KrakenMessage::Snapshot(KrakenSnapshot {
                channel_id: 336,
                data: KrakenSnapshotData {
                    asks: vec![
                        KrakenLevel {
                            price: 16835.00000,
                            quantity: 0.16686808,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:52:04Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        },
                        KrakenLevel {
                            price: 16838.30000,
                            quantity: 0.47317063,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:52:02Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        }
                    ],
                    bids: vec![
                        KrakenLevel {
                            price: 16834.90000,
                            quantity: 5.36202364,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:52:09Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        },
                        KrakenLevel {
                            price: 16834.40000,
                            quantity: 4.39497549,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:52:09Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        }
                    ],
                },
                channel_name: "book-10".to_string(),
                symbol: "XBT/USD".to_string()
            })
        )
    }

    #[test]
    fn deserialise_single_payload_json_to_l2_quote() {
        let input = r#"[336,{"b":[["16802.10000","0.00000000","1672845724.467174"],["16800.70000","0.38922671","1672845714.146221","r"]],"c":"2138871801"},"book-10","XBT/USD"]"#;

        assert_eq!(
            serde_json::from_str::<KrakenMessage>(input).expect("failed to deserialise"),
            KrakenMessage::L2UpdateSingle(KrakenL2UpdateSingle {
                channel_id: 336,
                data: KrakenL2Data {
                    bids: Some(vec![
                        KrakenLevel {
                            price: 16802.10000,
                            quantity: 0.00000000,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:22:04Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        },
                        KrakenLevel {
                            price: 16800.70000,
                            quantity: 0.38922671,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:21:54Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: Some("r".to_string())
                        }
                    ]),
                    asks: None,
                    checksum: Some("2138871801".to_string())
                },
                channel_name: "book-10".to_string(),
                symbol: "XBT/USD".to_string()
            })
        )
    }

    #[test]
    fn deserialise_double_payload_json_to_l2_quote() {
        let input = r#"[336,{"a":[["16781.30000","0.00000000","1672845081.666864"],["16783.30000","0.22450899","1672845077.409939","r"]]},{"b":[["16781.00000","0.02396342","1672845081.667161"],["16781.00000","0.02085587","1672845081.667277"]],"c":"791170235"},"book-10","XBT/USD"]"#;

        assert_eq!(
            serde_json::from_str::<KrakenMessage>(input).expect("failed to deserialise"),
            KrakenMessage::L2UpdateDouble(KrakenL2UpdateDouble {
                channel_id: 336,
                data1: KrakenL2Data {
                    asks: Some(vec![
                        KrakenLevel {
                            price: 16781.30000,
                            quantity: 0.00000000,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:11:21Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        },
                        KrakenLevel {
                            price: 16783.30000,
                            quantity: 0.22450899,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:11:17Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: Some("r".to_string())
                        },
                    ]),
                    bids: None,
                    checksum: None
                },
                data2: KrakenL2Data {
                    bids: Some(vec![
                        KrakenLevel {
                            price: 16781.00000,
                            quantity: 0.02396342,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:11:21Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        },
                        KrakenLevel {
                            price: 16781.00000,
                            quantity: 0.02085587,
                            timestamp: DateTime::parse_from_rfc3339("2023-01-04T15:11:21Z")
                                .unwrap()
                                .with_timezone(&Utc),
                            republished: None
                        }
                    ]),
                    asks: None,
                    checksum: Some("791170235".to_string())
                },
                channel_name: "book-10".to_string(),
                symbol: "XBT/USD".to_string()
            })
        )
    }
}
