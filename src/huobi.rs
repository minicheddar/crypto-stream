use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_unix_epoch_ms, Instrument, MarketData, MarketDataKind, OrderBook, OrderBookLevel, Side,
    Trade, Venue,
};
use crate::websocket::{
    Websocket, WebsocketClient, WebsocketSubscriber, WebsocketSubscription,
    WebsocketSubscriptionKind,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tungstenite::Message;

pub struct Huobi {
    stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl Huobi {
    pub const BASE_WS_URL: &'static str = "wss://api-aws.huobi.pro/ws";

    pub const TRADE_CHANNEL: &'static str = "trade.detail";
    pub const L2_QUOTE_CHANNEL: &'static str = "mbp.refresh.10";

    pub fn new() -> Self {
        Self {
            stream_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn build_stream_name(sub: &WebsocketSubscription) -> Result<(String, &str), String> {
        let market = format!(
            "{}{}",
            sub.instrument.base.to_lowercase(),
            sub.instrument.quote.to_lowercase()
        );

        let channel = match sub.kind {
            WebsocketSubscriptionKind::Trade => Self::TRADE_CHANNEL,
            WebsocketSubscriptionKind::Quote => Self::L2_QUOTE_CHANNEL,
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
            Message::Binary(bytes) => {
                let mut gz = GzDecoder::new(&bytes[..]);
                let mut json = String::new();
                gz.read_to_string(&mut json)
                    .expect("Error decompressing huobi message");
                // println!("{}", json);
                let map = stream_subscriptions.lock().unwrap();

                match serde_json::from_str(&json).unwrap() {
                    HuobiMessage::Trade(trade) => {
                        let sub = map
                            .get(&trade.channel)
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((
                            sub.instrument.clone(),
                            trade.tick.data[0].clone(),
                        )));
                    }
                    HuobiMessage::Ping(ping) => {
                        socket
                            .write_message(Message::Text(
                                serde_json::to_string(&HuobiPong {
                                    pong: ping.timestamp,
                                })
                                .expect("unable to serialise huobi pong"),
                            ))
                            .expect("unable to send huobi pong");
                        return None;
                    }
                    HuobiMessage::Snapshot(snapshot) => {
                        let sub = map
                            .get(&snapshot.channel)
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((
                            sub.instrument.clone(),
                            snapshot.response_timestamp,
                            snapshot.tick.clone(),
                        )));
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
impl WebsocketSubscriber for Huobi {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        for sub in subscriptions {
            if sub.venue == Venue::Huobi {
                let (market, channel) = Self::build_stream_name(&sub).unwrap();

                // update subscription map so we can match against it on each exchange message
                let sub_key = format!("market.{}.{}", market, &channel);
                self.stream_subscriptions
                    .lock()
                    .unwrap()
                    .insert(sub_key.clone(), sub.clone());

                // subscribe to market / channel
                let _ = socket.write_message(Message::Text(
                    json!({
                        "sub": sub_key,
                        "id": "id1"
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
pub enum HuobiMessage {
    Ping(HuobiPing),

    Trade(HuobiTrade),

    Snapshot(HuobiSnapshot),
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiPing {
    #[serde(alias = "ping")]
    pub timestamp: u64,
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiPong {
    pub pong: u64,
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiTrade {
    #[serde(alias = "ch")]
    pub channel: String,

    #[serde(alias = "ts", deserialize_with = "from_unix_epoch_ms")]
    pub response_timestamp: DateTime<Utc>,

    pub tick: HuobiTradeTick,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiTradeTick {
    #[serde(alias = "id")]
    pub transaction_id: u64,

    #[serde(alias = "ts", deserialize_with = "from_unix_epoch_ms")]
    pub creation_timestamp: DateTime<Utc>,

    pub data: Vec<HuobiTradeData>,
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiTradeData {
    #[serde(alias = "tradeId")]
    pub trade_id: u64,

    pub amount: f64,

    pub price: f64,

    #[serde(alias = "ts", deserialize_with = "from_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    #[serde(alias = "direction")]
    pub side: Side,
}

impl From<(Instrument, HuobiTradeData)> for MarketData {
    fn from((instrument, trade): (Instrument, HuobiTradeData)) -> Self {
        Self {
            venue: Venue::Huobi,
            instrument,
            venue_time: trade.timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: Some(trade.trade_id.to_string()),
                price: trade.price,
                quantity: trade.amount,
                side: trade.side,
            }),
        }
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiSnapshot {
    #[serde(alias = "ch")]
    pub channel: String,

    #[serde(alias = "ts", deserialize_with = "from_unix_epoch_ms")]
    pub response_timestamp: DateTime<Utc>,

    pub tick: HuobiSnapshotTick,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiSnapshotTick {
    #[serde(alias = "seqNum")]
    pub sequence_number: u64,

    pub bids: Vec<HuobiLevel>,

    pub asks: Vec<HuobiLevel>,
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct HuobiLevel {
    pub price: f64,

    pub quantity: f64,
}

impl From<(Instrument, DateTime<Utc>, HuobiSnapshotTick)> for MarketData {
    fn from((instrument, timestamp, snapshot): (Instrument, DateTime<Utc>, HuobiSnapshotTick)) -> Self {
        let bids = snapshot
            .bids
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let asks = snapshot
            .asks
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        Self {
            venue: Venue::Huobi,
            instrument,
            venue_time: timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::L2Snapshot(OrderBook { bids, asks }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"{"ch":"market.btcusdt.trade.detail","ts":1672824147727,"tick":{"id":161573809785,"ts":1672824147724,"data":[{"id":161573809785708645025711503,"ts":1672824147724,"tradeId":102751627361,"amount":0.001,"price":16855.48,"direction":"buy"}]}}"#;

        assert_eq!(
            serde_json::from_str::<HuobiMessage>(input).expect("failed to deserialise"),
            HuobiMessage::Trade(HuobiTrade {
                channel: "market.btcusdt.trade.detail".to_string(),
                response_timestamp: DateTime::parse_from_rfc3339("2023-01-04T09:22:27.727Z")
                    .unwrap()
                    .with_timezone(&Utc),
                tick: HuobiTradeTick {
                    transaction_id: 161573809785,
                    creation_timestamp: DateTime::parse_from_rfc3339("2023-01-04T09:22:27.724Z")
                        .unwrap()
                        .with_timezone(&Utc),
                    data: vec![HuobiTradeData {
                        trade_id: 102751627361,
                        amount: 0.001,
                        price: 16855.48,
                        timestamp: DateTime::parse_from_rfc3339("2023-01-04T09:22:27.724Z")
                            .unwrap()
                            .with_timezone(&Utc),
                        side: Side::Buy
                    }]
                }
            })
        );
    }

    #[test]
    fn deserialise_json_to_snapshot() {
        let input = r#"{"ch":"market.btcusdt.mbp.refresh.10","ts":1672825916217,"tick":{"seqNum":161573989890,"bids":[[16840.68,0.76565],[16840.0,0.653206]],"asks":[[16840.69,1.365447],[16841.55,0.3]]}}"#;

        assert_eq!(
            serde_json::from_str::<HuobiMessage>(input).expect("failed to deserialise"),
            HuobiMessage::Snapshot(HuobiSnapshot {
                channel: "market.btcusdt.mbp.refresh.10".to_string(),
                response_timestamp: DateTime::parse_from_rfc3339("2023-01-04T09:51:56.217Z")
                    .unwrap()
                    .with_timezone(&Utc),
                tick: HuobiSnapshotTick {
                    sequence_number: 161573989890,
                    bids: vec![
                        HuobiLevel {
                            price: 16840.68,
                            quantity: 0.76565
                        },
                        HuobiLevel {
                            price: 16840.0,
                            quantity: 0.653206
                        }
                    ],
                    asks: vec![
                        HuobiLevel {
                            price: 16840.69,
                            quantity: 1.365447
                        },
                        HuobiLevel {
                            price: 16841.55,
                            quantity: 0.3
                        }
                    ]
                }
            })
        );
    }
}
