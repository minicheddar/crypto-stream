use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_str, from_str_unix_epoch_ms, from_unix_epoch_ms, Instrument, MarketData, MarketDataKind,
    OrderBook, OrderBookLevel, Side, Trade, Venue,
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

pub struct GateIO {
    stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl GateIO {
    pub const BASE_WS_URL: &'static str = "wss://api.gateio.ws/ws/v4/";

    pub const TRADE_CHANNEL: &'static str = "spot.trades";
    pub const L2_QUOTE_CHANNEL: &'static str = "spot.order_book";
    const QUOTE_DEPTH: &'static str = "10";

    pub fn new() -> Self {
        Self {
            stream_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn build_stream_name(sub: &WebsocketSubscription) -> Result<(String, &str), String> {
        let market = format!(
            "{}_{}",
            sub.instrument.base.to_uppercase(),
            sub.instrument.quote.to_uppercase()
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
            Message::Text(json) => {
                // println!("{}", json);

                let map = stream_subscriptions.lock().unwrap();

                match serde_json::from_str(&json).unwrap() {
                    GateMessage::Trade(trade) => {
                        let sub = map
                            .get(&format!("{}|{}", trade.result.symbol, trade.channel))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), trade.result)));
                    }
                    GateMessage::Snapshot(snapshot) => {
                        let sub = map
                            .get(&format!("{}|{}", snapshot.result.symbol, snapshot.channel))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), snapshot.result)));
                    }
                }
            }
            _ => {
                // println!("other: {:?}", x);
                None
            }
        }
    }
}

#[async_trait]
impl WebsocketSubscriber for GateIO {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        for sub in subscriptions {
            if sub.venue == Venue::GateIO {
                let (market, channel) = Self::build_stream_name(&sub).unwrap();

                self.stream_subscriptions
                    .lock()
                    .unwrap()
                    .insert(format!("{}|{}", market, &channel), sub.clone());

                // subscribe to market / channel
                let payload = match sub.kind {
                    WebsocketSubscriptionKind::Trade => vec![market],
                    WebsocketSubscriptionKind::Quote => {
                        vec![market, Self::QUOTE_DEPTH.to_string(), "100ms".to_string()]
                    }
                };

                let _ = socket.write_message(Message::Text(
                    json!({
                        "time": Utc::now().timestamp(),
                        "channel": channel,
                        "event": "subscribe".to_string(),
                        "payload": payload
                    })
                    .to_string(),
                ));

                if let Message::Text(_) = socket.read_message().expect("Error reading message") {
                    // println!("{:?}", json);
                }
            }
        }

        let exchange_rx = self.handle_messages(socket).await;

        Ok(exchange_rx)
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum GateMessage {
    Trade(GateTrade),

    Snapshot(GateSnapshot),
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GateTrade {
    #[serde(alias = "time_ms", deserialize_with = "from_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    pub channel: String,

    pub event: String,

    pub result: GateTradeData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateTradeData {
    pub id: u64,

    #[serde(alias = "create_time_ms", deserialize_with = "from_str_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    pub side: Side,

    #[serde(alias = "currency_pair")]
    pub symbol: String,

    #[serde(deserialize_with = "from_str")]
    pub amount: f64,

    #[serde(deserialize_with = "from_str")]
    pub price: f64,
}

impl From<(Instrument, GateTradeData)> for MarketData {
    fn from((instrument, trade): (Instrument, GateTradeData)) -> Self {
        Self {
            venue: Venue::GateIO,
            instrument,
            venue_time: trade.timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: Some(trade.id.to_string()),
                price: trade.price,
                quantity: trade.amount,
                side: trade.side,
            }),
        }
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GateSnapshot {
    #[serde(alias = "time_ms", deserialize_with = "from_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    pub channel: String,

    pub event: String,

    pub result: GateSnapshotData,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateSnapshotData {
    #[serde(alias = "t", deserialize_with = "from_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    #[serde(alias = "lastUpdateId")]
    pub last_update_id: u64,

    #[serde(alias = "s")]
    pub symbol: String,

    pub bids: Vec<OrderBookLevel>,

    pub asks: Vec<OrderBookLevel>,
}

impl From<(Instrument, GateSnapshotData)> for MarketData {
    fn from((instrument, snapshot): (Instrument, GateSnapshotData)) -> Self {
        Self {
            venue: Venue::GateIO,
            instrument,
            venue_time: snapshot.timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::L2Snapshot(OrderBook {
                bids: snapshot.bids,
                asks: snapshot.asks,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"{"time":1672832218,"time_ms":1672832218641,"channel":"spot.trades","event":"update","result":{"id":4862947666,"create_time":1672832218,"create_time_ms":"1672832218630.0","side":"sell","currency_pair":"BTC_USDT","amount":"0.0001","price":"16850.4"}}"#;

        assert_eq!(
            serde_json::from_str::<GateMessage>(input).expect("failed to deserialise"),
            GateMessage::Trade(GateTrade {
                timestamp: DateTime::parse_from_rfc3339("2023-01-04T11:36:58.641Z")
                    .unwrap()
                    .with_timezone(&Utc),
                channel: "spot.trades".to_string(),
                event: "update".to_string(),
                result: GateTradeData {
                    id: 4862947666,
                    timestamp: DateTime::parse_from_rfc3339("2023-01-04T11:36:58.630Z")
                        .unwrap()
                        .with_timezone(&Utc),
                    side: Side::Sell,
                    symbol: "BTC_USDT".to_string(),
                    amount: 0.0001,
                    price: 16850.4
                }
            })
        );
    }

    #[test]
    fn deserialise_json_to_snapshot() {
        let input = r#"{"time":1672832986,"time_ms":1672832986322,"channel":"spot.order_book","event":"update","result":{"t":1672832986308,"lastUpdateId":10436902160,"s":"BTC_USDT","bids":[["16842.5","0.0001"],["16842.4","0.0001"]],"asks":[["16842.6","0.0001"],["16842.9","0.0004"]]}}"#;

        assert_eq!(
            serde_json::from_str::<GateMessage>(input).expect("failed to deserialise"),
            GateMessage::Snapshot(GateSnapshot {
                timestamp: DateTime::parse_from_rfc3339("2023-01-04T11:49:46.322Z")
                    .unwrap()
                    .with_timezone(&Utc),
                channel: "spot.order_book".to_string(),
                event: "update".to_string(),
                result: GateSnapshotData {
                    timestamp: DateTime::parse_from_rfc3339("2023-01-04T11:49:46.308Z")
                        .unwrap()
                        .with_timezone(&Utc),
                    last_update_id: 10436902160,
                    symbol: "BTC_USDT".to_string(),
                    bids: vec![
                        OrderBookLevel {
                            price: 16842.5,
                            quantity: 0.0001
                        },
                        OrderBookLevel {
                            price: 16842.4,
                            quantity: 0.0001
                        },
                    ],
                    asks: vec![
                        OrderBookLevel {
                            price: 16842.6,
                            quantity: 0.0001
                        },
                        OrderBookLevel {
                            price: 16842.9,
                            quantity: 0.0004
                        },
                    ]
                }
            })
        );
    }
}
