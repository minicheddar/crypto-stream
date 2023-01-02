use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_str, Instrument, MarketData, MarketDataKind, OrderBook, OrderBookLevel, Side, Trade, Venue,
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

pub struct Coinbase {
    pub stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl Coinbase {
    pub const BASE_WS_URL: &'static str = "wss://ws-feed.exchange.coinbase.com";
    pub const TRADE_CHANNEL: &'static str = "matches";
    pub const L2_QUOTE_CHANNEL: &'static str = "level2";

    pub fn new() -> Self {
        Self {
            stream_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn build_stream_name(sub: &WebsocketSubscription) -> Result<(String, &str), String> {
        let product_id = format!(
            "{}-{}",
            sub.instrument.base.to_uppercase(),
            sub.instrument.quote.to_uppercase()
        );

        let channel = match sub.kind {
            WebsocketSubscriptionKind::Trade => Self::TRADE_CHANNEL,
            WebsocketSubscriptionKind::L2Quote => Self::L2_QUOTE_CHANNEL,
            // _ => panic!("WebsocketSubscriptionKind not supported for exchange"),
        };

        Ok((product_id, channel))
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
                    CoinbaseMessage::Trade(trade) => {
                        let sub = map
                            .get(&format!("{}|trade", trade.product_id))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), trade)));
                    }
                    CoinbaseMessage::Snapshot(snapshot) => {
                        let sub = map
                            .get(&format!("{}|l2update", snapshot.product_id))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), snapshot)));
                    }
                    CoinbaseMessage::L2Update(l2) => {
                        let sub = map
                            .get(&format!("{}|l2update", l2.product_id))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), l2)));
                    }
                }
            }
            x => {
                println!("{:?}", x);
                None
            }
        }
    }
}

#[async_trait]
impl WebsocketSubscriber for Coinbase {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        for sub in subscriptions {
            if sub.venue == Venue::Coinbase {
                let (market, channel) = Self::build_stream_name(&sub).unwrap();

                // update subscription map so we can match against it on each exchange message
                let sub_channel;
                match channel {
                    Self::TRADE_CHANNEL => sub_channel = "trade",
                    Self::L2_QUOTE_CHANNEL => sub_channel = "l2update",

                    _ => panic!("Unexpected channel"),
                }
                self.stream_subscriptions
                    .lock()
                    .unwrap()
                    .insert(format!("{}|{}", market, &sub_channel), sub.clone());

                // subscribe to market / channel
                let _ = socket.write_message(tungstenite::Message::Text(
                    json!({
                        "type": "subscribe",
                        "product_ids": [market],
                        "channels": [channel],
                    })
                    .to_string(),
                ));

                // check that subscription response does not throw an error
                if let Message::Text(json) = socket.read_message().expect("Error reading message") {
                    if let CoinbaseSubscriptionResponse::Error { reason } =
                        serde_json::from_str(&json).unwrap()
                    {
                        panic!("{reason}");
                    }
                }
            }
        }

        let exchange_rx = self.handle_messages(socket).await;

        Ok(exchange_rx)
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CoinbaseSubscriptionResponse {
    #[serde(alias = "subscriptions")]
    Subscribed {
        channels: serde_json::Value,
    },
    Error {
        reason: String,
    },
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum CoinbaseMessage {
    #[serde(alias = "last_match", alias = "match")]
    Trade(CoinbaseTrade),

    #[serde(alias = "snapshot")]
    Snapshot(CoinbaseSnapshot),

    #[serde(alias = "l2update")]
    L2Update(CoinbaseL2Update),
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct CoinbaseTrade {
    pub trade_id: u64,

    pub maker_order_id: String,

    pub taker_order_id: String,

    pub side: Side,

    #[serde(deserialize_with = "from_str")]
    pub size: f64,

    #[serde(deserialize_with = "from_str")]
    pub price: f64,

    pub product_id: String,

    pub sequence: u64,

    #[serde(deserialize_with = "from_str")]
    pub time: DateTime<Utc>,
}

impl From<(Instrument, CoinbaseTrade)> for MarketData {
    fn from((instrument, trade): (Instrument, CoinbaseTrade)) -> Self {
        Self {
            venue: Venue::Coinbase,
            instrument,
            venue_time: trade.time,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: trade.trade_id.to_string(),
                price: trade.price,
                quantity: trade.size,
                side: trade.side,
            }),
        }
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct CoinbaseSnapshot {
    pub product_id: String,

    #[serde(alias = "bids")]
    pub bids: Vec<OrderBookLevel>,

    #[serde(alias = "asks")]
    pub asks: Vec<OrderBookLevel>,
}

impl From<(Instrument, CoinbaseSnapshot)> for MarketData {
    fn from((instrument, snapshot): (Instrument, CoinbaseSnapshot)) -> Self {
        Self {
            venue: Venue::Coinbase,
            instrument,
            venue_time: Utc::now(),
            received_time: Utc::now(),
            kind: MarketDataKind::QuoteL2(OrderBook {
                bids: snapshot.bids,
                asks: snapshot.asks,
            }),
        }
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct CoinbaseL2Change {
    side: Side,

    #[serde(deserialize_with = "from_str")]
    price: f64,

    #[serde(deserialize_with = "from_str")]
    amount: f64,
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct CoinbaseL2Update {
    pub product_id: String,

    #[serde(deserialize_with = "from_str")]
    pub time: DateTime<Utc>,

    pub changes: Vec<CoinbaseL2Change>,
}

impl From<(Instrument, CoinbaseL2Update)> for MarketData {
    fn from((instrument, l2): (Instrument, CoinbaseL2Update)) -> Self {
        let bids = l2
            .changes
            .iter()
            .filter(|x| x.side == Side::Buy)
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.amount,
            })
            .collect();

        let asks = l2
            .changes
            .iter()
            .filter(|x| x.side == Side::Sell)
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.amount,
            })
            .collect();

        Self {
            venue: Venue::Coinbase,
            instrument,
            venue_time: Utc::now(),
            received_time: Utc::now(),
            kind: MarketDataKind::QuoteL2(OrderBook { bids, asks }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{OrderBookLevel, Side};

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"{"type":"match","trade_id":477865970,"maker_order_id":"a111915e-d10b-4a35-9656-ef276e379c4a","taker_order_id":"38a7db2a-b909-4464-bc58-25ff73d350da","side":"buy","size":"0.00020377","price":"16695.41","product_id":"BTC-USD","sequence":52237942526,"time":"2023-01-02T14:43:29.610435Z"}"#;

        assert_eq!(
            serde_json::from_str::<CoinbaseMessage>(input).expect("failed to deserialise"),
            CoinbaseMessage::Trade(CoinbaseTrade {
                trade_id: 477865970,
                maker_order_id: "a111915e-d10b-4a35-9656-ef276e379c4a".to_string(),
                taker_order_id: "38a7db2a-b909-4464-bc58-25ff73d350da".to_string(),
                side: Side::Buy,
                size: 0.00020377,
                price: 16695.41,
                product_id: "BTC-USD".to_string(),
                sequence: 52237942526,
                time: DateTime::parse_from_rfc3339("2023-01-02T14:43:29.610435Z")
                    .unwrap()
                    .with_timezone(&Utc)
            })
        );
    }

    #[test]
    fn deserialise_json_to_snapshot() {
        let input = r#"{"type":"snapshot","product_id":"BTC-USD","asks":[["16688.91","0.04395852"],["16688.92","0.00219120"]],"bids":[["16688.11","0.19317565"],["16688.10","0.00492671"]]}"#;

        assert_eq!(
            serde_json::from_str::<CoinbaseMessage>(input).expect("failed to deserialise"),
            CoinbaseMessage::Snapshot(CoinbaseSnapshot {
                product_id: "BTC-USD".to_string(),
                bids: vec![
                    OrderBookLevel {
                        price: 16688.11,
                        quantity: 0.19317565,
                    },
                    OrderBookLevel {
                        price: 16688.10,
                        quantity: 0.00492671,
                    },
                ],
                asks: vec![
                    OrderBookLevel {
                        price: 16688.91,
                        quantity: 0.04395852,
                    },
                    OrderBookLevel {
                        price: 16688.92,
                        quantity: 0.00219120,
                    },
                ]
            })
        );
    }

    #[test]
    fn deserialise_json_to_l2_update() {
        let input = r#"{"type":"l2update","product_id":"BTC-USD","changes":[["buy","16694.62","0.01"]],"time":"2023-01-02T15:11:20.883271Z"}"#;

        assert_eq!(
            serde_json::from_str::<CoinbaseMessage>(input).expect("failed to deserialise"),
            CoinbaseMessage::L2Update(CoinbaseL2Update {
                product_id: "BTC-USD".to_string(),
                time: DateTime::parse_from_rfc3339("2023-01-02T15:11:20.883271Z")
                    .unwrap()
                    .with_timezone(&Utc),
                changes: vec![CoinbaseL2Change {
                    side: Side::Buy,
                    price: 16694.62,
                    amount: 0.01
                }]
            })
        )
    }
}
