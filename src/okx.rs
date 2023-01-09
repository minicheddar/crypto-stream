use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_str, from_str_unix_epoch_ms, Instrument, MarketData, MarketDataKind, OrderBook,
    OrderBookLevel, Side, Trade, Venue,
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

pub struct Okx {
    stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl Okx {
    pub const BASE_WS_URL: &'static str = "wss://ws.okx.com:8443/ws/v5/public";

    pub const TRADE_CHANNEL: &'static str = "trades";
    pub const L2_QUOTE_CHANNEL: &'static str = "books";

    pub fn new() -> Self {
        Self {
            stream_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn build_stream_name(sub: &WebsocketSubscription) -> Result<(String, &str), String> {
        let market = format!(
            "{}-{}",
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
                    OkxMessage::Trade(trade) => {
                        let sub = map
                            .get(&format!(
                                "{}|{}",
                                trade.arg.instrument_id,
                                Self::TRADE_CHANNEL
                            ))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((
                            sub.instrument.clone(),
                            trade.data[0].clone(),
                        )));
                    }
                    OkxMessage::L2Update(update) => {
                        let sub = map
                            .get(&format!(
                                "{}|{}",
                                update.arg.instrument_id,
                                Self::L2_QUOTE_CHANNEL
                            ))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((
                            sub.instrument.clone(),
                            update.action,
                            update.data[0].clone(),
                        )));
                    }
                }
            }
            x => {
                // println!("other: {:?}", x);
                None
            }
        }
    }
}

#[async_trait]
impl WebsocketSubscriber for Okx {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        for sub in subscriptions {
            if sub.venue == Venue::Okx {
                let (market, channel) = Self::build_stream_name(&sub).unwrap();

                // update subscription map so we can match against it on each exchange message
                self.stream_subscriptions.lock().unwrap().insert(
                    format!("{}|{}", market.to_uppercase(), &channel),
                    sub.clone(),
                );

                // subscribe to market / channel
                let _ = socket.write_message(Message::Text(
                    json!({
                        "op": "subscribe",
                        "args": [{
                            "channel": &channel,
                            "instId": market
                        }],
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

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum OkxMessage {
    Trade(OkxTrade),

    L2Update(OkxL2Update),
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxArg {
    pub channel: String,

    #[serde(alias = "instId")]
    pub instrument_id: String,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OkxAction {
    Update,

    Snapshot,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxTrade {
    pub arg: OkxArg,
    pub data: Vec<OkxTradeData>,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxTradeData {
    #[serde(alias = "instId")]
    pub instrument_id: String,

    #[serde(alias = "tradeId", deserialize_with = "from_str")]
    pub trade_id: u64,

    #[serde(alias = "px", deserialize_with = "from_str")]
    pub price: f64,

    #[serde(alias = "sz", deserialize_with = "from_str")]
    pub size: f64,

    pub side: Side,

    #[serde(alias = "ts", deserialize_with = "from_str_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,
}

impl From<(Instrument, OkxTradeData)> for MarketData {
    fn from((instrument, trade): (Instrument, OkxTradeData)) -> Self {
        Self {
            venue: Venue::Okx,
            instrument,
            venue_time: trade.timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: Some(trade.trade_id.to_string()),
                price: trade.price,
                quantity: trade.size,
                side: trade.side,
            }),
        }
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxL2Update {
    pub arg: OkxArg,
    pub action: OkxAction,
    pub data: Vec<OkxL2Data>,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OkxL2Data {
    pub bids: Vec<OkxLevel>,

    pub asks: Vec<OkxLevel>,

    #[serde(alias = "ts", deserialize_with = "from_str_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    pub checksum: i64,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OkxLevel {
    #[serde(deserialize_with = "from_str")]
    pub price: f64,

    #[serde(deserialize_with = "from_str")]
    pub quantity: f64,

    #[serde(skip_serializing)]
    pub deprecated: String,

    #[serde(deserialize_with = "from_str")]
    pub num_orders_at_level: i64,
}

impl From<(Instrument, OkxAction, OkxL2Data)> for MarketData {
    fn from((instrument, action, update): (Instrument, OkxAction, OkxL2Data)) -> Self {
        let bids = update
            .bids
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let asks = update
            .asks
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let kind = match action {
            OkxAction::Snapshot => MarketDataKind::L2Snapshot(OrderBook { bids, asks }),
            OkxAction::Update => MarketDataKind::L2Update(OrderBook { bids, asks }),
        };

        Self {
            venue: Venue::Okx,
            instrument,
            venue_time: update.timestamp,
            received_time: Utc::now(),
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"391848877","px":"16741.9","sz":"0.01928","side":"sell","ts":"1672755605254"}]}"#;

        assert_eq!(
            serde_json::from_str::<OkxMessage>(input).expect("failed to deserialise"),
            OkxMessage::Trade(OkxTrade {
                arg: OkxArg {
                    channel: "trades".to_string(),
                    instrument_id: "BTC-USDT".to_string()
                },
                data: vec![OkxTradeData {
                    instrument_id: "BTC-USDT".to_string(),
                    trade_id: 391848877,
                    price: 16741.9,
                    size: 0.01928,
                    side: Side::Sell,
                    timestamp: DateTime::parse_from_rfc3339("2023-01-03T14:20:05.254Z")
                        .unwrap()
                        .with_timezone(&Utc),
                }]
            })
        );
    }

    #[test]
    fn deserialise_json_to_snapshot() {
        let input = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["16676","0.00021488","0","1"],["16676.1","0.01117729","0","1"]],"bids":[["16675.9","0.77561803","0","4"],["16675.3","0.09913111","0","1"]],"ts":"1672758244804","checksum":425110082}]}"#;

        assert_eq!(
            serde_json::from_str::<OkxMessage>(input).expect("failed to deserialise"),
            OkxMessage::L2Update(OkxL2Update {
                arg: OkxArg {
                    channel: "books".to_string(),
                    instrument_id: "BTC-USDT".to_string()
                },
                action: OkxAction::Snapshot,
                data: vec![OkxL2Data {
                    bids: vec![
                        OkxLevel {
                            price: 16675.9,
                            quantity: 0.77561803,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 4
                        },
                        OkxLevel {
                            price: 16675.3,
                            quantity: 0.09913111,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 1
                        }
                    ],
                    asks: vec![
                        OkxLevel {
                            price: 16676.0,
                            quantity: 0.00021488,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 1
                        },
                        OkxLevel {
                            price: 16676.1,
                            quantity: 0.01117729,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 1
                        }
                    ],
                    timestamp: DateTime::parse_from_rfc3339("2023-01-03T15:04:04.804Z")
                        .unwrap()
                        .with_timezone(&Utc),
                    checksum: 425110082
                }]
            })
        );
    }

    #[test]
    fn deserialise_json_to_l2_update() {
        let input = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["16678.7","0.35997156","0","1"],["16678.8","0.73367326","0","2"]],"bids":[["16675.1","0.1824","0","2"],["16674.1","1.06931486","0","4"]],"ts":"1672758245204","checksum":-2137937443}]}"#;

        assert_eq!(
            serde_json::from_str::<OkxMessage>(input).expect("failed to deserialise"),
            OkxMessage::L2Update(OkxL2Update {
                arg: OkxArg {
                    channel: "books".to_string(),
                    instrument_id: "BTC-USDT".to_string()
                },
                action: OkxAction::Update,
                data: vec![OkxL2Data {
                    bids: vec![
                        OkxLevel {
                            price: 16675.1,
                            quantity: 0.1824,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 2
                        },
                        OkxLevel {
                            price: 16674.1,
                            quantity: 1.06931486,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 4
                        }
                    ],
                    asks: vec![
                        OkxLevel {
                            price: 16678.7,
                            quantity: 0.35997156,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 1
                        },
                        OkxLevel {
                            price: 16678.8,
                            quantity: 0.73367326,
                            deprecated: "0".to_string(),
                            num_orders_at_level: 2
                        }
                    ],
                    timestamp: DateTime::parse_from_rfc3339("2023-01-03T15:04:05.204Z")
                        .unwrap()
                        .with_timezone(&Utc),
                    checksum: -2137937443
                }]
            })
        );
    }
}
