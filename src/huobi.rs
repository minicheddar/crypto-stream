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
            Message::Binary(bytes) => {
                let mut gz = GzDecoder::new(&bytes[..]);
                let mut json = String::new();
                gz.read_to_string(&mut json)
                    .expect("Error decompressing huobi message");
                println!("{}", json);
                let map = stream_subscriptions.lock().unwrap();

                // let msg: HuobiMessage = serde_json::from_str(&json).unwrap();
                // println!("{:?}", &msg);

                match serde_json::from_str(&json).unwrap() {
                    HuobiMessage::Trade(trade) => {
                        let sub = map
                            .get(&trade.channel)
                            .expect("unable to find matching subscription");
                        println!("{:?}", &trade);

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
                }

                // match serde_json::from_str(&json).unwrap() {
                //     OkxMessage::Trade(trade) => {
                //         let sub = map
                //             .get(&format!(
                //                 "{}|{}",
                //                 trade.arg.instrument_id,
                //                 Self::TRADE_CHANNEL
                //             ))
                //             .expect("unable to find matching subscription");

                //         return Some(MarketData::from((
                //             sub.instrument.clone(),
                //             trade.data[0].clone(),
                //         )));
                //     }
                //     OkxMessage::L2Update(quote) => {
                //         let sub = map
                //             .get(&format!(
                //                 "{}|{}",
                //                 quote.arg.instrument_id,
                //                 Self::L2_QUOTE_CHANNEL
                //             ))
                //             .expect("unable to find matching subscription");

                //         return Some(MarketData::from((
                //             sub.instrument.clone(),
                //             quote.data[0].clone(),
                //         )));
                //     }
                // }
                None
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
    // L2Update(OkxL2Update),
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

    pub tick: Tick,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tick {
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
                id: trade.trade_id.to_string(),
                price: trade.price,
                quantity: trade.amount,
                side: trade.side,
            }),
        }
    }
}

// #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
// #[serde(rename_all = "camelCase")]
// pub struct OkxL2Update {
//     pub arg: OkxArg,
//     pub action: OkxAction,
//     pub data: Vec<OkxL2Data>,
// }

// #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
// #[serde(rename_all = "camelCase")]
// pub struct OkxL2Data {
//     pub bids: Vec<OkxLevel>,

//     pub asks: Vec<OkxLevel>,

//     #[serde(alias = "ts", deserialize_with = "from_str_unix_epoch_ms")]
//     pub timestamp: DateTime<Utc>,

//     pub checksum: i64,
// }

// #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
// pub struct OkxLevel {
//     #[serde(deserialize_with = "from_str")]
//     pub price: f64,

//     #[serde(deserialize_with = "from_str")]
//     pub quantity: f64,

//     #[serde(skip_serializing)]
//     pub deprecated: String,

//     #[serde(deserialize_with = "from_str")]
//     pub num_orders_at_level: i64,
// }

// impl From<(Instrument, OkxL2Data)> for MarketData {
//     fn from((instrument, quote): (Instrument, OkxL2Data)) -> Self {
//         let bids = quote
//             .bids
//             .iter()
//             .map(|x| OrderBookLevel {
//                 price: x.price,
//                 quantity: x.quantity,
//             })
//             .collect();

//         let asks = quote
//             .asks
//             .iter()
//             .map(|x| OrderBookLevel {
//                 price: x.price,
//                 quantity: x.quantity,
//             })
//             .collect();

//         Self {
//             venue: Venue::Okx,
//             instrument,
//             venue_time: quote.timestamp,
//             received_time: Utc::now(),
//             kind: MarketDataKind::QuoteL2(OrderBook { bids, asks }),
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"{"ch":"market.btcusdt.trade.detail","ts":1672824147727,"tick":{"id":161573809785,"ts":1672824147724,"data":[{"id":161573809785708645025711503,"ts":1672824147724,"tradeId":102751627361,"amount":0.001,"price":16855.48,"direction":"buy"}]}}"#;

        // HuobiTrade {
        //     arg: OkxArg {
        //         channel: "trades".to_string(),
        //         instrument_id: "BTC-USDT".to_string()
        //     },
        //     data: vec![OkxTradeData {
        //         instrument_id: "BTC-USDT".to_string(),
        //         trade_id: 391848877,
        //         price: 16741.9,
        //         size: 0.01928,
        //         side: Side::Sell,
        //         timestamp: DateTime::parse_from_rfc3339("2023-01-03T14:20:05.254Z")
        //             .unwrap()
        //             .with_timezone(&Utc),
        //     }]
        // })
        assert_eq!(
            serde_json::from_str::<HuobiMessage>(input).expect("failed to deserialise"),
            HuobiMessage::Trade(HuobiTrade {
                channel: "market.btcusdt.trade.detail".to_string(),
                response_timestamp: DateTime::parse_from_rfc3339("2023-01-04T09:22:27.727Z")
                    .unwrap()
                    .with_timezone(&Utc),
                tick: Tick {
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

    // #[test]
    // fn deserialise_json_to_snapshot() {
    //     let input = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["16676","0.00021488","0","1"],["16676.1","0.01117729","0","1"]],"bids":[["16675.9","0.77561803","0","4"],["16675.3","0.09913111","0","1"]],"ts":"1672758244804","checksum":425110082}]}"#;

    //     assert_eq!(
    //         serde_json::from_str::<OkxMessage>(input).expect("failed to deserialise"),
    //         OkxMessage::L2Update(OkxL2Update {
    //             arg: OkxArg {
    //                 channel: "books".to_string(),
    //                 instrument_id: "BTC-USDT".to_string()
    //             },
    //             action: OkxAction::Snapshot,
    //             data: vec![OkxL2Data {
    //                 bids: vec![
    //                     OkxLevel {
    //                         price: 16675.9,
    //                         quantity: 0.77561803,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 4
    //                     },
    //                     OkxLevel {
    //                         price: 16675.3,
    //                         quantity: 0.09913111,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 1
    //                     }
    //                 ],
    //                 asks: vec![
    //                     OkxLevel {
    //                         price: 16676.0,
    //                         quantity: 0.00021488,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 1
    //                     },
    //                     OkxLevel {
    //                         price: 16676.1,
    //                         quantity: 0.01117729,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 1
    //                     }
    //                 ],
    //                 timestamp: DateTime::parse_from_rfc3339("2023-01-03T15:04:04.804Z")
    //                     .unwrap()
    //                     .with_timezone(&Utc),
    //                 checksum: 425110082
    //             }]
    //         })
    //     );
    // }

    // #[test]
    // fn deserialise_json_to_l2_update() {
    //     let input = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["16678.7","0.35997156","0","1"],["16678.8","0.73367326","0","2"]],"bids":[["16675.1","0.1824","0","2"],["16674.1","1.06931486","0","4"]],"ts":"1672758245204","checksum":-2137937443}]}"#;

    //     assert_eq!(
    //         serde_json::from_str::<OkxMessage>(input).expect("failed to deserialise"),
    //         OkxMessage::L2Update(OkxL2Update {
    //             arg: OkxArg {
    //                 channel: "books".to_string(),
    //                 instrument_id: "BTC-USDT".to_string()
    //             },
    //             action: OkxAction::Update,
    //             data: vec![OkxL2Data {
    //                 bids: vec![
    //                     OkxLevel {
    //                         price: 16675.1,
    //                         quantity: 0.1824,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 2
    //                     },
    //                     OkxLevel {
    //                         price: 16674.1,
    //                         quantity: 1.06931486,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 4
    //                     }
    //                 ],
    //                 asks: vec![
    //                     OkxLevel {
    //                         price: 16678.7,
    //                         quantity: 0.35997156,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 1
    //                     },
    //                     OkxLevel {
    //                         price: 16678.8,
    //                         quantity: 0.73367326,
    //                         deprecated: "0".to_string(),
    //                         num_orders_at_level: 2
    //                     }
    //                 ],
    //                 timestamp: DateTime::parse_from_rfc3339("2023-01-03T15:04:05.204Z")
    //                     .unwrap()
    //                     .with_timezone(&Utc),
    //                 checksum: -2137937443
    //             }]
    //         })
    //     );
    // }
}
