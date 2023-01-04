use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_str, from_str_unix_epoch_sec, from_unix_epoch_ms, Instrument, MarketData, MarketDataKind,
    OrderBook, OrderBookLevel, OrderKind, Side, Trade, Venue,
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
    pub const L2_QUOTE_CHANNEL: &'static str = "book-10";

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
                println!("{}", json);
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
                    // BinanceMessage::L2Quote(quote) => {
                    //     let sub = map
                    //         .get(&format!("{}|{}", quote.symbol, Self::L2_QUOTE_CHANNEL))
                    //         .expect("unable to find matching subscription");

                    //     return Some(MarketData::from((sub.instrument.clone(), quote)));
                    // }
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
                self.stream_subscriptions
                    .lock()
                    .unwrap()
                    .insert(format!("{}|{}", market, &channel), sub.clone());

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
    // #[serde(alias = "depthUpdate")]
    // L2Quote(BinanceL2Quote),
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
pub struct BinanceL1Quote {
    #[serde(alias = "E", deserialize_with = "from_unix_epoch_ms")]
    pub event_time: DateTime<Utc>,

    #[serde(alias = "T", deserialize_with = "from_unix_epoch_ms")]
    pub transacton_time: DateTime<Utc>,

    #[serde(alias = "s", deserialize_with = "from_str")]
    pub symbol: String,

    #[serde(alias = "u")]
    pub orderbook_update_id: u64,

    #[serde(alias = "b", deserialize_with = "from_str")]
    pub bid_price: f64,

    #[serde(alias = "B", deserialize_with = "from_str")]
    pub bid_quantity: f64,

    #[serde(alias = "a", deserialize_with = "from_str")]
    pub ask_price: f64,

    #[serde(alias = "A", deserialize_with = "from_str")]
    pub ask_quantity: f64,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceL2Quote {
    #[serde(alias = "E", deserialize_with = "from_unix_epoch_ms")]
    pub event_time: DateTime<Utc>,

    #[serde(alias = "T", deserialize_with = "from_unix_epoch_ms")]
    pub transaction_time: DateTime<Utc>,

    #[serde(alias = "s", deserialize_with = "from_str")]
    pub symbol: String,

    #[serde(alias = "U")]
    pub first_update_id: u64,

    #[serde(alias = "u")]
    pub final_update_id: u64,

    #[serde(alias = "pu")]
    pub final_update_id_last_stream: u64,

    #[serde(alias = "b")]
    pub bids: Vec<OrderBookLevel>,

    #[serde(alias = "a")]
    pub asks: Vec<OrderBookLevel>,
}

impl From<(Instrument, BinanceL2Quote)> for MarketData {
    fn from((instrument, quote): (Instrument, BinanceL2Quote)) -> Self {
        Self {
            venue: Venue::Kraken,
            instrument,
            venue_time: quote.event_time,
            received_time: Utc::now(),
            kind: MarketDataKind::QuoteL2(OrderBook {
                bids: quote.bids,
                asks: quote.asks,
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

    // #[test]
    // fn deserialise_json_to_l2_quote() {
    //     let input = r#"{"e":"depthUpdate","E":1672670043998,"T":1672670043992,"s":"BNBUSDT","U":2323268998826,"u":2323269001838,"pu":2323268998603,"b":[["246.220","32.04"],["246.210","42.66"]],"a":[["246.230","40.84"],["246.240","39.10"]]}"#;

    //     assert_eq!(
    //         serde_json::from_str::<KrakenMessage>(input).expect("failed to deserialise"),
    //         KrakenMessage::L2Quote(BinanceL2Quote {
    //             event_time: Utc.timestamp_millis_opt(1672670043998).unwrap(),
    //             transaction_time: Utc.timestamp_millis_opt(1672670043992).unwrap(),
    //             symbol: "BNBUSDT".to_string(),
    //             first_update_id: 2323268998826,
    //             final_update_id: 2323269001838,
    //             final_update_id_last_stream: 2323268998603,
    //             bids: vec![
    //                 OrderBookLevel {
    //                     price: 246.220,
    //                     quantity: 32.04,
    //                 },
    //                 OrderBookLevel {
    //                     price: 246.210,
    //                     quantity: 42.66,
    //                 },
    //             ],
    //             asks: vec![
    //                 OrderBookLevel {
    //                     price: 246.230,
    //                     quantity: 40.84,
    //                 },
    //                 OrderBookLevel {
    //                     price: 246.240,
    //                     quantity: 39.10,
    //                 },
    //             ]
    //         })
    //     )
    // }
}
