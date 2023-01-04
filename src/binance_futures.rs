use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use crate::model::{
    from_str, from_unix_epoch_ms, is_buyer_maker, Instrument, MarketData, MarketDataKind,
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

pub struct BinanceFutures {
    stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl BinanceFutures {
    pub const BASE_WS_URL: &'static str = "wss://fstream.binance.com/ws";

    pub const TRADE_CHANNEL: &'static str = "@trade";
    pub const L2_QUOTE_CHANNEL: &'static str = "@depth10@100ms";

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
            Message::Text(json) => {
                // println!("{}", json);
                let map = stream_subscriptions.lock().unwrap();

                match serde_json::from_str(&json).unwrap() {
                    BinanceMessage::Trade(trade) => {
                        let sub = map
                            .get(&format!("{}|{}", trade.symbol, Self::TRADE_CHANNEL))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), trade)));
                    }
                    BinanceMessage::L2Quote(quote) => {
                        let sub = map
                            .get(&format!("{}|{}", quote.symbol, Self::L2_QUOTE_CHANNEL))
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((sub.instrument.clone(), quote)));
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
impl WebsocketSubscriber for BinanceFutures {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        let mut sub_id: u8 = 1;
        for sub in subscriptions {
            if sub.venue == Venue::BinanceFuturesUsd {
                let (market, channel) = Self::build_stream_name(&sub).unwrap();

                // update subscription map so we can match against it on each exchange message
                self.stream_subscriptions.lock().unwrap().insert(
                    format!("{}|{}", market.to_uppercase(), &channel),
                    sub.clone(),
                );

                // subscribe to market / channel
                let _ = socket.write_message(Message::Text(
                    json!({
                        "method": "SUBSCRIBE",
                        "params": [format!("{}{}", market, &channel)],
                        "id": sub_id
                    })
                    .to_string(),
                ));

                if let Message::Text(json) = socket.read_message().expect("Error reading message") {
                    println!("{:?}", json);
                }

                sub_id += 1;
            }
        }

        let exchange_rx = self.handle_messages(socket).await;

        Ok(exchange_rx)
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
#[serde(tag = "e", rename_all = "camelCase")]
pub enum BinanceMessage {
    #[serde(alias = "trade")]
    Trade(BinanceTrade),

    #[serde(alias = "depthUpdate")]
    L2Quote(BinanceL2Quote),
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
pub struct BinanceAggregatedTrade {
    #[serde(alias = "E", deserialize_with = "from_unix_epoch_ms")]
    pub event_time: DateTime<Utc>,

    #[serde(alias = "T", deserialize_with = "from_unix_epoch_ms")]
    pub transaction_time: DateTime<Utc>,

    #[serde(alias = "s", deserialize_with = "from_str")]
    pub symbol: String,

    #[serde(alias = "p", deserialize_with = "from_str")]
    pub price: f64,

    #[serde(alias = "q", deserialize_with = "from_str")]
    pub quantity: f64,

    #[serde(alias = "a")]
    pub id: u64,

    #[serde(alias = "f")]
    pub first_trade_id: u64,

    #[serde(alias = "l")]
    pub last_trade_id: u64,

    #[serde(alias = "m", deserialize_with = "is_buyer_maker")]
    pub is_buyer_maker: Side,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceTrade {
    #[serde(alias = "E", deserialize_with = "from_unix_epoch_ms")]
    pub event_time: DateTime<Utc>,

    #[serde(alias = "T", deserialize_with = "from_unix_epoch_ms")]
    pub transaction_time: DateTime<Utc>,

    #[serde(alias = "s", deserialize_with = "from_str")]
    pub symbol: String,

    #[serde(alias = "t")]
    pub id: u64,

    #[serde(alias = "p", deserialize_with = "from_str")]
    pub price: f64,

    #[serde(alias = "q", deserialize_with = "from_str")]
    pub quantity: f64,

    #[serde(alias = "m", deserialize_with = "is_buyer_maker")]
    pub side: Side,
}

impl From<(Instrument, BinanceTrade)> for MarketData {
    fn from((instrument, trade): (Instrument, BinanceTrade)) -> Self {
        Self {
            venue: Venue::BinanceFuturesUsd,
            instrument,
            venue_time: trade.event_time,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: Some(trade.id.to_string()),
                price: trade.price,
                quantity: trade.quantity,
                side: trade.side,
            }),
        }
    }
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
            venue: Venue::BinanceFuturesUsd,
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
    use crate::model::{OrderBookLevel, Side};
    use chrono::TimeZone;

    #[test]
    fn deserialise_json_to_trade() {
        let input = r#"{"e":"trade","E":1672668653672,"T":1672668653667,"s":"BTCUSDT","t":3168007982,"p":"16692.00","q":"0.001","X":"MARKET","m":true}"#;

        assert_eq!(
            serde_json::from_str::<BinanceMessage>(input).expect("failed to deserialise"),
            BinanceMessage::Trade(BinanceTrade {
                event_time: Utc.timestamp_millis_opt(1672668653672).unwrap(),
                transaction_time: Utc.timestamp_millis_opt(1672668653667).unwrap(),
                symbol: "BTCUSDT".to_string(),
                id: 3168007982,
                price: 16692.00,
                quantity: 0.001,
                side: Side::Sell,
            })
        );
    }

    #[test]
    fn deserialise_json_to_l2_quote() {
        let input = r#"{"e":"depthUpdate","E":1672670043998,"T":1672670043992,"s":"BNBUSDT","U":2323268998826,"u":2323269001838,"pu":2323268998603,"b":[["246.220","32.04"],["246.210","42.66"]],"a":[["246.230","40.84"],["246.240","39.10"]]}"#;

        assert_eq!(
            serde_json::from_str::<BinanceMessage>(input).expect("failed to deserialise"),
            BinanceMessage::L2Quote(BinanceL2Quote {
                event_time: Utc.timestamp_millis_opt(1672670043998).unwrap(),
                transaction_time: Utc.timestamp_millis_opt(1672670043992).unwrap(),
                symbol: "BNBUSDT".to_string(),
                first_update_id: 2323268998826,
                final_update_id: 2323269001838,
                final_update_id_last_stream: 2323268998603,
                bids: vec![
                    OrderBookLevel {
                        price: 246.220,
                        quantity: 32.04,
                    },
                    OrderBookLevel {
                        price: 246.210,
                        quantity: 42.66,
                    },
                ],
                asks: vec![
                    OrderBookLevel {
                        price: 246.230,
                        quantity: 40.84,
                    },
                    OrderBookLevel {
                        price: 246.240,
                        quantity: 39.10,
                    },
                ]
            })
        )
    }
}
