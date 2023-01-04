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

pub struct BinanceSpot {
    stream_subscriptions: Arc<Mutex<HashMap<String, WebsocketSubscription>>>,
}

impl BinanceSpot {
    pub const BASE_WS_URL: &'static str = "wss://stream.binance.com:9443/ws";

    pub const TRADE_CHANNEL: &'static str = "@trade";
    pub const L2_QUOTE_CHANNEL: &'static str = "@depth@100ms";

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
impl WebsocketSubscriber for BinanceSpot {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error> {
        let mut socket = WebsocketClient::connect(Self::BASE_WS_URL)
            .await
            .expect("unable to connect");

        let mut sub_id: u8 = 1;
        for sub in subscriptions {
            if sub.venue == Venue::BinanceSpot {
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
pub struct BinanceTrade {
    #[serde(alias = "E", deserialize_with = "from_unix_epoch_ms")]
    pub event_time: DateTime<Utc>,

    #[serde(alias = "s", deserialize_with = "from_str")]
    pub symbol: String,

    #[serde(alias = "t")]
    pub trade_id: u64,

    #[serde(alias = "p", deserialize_with = "from_str")]
    pub price: f64,

    #[serde(alias = "q", deserialize_with = "from_str")]
    pub quantity: f64,

    #[serde(alias = "b")]
    pub buyer_order_id: u64,

    #[serde(alias = "a")]
    pub seller_order_id: u64,

    #[serde(alias = "T", deserialize_with = "from_unix_epoch_ms")]
    pub transaction_time: DateTime<Utc>,

    #[serde(alias = "m", deserialize_with = "is_buyer_maker")]
    pub side: Side,
}

impl From<(Instrument, BinanceTrade)> for MarketData {
    fn from((instrument, trade): (Instrument, BinanceTrade)) -> Self {
        Self {
            venue: Venue::BinanceSpot,
            instrument,
            venue_time: trade.event_time,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: Some(trade.trade_id.to_string()),
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

    #[serde(alias = "s", deserialize_with = "from_str")]
    pub symbol: String,

    #[serde(alias = "U")]
    pub first_update_id: u64,

    #[serde(alias = "u")]
    pub final_update_id: u64,

    #[serde(alias = "b")]
    pub bids: Vec<OrderBookLevel>,

    #[serde(alias = "a")]
    pub asks: Vec<OrderBookLevel>,
}

impl From<(Instrument, BinanceL2Quote)> for MarketData {
    fn from((instrument, quote): (Instrument, BinanceL2Quote)) -> Self {
        Self {
            venue: Venue::BinanceSpot,
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
        let input = r#"{"e":"trade","E":1672745782430,"s":"BTCUSDT","t":2417705108,"p":"16735.16000000","q":"0.04678000","b":16965571202,"a":16965571059,"T":1672745782430,"m":false,"M":true}"#;

        assert_eq!(
            serde_json::from_str::<BinanceMessage>(input).expect("failed to deserialise"),
            BinanceMessage::Trade(BinanceTrade {
                event_time: Utc.timestamp_millis_opt(1672745782430).unwrap(),
                symbol: "BTCUSDT".to_string(),
                trade_id: 2417705108,
                price: 16735.16000000,
                quantity: 0.04678000,
                buyer_order_id: 16965571202,
                seller_order_id: 16965571059,
                transaction_time: Utc.timestamp_millis_opt(1672745782430).unwrap(),
                side: Side::Buy,
            })
        );
    }

    #[test]
    fn deserialise_json_to_l2_quote() {
        let input = r#"{"e":"depthUpdate","E":1672745259450,"s":"BNBUSDT","U":29961902358,"u":29961902414,"b":[["246.220","32.04"],["246.210","42.66"]],"a":[["246.230","40.84"],["246.240","39.10"]]}"#;

        assert_eq!(
            serde_json::from_str::<BinanceMessage>(input).expect("failed to deserialise"),
            BinanceMessage::L2Quote(BinanceL2Quote {
                event_time: Utc.timestamp_millis_opt(1672745259450).unwrap(),
                symbol: "BNBUSDT".to_string(),
                first_update_id: 29961902358,
                final_update_id: 29961902414,
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
