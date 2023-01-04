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
    const QUOTE_DEPTH: &'static str = "5";

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
                    GateIOMessage::Trade(trade) => {
                        let sub = map
                            .get(&format!("{}|{}", trade.result.symbol, trade.channel))
                            .expect("unable to find matching subscription");

                        println!("{:?}", trade);

                        return Some(MarketData::from((sub.instrument.clone(), trade.result)));
                    }
                    GateIOMessage::L2Update(quote) => {
                        let sub = map
                            .get(&quote.channel)
                            .expect("unable to find matching subscription");

                        return Some(MarketData::from((
                            sub.instrument.clone(),
                            quote.response_timestamp,
                            quote.tick.clone(),
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
                    WebsocketSubscriptionKind::L2Quote => {
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
pub enum GateIOMessage {
    Trade(GateIOTrade),

    L2Update(HuobiL2Update), // L2Update(OkxL2Update),
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GateIOTrade {
    #[serde(alias = "time_ms", deserialize_with = "from_unix_epoch_ms")]
    pub timestamp: DateTime<Utc>,

    pub channel: String,

    pub event: String,

    pub result: GateIOTradeData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateIOTradeData {
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

impl From<(Instrument, GateIOTradeData)> for MarketData {
    fn from((instrument, trade): (Instrument, GateIOTradeData)) -> Self {
        Self {
            venue: Venue::GateIO,
            instrument,
            venue_time: trade.timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::Trade(Trade {
                id: trade.id.to_string(),
                price: trade.price,
                quantity: trade.amount,
                side: trade.side,
            }),
        }
    }
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiL2Update {
    #[serde(alias = "ch")]
    pub channel: String,

    #[serde(alias = "ts", deserialize_with = "from_unix_epoch_ms")]
    pub response_timestamp: DateTime<Utc>,

    pub tick: HuobiL2Tick,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HuobiL2Tick {
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

impl From<(Instrument, DateTime<Utc>, HuobiL2Tick)> for MarketData {
    fn from((instrument, timestamp, quote): (Instrument, DateTime<Utc>, HuobiL2Tick)) -> Self {
        let bids = quote
            .bids
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        let asks = quote
            .asks
            .iter()
            .map(|x| OrderBookLevel {
                price: x.price,
                quantity: x.quantity,
            })
            .collect();

        Self {
            venue: Venue::Okx,
            instrument,
            venue_time: timestamp,
            received_time: Utc::now(),
            kind: MarketDataKind::QuoteL2(OrderBook { bids, asks }),
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
            serde_json::from_str::<GateIOMessage>(input).expect("failed to deserialise"),
            GateIOMessage::Trade(GateIOTrade {
                timestamp: DateTime::parse_from_rfc3339("2023-01-04T11:36:58.641Z")
                    .unwrap()
                    .with_timezone(&Utc),
                channel: "spot.trades".to_string(),
                event: "update".to_string(),
                result: GateIOTradeData {
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
    fn deserialise_json_to_l2_update() {
        let input = r#"{"ch":"market.btcusdt.mbp.refresh.10","ts":1672825916217,"tick":{"seqNum":161573989890,"bids":[[16840.68,0.76565],[16840.0,0.653206]],"asks":[[16840.69,1.365447],[16841.55,0.3]]}}"#;

        assert_eq!(
            serde_json::from_str::<GateIOMessage>(input).expect("failed to deserialise"),
            GateIOMessage::L2Update(HuobiL2Update {
                channel: "market.btcusdt.mbp.refresh.10".to_string(),
                response_timestamp: DateTime::parse_from_rfc3339("2023-01-04T09:51:56.217Z")
                    .unwrap()
                    .with_timezone(&Utc),
                tick: HuobiL2Tick {
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
