use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::WebSocket;
use url::Url;

use crate::model::{Instrument, InstrumentKind, MarketData, Venue};

pub type WebsocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type Websocket = WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>;

pub struct WebsocketClient;
impl WebsocketClient {
    pub async fn connect(endpoint: &str) -> Result<Websocket, tungstenite::Error> {
        let url = Url::parse(endpoint).unwrap();
        let (stream, _) = tungstenite::connect(url)?;
        println!("Connected to {endpoint}");
        Ok(stream)
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub enum WebsocketSubscriptionKind {
    Trade,
    L2Quote,
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub struct WebsocketSubscription {
    pub venue: Venue,
    pub instrument: Instrument,
    pub kind: WebsocketSubscriptionKind,
}

impl WebsocketSubscription {
    pub fn new(
        venue: Venue,
        base_symbol: &str,
        quote_symbol: &str,
        instrument_kind: InstrumentKind,
        sub_kind: WebsocketSubscriptionKind,
    ) -> Self {
        Self {
            venue,
            instrument: Instrument {
                base: base_symbol.into(),
                quote: quote_symbol.into(),
                kind: instrument_kind,
            },
            kind: sub_kind,
        }
    }
}

#[async_trait]
pub trait WebsocketSubscriber {
    async fn subscribe(
        &mut self,
        subscriptions: &Vec<WebsocketSubscription>,
    ) -> Result<UnboundedReceiverStream<MarketData>, tungstenite::Error>;
}
