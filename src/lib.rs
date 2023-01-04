use crate::{
    binance_futures::BinanceFutures,
    binance_spot::BinanceSpot,
    coinbase::Coinbase,
    huobi::Huobi,
    model::*,
    okx::Okx,
    websocket::{WebsocketSubscriber, WebsocketSubscription},
};
use futures::stream::{self, SelectAll};
use std::collections::HashMap;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub mod binance_futures;
pub mod binance_spot;
pub mod coinbase;
pub mod huobi;
pub mod model;
pub mod okx;
pub mod websocket;

pub fn build_venue_subscriptions(
    subscriptions: Vec<WebsocketSubscription>,
) -> HashMap<Venue, Vec<WebsocketSubscription>> {
    let mut venue_subs: HashMap<Venue, Vec<WebsocketSubscription>> = HashMap::new();
    for sub in subscriptions {
        if let Some(ws_subs) = venue_subs.get_mut(&sub.venue) {
            ws_subs.push(sub);
            ws_subs.dedup();
        } else {
            let ws_subs = vec![sub.clone()];
            venue_subs.insert(sub.venue, ws_subs);
        }
    }
    return venue_subs;
}

pub async fn subscriptions_into_stream(
    venue_subs: HashMap<Venue, Vec<WebsocketSubscription>>,
) -> SelectAll<UnboundedReceiverStream<MarketData>> {
    let mut streams = Vec::with_capacity(venue_subs.len());
    for (venue, subs) in venue_subs {
        match venue {
            Venue::Coinbase => streams.push(Coinbase::new().subscribe(&subs).await.unwrap()),
            Venue::BinanceFuturesUsd => {
                streams.push(BinanceFutures::new().subscribe(&subs).await.unwrap())
            }
            Venue::BinanceSpot => streams.push(BinanceSpot::new().subscribe(&subs).await.unwrap()),
            Venue::Huobi => streams.push(Huobi::new().subscribe(&subs).await.unwrap()),
            Venue::Okx => streams.push(Okx::new().subscribe(&subs).await.unwrap()),
        }
    }
    return stream::select_all(streams);
}
