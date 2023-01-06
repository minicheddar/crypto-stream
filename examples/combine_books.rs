use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    orderbook::{levels_to_map, LimitOrderBook},
    subscriptions_into_stream,
    websocket::{WebsocketSubscription, WebsocketSubscriptionKind},
};
use futures::StreamExt;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let subscriptions = vec![
        WebsocketSubscription::new(
            Venue::Coinbase,
            "BTC",
            "USD",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::Quote,
        ),
        WebsocketSubscription::new(
            Venue::Kraken,
            "XBT",
            "USD",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::Quote,
        ),
    ];

    let venue_subs = build_venue_subscriptions(subscriptions);
    let mut stream = subscriptions_into_stream(venue_subs).await;

    let mut books: HashMap<Venue, LimitOrderBook> = HashMap::new();

    while let Some(msg) = stream.next().await {
        match msg.kind {
            MarketDataKind::Trade(_) => todo!(),
            MarketDataKind::L2Snapshot(snapshot) => {
                let bids = levels_to_map(snapshot.bids);
                let asks = levels_to_map(snapshot.asks);

                // TODO: only take 10 from each side
                books
                    .entry(msg.venue)
                    .and_modify(|x| {
                        x.bids = bids.clone();
                        x.asks = asks.clone();
                    })
                    .or_insert(LimitOrderBook::new(bids, asks));
            }
            MarketDataKind::L2Update(_) => {}
        }
    }
}
