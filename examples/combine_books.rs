use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    orderbook::{levels_to_orderbook, merge_orderbooks, LimitOrderBook},
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
    const DEPTH: usize = 10;

    while let Some(msg) = stream.next().await {
        match msg.kind {
            MarketDataKind::L2Snapshot(snapshot) => {
                let bids = levels_to_orderbook(snapshot.bids, DEPTH);
                let asks = levels_to_orderbook(snapshot.asks, DEPTH);

                books
                    .entry(msg.venue)
                    .and_modify(|x| {
                        x.bids = bids.clone();
                        x.asks = asks.clone();
                    })
                    .or_insert(LimitOrderBook::new(bids, asks));
            }
            MarketDataKind::L2Update(update) => {
                let bid_len = update.bids.len();
                let bids = levels_to_orderbook(update.bids, bid_len);

                let ask_len = update.asks.len();
                let asks = levels_to_orderbook(update.asks, ask_len);

                if let Some(book) = books.get_mut(&msg.venue) {
                    book.bids = merge_orderbooks(&book.bids, &bids);
                    book.asks = merge_orderbooks(&book.asks, &asks);
                }
            }
            _ => continue,
        }
    }
}
