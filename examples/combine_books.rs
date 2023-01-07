use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use chrono::Utc;
use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    orderbook::{levels_to_orderbook, merge_orderbooks, Level, LimitOrderBook, Side},
    subscriptions_into_stream,
    websocket::{WebsocketSubscription, WebsocketSubscriptionKind},
};
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};

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
    let mut market_data = subscriptions_into_stream(venue_subs).await;

    let mut exchange_books: HashMap<Venue, LimitOrderBook> = HashMap::new();
    const DEPTH: usize = 10;

    while let Some(msg) = market_data.next().await {
        let now = Utc::now();
        match msg.kind {
            MarketDataKind::L2Snapshot(snapshot) => {
                let venue = msg.venue.clone();
                let bids = levels_to_orderbook(&snapshot.bids, venue, Side::Bid, DEPTH);
                let asks = levels_to_orderbook(&snapshot.asks, venue, Side::Ask, DEPTH);

                exchange_books
                    .entry(venue)
                    .and_modify(|x| {
                        x.bids = bids.clone();
                        x.asks = asks.clone();
                    })
                    .or_insert(LimitOrderBook::new(bids, asks));
            }
            MarketDataKind::L2Update(update) => {
                let venue = msg.venue.clone();
                let bid_len = update.bids.len();
                let bids = levels_to_orderbook(&update.bids, venue, Side::Bid, bid_len);

                let ask_len = update.asks.len();
                let asks = levels_to_orderbook(&update.asks, venue, Side::Ask, ask_len);

                if let Some(ob) = exchange_books.get_mut(&venue) {
                    ob.bids = merge_orderbooks(&ob.bids, &bids);
                    ob.asks = merge_orderbooks(&ob.asks, &asks);
                }
            }
            _ => continue,
        }

        let combined_book = exchange_books.values().fold(
            LimitOrderBook::new(BTreeMap::new(), BTreeMap::new()),
            |mut combined_book, ob| {
                combined_book.bids.extend(ob.bids.clone());
                combined_book.asks.extend(ob.asks.clone());
                combined_book
            },
        );

        let bids: Vec<&Level> = combined_book
            .bids
            .values()
            .into_iter()
            .rev()
            .take(DEPTH)
            .collect();
        let asks: Vec<&Level> = combined_book
            .asks
            .values()
            .into_iter()
            .take(DEPTH)
            .collect();

        let took = (Utc::now() - now).num_microseconds().unwrap();
        println!("BIDS: {:?}", bids);
        println!("ASKS: {:?}", asks);
        println!("Combining orderbooks took: {took}us");
    }
}
