use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use chrono::Utc;
use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    subscriptions_into_stream,
    websocket::{WebsocketSubscription, WebsocketSubscriptionKind},
};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let subscriptions = vec![
        WebsocketSubscription::new(
            Venue::BinanceFuturesUsd,
            "BTC",
            "USDT",
            InstrumentKind::FuturesPerpetual,
            WebsocketSubscriptionKind::L2Quote,
        ),
        WebsocketSubscription::new(
            Venue::Coinbase,
            "BTC",
            "USD",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::L2Quote,
        ),
        WebsocketSubscription::new(
            Venue::BinanceSpot,
            "BTC",
            "USDT",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::L2Quote,
        ),
        WebsocketSubscription::new(
            Venue::Okx,
            "BTC",
            "USDT",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::L2Quote,
        ),
        WebsocketSubscription::new(
            Venue::Huobi,
            "BTC",
            "USDT",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::L2Quote,
        ),
    ];

    let venue_subs = build_venue_subscriptions(subscriptions);
    let mut stream = subscriptions_into_stream(venue_subs).await;

    while let Some(msg) = stream.next().await {
        // let exchange_diff = (Utc::now() - msg.venue_time).num_milliseconds(); // uncomment to view latency from exchange
        let _process_time = (Utc::now() - msg.received_time).num_microseconds();
        if let Some(dt) = _process_time {
            println!("{:?} | took: {}us", msg, dt);
        }
    }
}
