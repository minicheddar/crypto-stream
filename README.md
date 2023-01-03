# crypto-stream
An experiment in unifying multiple cryptocurrency exchange streams under a single API. Think CCXT websockets, but in Rust.

---

:construction: *This library is an active work in progress and not yet suitable for production use* :construction:

## Features
* Simple subscription API
* Supports real-time trade and L2 quote data
* Transforms exchange-native messages to a common `MarketData` type, for easier cross-exchange signal generation
* Websocket integrations for:
    * Binance (USD Futures + Spot)
    * Coinbase
    * OKx (Futures + Spot)

## Roadmap:
* Add support for more exchanges (Huobi, Gate.io, Bybit, Kucoin, Kraken.. maybe others if there's a demand)
* Cross-exchange/unified order book building
* Add support for additional market data streams (liquidations, funding ticker, mark price)
* Add support for additional sinks (mmap file, gRPC, zmq socket)
* Add support for user-stream data streams (order fills/cancellations, balance updates)
* Add support for streaming Uniswap V2/V3 reserve changes


## Getting started:
`$ cargo run --example sub_multi_stream`

or 

```rust, no_run
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
            Venue::Coinbase,
            "BTC",
            "USD",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::Trade,
        ),
        WebsocketSubscription::new(
            Venue::BinanceFuturesUsd,
            "BTC",
            "USDT",
            InstrumentKind::FuturesPerpetual,
            WebsocketSubscriptionKind::Trade,
        )
    ];

    let venue_subs = build_venue_subscriptions(subscriptions);
    let mut stream = subscriptions_into_stream(venue_subs).await;

    while let Some(msg) = stream.next().await {
        let _process_time = (Utc::now() - msg.received_time).num_microseconds();
        if let Some(dt) = _process_time {
            println!("{:?} | took: {}us", msg, dt);
        }
    }
}
```