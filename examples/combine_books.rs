use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    orderbook::CrossVenueOrderBook,
    subscriptions_into_stream,
    tui::{render_orderbook, setup_terminal_ui},
    websocket::{WebsocketSubscription, WebsocketSubscriptionKind},
};
use futures::StreamExt;
use std::vec;

#[tokio::main]
async fn main() {
    // env::set_var("RUST_BACKTRACE", "full");

    let subscriptions = vec![
        WebsocketSubscription::new(
            Venue::BinanceSpot,
            "BTC",
            "USDT",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::Quote,
        ),
        WebsocketSubscription::new(
            Venue::GateIO,
            "BTC",
            "USDT",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::Quote,
        ),
        WebsocketSubscription::new(
            Venue::Huobi,
            "BTC",
            "USDT",
            InstrumentKind::Spot,
            WebsocketSubscriptionKind::Quote,
        ),
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

    // subscribe to websockets for each venue
    let venue_subs = build_venue_subscriptions(subscriptions);

    // combine each venues websocket stream into a combined stream
    let mut market_data = subscriptions_into_stream(venue_subs).await;

    // initialise TUI
    let mut terminal = setup_terminal_ui();

    let mut cross_book = CrossVenueOrderBook::new("BTCUSD".to_string(), 10);
    while let Some(msg) = market_data.next().await {
        // map quote streams to combined orderbook levels
        cross_book.update(&msg);
        let levels = cross_book.to_levels(cross_book.depth);

        // render combined orderbook
        terminal
            .draw(|f| render_orderbook(f, &cross_book.symbol, levels))
            .expect("error rendering TUI");

        // // listen for exit event to shutdown
        // if let Event::Key(key) = event::read().unwrap() {
        //     match key.code {
        //         KeyCode::Char('q') => break,
        //         _ => continue,
        //     }
        // }
    }
}
