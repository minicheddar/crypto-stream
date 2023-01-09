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
use std::time::{Duration, Instant};
use std::vec;

#[tokio::main]
async fn main() {
    let subscriptions = vec![
        WebsocketSubscription::new(Venue::BinanceSpot, "BTC", "USDT", InstrumentKind::Spot, WebsocketSubscriptionKind::Quote),
        WebsocketSubscription::new(Venue::GateIO, "BTC", "USDT", InstrumentKind::Spot, WebsocketSubscriptionKind::Quote),
        WebsocketSubscription::new(Venue::Okx, "BTC", "USDT", InstrumentKind::Spot, WebsocketSubscriptionKind::Quote),
    ];

    // subscribe to websockets for each venue
    let venue_subs = build_venue_subscriptions(subscriptions);

    // combine each venues websocket stream into a combined stream
    let mut market_data = subscriptions_into_stream(venue_subs).await;

    // initialise TUI
    let mut terminal = setup_terminal_ui();

    let mut cross_book = CrossVenueOrderBook::new("BTC/USD".to_string(), 10);

    const TICK_RATE: Duration = Duration::from_millis(100);
    let mut last_draw = Instant::now();
    loop {
        if let Some(msg) = market_data.next().await {
            // map quote streams to combined orderbook levels
            cross_book.update(&msg);
            let levels = cross_book.to_levels(cross_book.depth);

            if last_draw.elapsed() > TICK_RATE {
                last_draw = Instant::now();

                // render combined orderbook in terminal
                terminal
                    .draw(|f| render_orderbook(f, &cross_book.symbol, levels))
                    .expect("error rendering TUI");

                // // listen for exit event to clean up terminal
                // // TODO: this causes lag in the TUI
                // if check_for_exit_signal(&mut terminal) {
                //     break;
                // }
            }
        }
    }
}
