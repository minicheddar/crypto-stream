use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crossterm::{
    event::EnableMouseCapture,
    execute,
    terminal::{enable_raw_mode, EnterAlternateScreen},
};
use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    orderbook::CrossVenueOrderBook,
    subscriptions_into_stream,
    tui::render_orderbook,
    websocket::{WebsocketSubscription, WebsocketSubscriptionKind},
};
use futures::StreamExt;
use std::{io, vec};
use tui::{backend::CrosstermBackend, Terminal};

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

    // setup terminal
    let _ = enable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(stdout, EnterAlternateScreen, EnableMouseCapture);
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut cross_book = CrossVenueOrderBook::new(10);

    while let Some(msg) = market_data.next().await {
        cross_book.update(&msg);
        let levels = cross_book.to_levels(cross_book.depth);

        terminal
            .draw(|f| render_orderbook(f, levels))
            .expect("error rendering TUI");
    }

    // // listen to exit event
    // if let Event::Key(key) = event::read()? {
    //     match key.code {
    //         KeyCode::Char('q') => return Ok(()),
    //         // KeyCode::Down => app.next(),
    //        // KeyCode::Up => app.previous(),
    //         _ => continue
    //     }
    // }
}
