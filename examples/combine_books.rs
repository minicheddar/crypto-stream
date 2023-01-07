use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use chrono::Utc;
use crossterm::{
    event::EnableMouseCapture,
    execute,
    terminal::{enable_raw_mode, EnterAlternateScreen},
};
use crypto_stream::{
    build_venue_subscriptions,
    model::*,
    orderbook::{levels_to_orderbook, merge_orderbooks, Level, LimitOrderBook, Side},
    subscriptions_into_stream,
    tui::render_orderbook,
    websocket::{WebsocketSubscription, WebsocketSubscriptionKind},
};
use futures::StreamExt;
use std::{
    collections::{BTreeMap, HashMap},
    io,
};
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

    let mut exchange_books: HashMap<Venue, LimitOrderBook> = HashMap::new();
    const DEPTH: usize = 10;

    // setup terminal
    enable_raw_mode();
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture);
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();

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

        // println!("");
        // println!("Combined bids BEFORE: {:?}", combined_book.bids);
        // println!("Combined asks BEFORE: {:?}", combined_book.asks);

        let asks: Vec<&Level> = combined_book
            .asks
            .values()
            .into_iter()
            .take(DEPTH)
            .collect();

        let bids: Vec<&Level> = combined_book
            .bids
            .values()
            .into_iter()
            .rev()
            .take(DEPTH)
            .collect();

        let levels: Vec<&Level> = asks.into_iter().chain(bids.into_iter()).collect();
        // let took = (Utc::now() - now).num_microseconds().unwrap();

        terminal
            .draw(|f| render_orderbook(f, levels))
            .expect("error rendering TUI");

        // if let Event::Key(key) = event::read()? {
        //     match key.code {
        //         KeyCode::Char('q') => return Ok(()),
        //         // KeyCode::Down => app.next(),
        //         // KeyCode::Up => app.previous(),
        //         _ => continue
        //     }
        // }
    }
}
