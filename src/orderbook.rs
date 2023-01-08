use crate::model::{MarketData, MarketDataKind, OrderBookLevel, Venue};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

type LevelMap = BTreeMap<Decimal, Level>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Level {
    pub price: Decimal,

    pub quantity: Decimal,

    pub venue: Venue,

    pub side: Side,
}

impl PartialOrd for Level {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.price.partial_cmp(&other.price) {
            Some(Ordering::Equal) => self.quantity.partial_cmp(&other.quantity),
            ord => ord,
        }
    }
}

pub fn levels_to_map(
    levels: &Vec<OrderBookLevel>,
    venue: Venue,
    side: Side,
    depth: usize,
) -> LevelMap {
    levels
        .split_at(depth)
        .0
        .iter()
        .map(|x| {
            (
                Decimal::from_f64(x.price).unwrap(),
                Level {
                    price: Decimal::from_f64(x.price).unwrap(),
                    quantity: Decimal::from_f64(x.quantity).unwrap(),
                    venue,
                    side,
                },
            )
        })
        .collect::<BTreeMap<Decimal, Level>>()
}

pub fn merge_maps(a: &LevelMap, b: &LevelMap) -> LevelMap {
    let mut book = a
        .into_iter()
        .chain(b)
        .fold(BTreeMap::new(), |mut map, (price, level)| {
            map.entry(*price)
                .and_modify(|l: &mut Level| l.quantity += level.quantity)
                .or_insert(level.clone());
            map
        });
    book.retain(|_, level| level.quantity != dec!(0));
    book
}

#[derive(PartialEq, Debug)]
pub struct LimitOrderBook {
    pub bids: LevelMap,
    pub asks: LevelMap,
}

impl LimitOrderBook {
    pub fn new(bids: LevelMap, asks: LevelMap) -> Self {
        LimitOrderBook { bids, asks }
    }
}

pub struct CrossVenueOrderBook {
    pub depth: usize,
    exchange_books: HashMap<Venue, LimitOrderBook>,
    combined_book: LimitOrderBook,
}

impl CrossVenueOrderBook {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            exchange_books: HashMap::new(),
            combined_book: LimitOrderBook::new(BTreeMap::new(), BTreeMap::new()),
        }
    }

    pub fn update(&mut self, msg: &MarketData) {
        match &msg.kind {
            MarketDataKind::L2Snapshot(snapshot) => {
                let venue = msg.venue.clone();
                let bids = levels_to_map(&snapshot.bids, venue, Side::Bid, self.depth);
                let asks = levels_to_map(&snapshot.asks, venue, Side::Ask, self.depth);

                self.exchange_books
                    .insert(venue, LimitOrderBook::new(bids, asks));
            }
            MarketDataKind::L2Update(update) => {
                let venue = msg.venue.clone();
                let bid_len = update.bids.len();
                let bids = levels_to_map(&update.bids, venue, Side::Bid, bid_len);

                let ask_len = update.asks.len();
                let asks = levels_to_map(&update.asks, venue, Side::Ask, ask_len);

                if let Some(ob) = self.exchange_books.get_mut(&venue) {
                    ob.bids = merge_maps(&ob.bids, &bids);
                    ob.asks = merge_maps(&ob.asks, &asks);
                }
            }
            _ => return,
        }

        self.combined_book = self.exchange_books.values().fold(
            LimitOrderBook::new(BTreeMap::new(), BTreeMap::new()),
            |mut combined_book, ob| {
                combined_book.bids.extend(ob.bids.clone());
                combined_book.asks.extend(ob.asks.clone());
                combined_book
            },
        );
    }

    pub fn to_levels(&self, depth: usize) -> Vec<&Level> {
        let asks: Vec<&Level> = self
            .combined_book
            .asks
            .values()
            .into_iter()
            .take(depth)
            // .cloned()
            .collect();

        let bids: Vec<&Level> = self
            .combined_book
            .bids
            .values()
            .into_iter()
            .rev()
            .take(depth)
            // .cloned()
            .collect();

        return asks.into_iter().chain(bids.into_iter()).collect();
    }
}