use crate::model::{MarketData, MarketDataKind, OrderBookLevel, Venue};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

type LevelMap = BTreeMap<Decimal, Level>;

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
        .iter()
        .take(depth)
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
                // TODO: The venue will always be from LevelMap 'a'. Need to decide which venue it should be (e.g. one with highest volume at that level?)
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
    pub symbol: String,
    pub depth: usize,
    exchange_books: HashMap<Venue, LimitOrderBook>,
    combined_book: LimitOrderBook,
}

impl CrossVenueOrderBook {
    pub fn new(symbol: String, depth: usize) -> Self {
        Self {
            symbol,
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
            .collect();

        let bids: Vec<&Level> = self
            .combined_book
            .bids
            .values()
            .into_iter()
            .rev()
            .take(depth)
            .collect();

        return asks.into_iter().chain(bids.into_iter()).collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::OrderBookLevel;

    #[test]
    fn levels_to_map_returns_all_when_less_than_depth() {
        let input = vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 10.0,
            },
            OrderBookLevel {
                price: 110.0,
                quantity: 20.0,
            },
            OrderBookLevel {
                price: 120.0,
                quantity: 30.0,
            },
        ];

        let output: BTreeMap<Decimal, Level> = [
            (
                Decimal::from_f64(100.0).unwrap(),
                Level {
                    price: Decimal::from_f64(100.0).unwrap(),
                    quantity: Decimal::from_f64(10.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(20.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(120.0).unwrap(),
                Level {
                    price: Decimal::from_f64(120.0).unwrap(),
                    quantity: Decimal::from_f64(30.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(levels_to_map(&input, Venue::Coinbase, Side::Bid, 4), output);
    }

    #[test]
    fn levels_to_map_returns_depth_when_greater_than_depth() {
        let input = vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 10.0,
            },
            OrderBookLevel {
                price: 110.0,
                quantity: 20.0,
            },
            OrderBookLevel {
                price: 120.0,
                quantity: 30.0,
            },
        ];

        let output: BTreeMap<Decimal, Level> = [
            (
                Decimal::from_f64(100.0).unwrap(),
                Level {
                    price: Decimal::from_f64(100.0).unwrap(),
                    quantity: Decimal::from_f64(10.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(20.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(levels_to_map(&input, Venue::Coinbase, Side::Bid, 2), output);
    }

    #[test]
    fn merge_maps_combines_levels_from_multiple_venues() {
        let coinbase_book: LevelMap = [
            (
                Decimal::from_f64(100.0).unwrap(),
                Level {
                    price: Decimal::from_f64(100.0).unwrap(),
                    quantity: Decimal::from_f64(10.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(20.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let binance_book: LevelMap = [
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(30.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(120.0).unwrap(),
                Level {
                    price: Decimal::from_f64(120.0).unwrap(),
                    quantity: Decimal::from_f64(40.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Bid,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let output: LevelMap = [
            (
                Decimal::from_f64(100.0).unwrap(),
                Level {
                    price: Decimal::from_f64(100.0).unwrap(),
                    quantity: Decimal::from_f64(10.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(50.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Bid,
                },
            ),
            (
                Decimal::from_f64(120.0).unwrap(),
                Level {
                    price: Decimal::from_f64(120.0).unwrap(),
                    quantity: Decimal::from_f64(40.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Bid,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(merge_maps(&coinbase_book, &binance_book), output);
    }

    #[test]
    fn merge_maps_removes_zero_quantity_levels() {
        let mut coinbase_book: LevelMap = [
            (
                Decimal::from_f64(100.0).unwrap(),
                Level {
                    price: Decimal::from_f64(100.0).unwrap(),
                    quantity: Decimal::from_f64(10.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Ask,
                },
            ),
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: dec!(0),
                    venue: Venue::Coinbase,
                    side: Side::Ask,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let binance_book: LevelMap = [
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(30.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Ask,
                },
            ),
            (
                Decimal::from_f64(115.0).unwrap(),
                Level {
                    price: Decimal::from_f64(115.0).unwrap(),
                    quantity: Decimal::from_f64(25.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Ask,
                },
            ),
            (
                Decimal::from_f64(120.0).unwrap(),
                Level {
                    price: Decimal::from_f64(120.0).unwrap(),
                    quantity: dec!(0),
                    venue: Venue::BinanceSpot,
                    side: Side::Ask,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let combined_book_1: LevelMap = [
            (
                Decimal::from_f64(100.0).unwrap(),
                Level {
                    price: Decimal::from_f64(100.0).unwrap(),
                    quantity: Decimal::from_f64(10.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Ask,
                },
            ),
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(30.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Ask,
                },
            ),
            (
                Decimal::from_f64(115.0).unwrap(),
                Level {
                    price: Decimal::from_f64(115.0).unwrap(),
                    quantity: Decimal::from_f64(25.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Ask,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        // assert drops 0 quantity levels
        assert_eq!(merge_maps(&coinbase_book, &binance_book), combined_book_1);

        coinbase_book.insert(
            Decimal::from_f64(100.0).unwrap(),
            Level {
                price: Decimal::from_f64(100.0).unwrap(),
                quantity: dec!(0),
                venue: Venue::Coinbase,
                side: Side::Ask,
            },
        );
        let combined_book_2: LevelMap = [
            (
                Decimal::from_f64(110.0).unwrap(),
                Level {
                    price: Decimal::from_f64(110.0).unwrap(),
                    quantity: Decimal::from_f64(30.0).unwrap(),
                    venue: Venue::Coinbase,
                    side: Side::Ask,
                },
            ),
            (
                Decimal::from_f64(115.0).unwrap(),
                Level {
                    price: Decimal::from_f64(115.0).unwrap(),
                    quantity: Decimal::from_f64(25.0).unwrap(),
                    venue: Venue::BinanceSpot,
                    side: Side::Ask,
                },
            ),
        ]
        .iter()
        .cloned()
        .collect();

        // assert drops 0 quantity levels that previously had quantity > 0
        assert_eq!(merge_maps(&coinbase_book, &binance_book), combined_book_2);
    }
}
