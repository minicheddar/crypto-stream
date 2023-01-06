use crate::model::{OrderBookLevel, Venue};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use std::{cmp::Ordering, collections::BTreeMap};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Level {
    pub price: Decimal,

    pub quantity: Decimal,

    pub venue: Venue,
}

impl PartialOrd for Level {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.price.partial_cmp(&other.price) {
            Some(Ordering::Equal) => self.quantity.partial_cmp(&other.quantity),
            ord => ord,
        }
    }
}

type LevelMap = BTreeMap<Decimal, Level>;

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

pub fn levels_to_orderbook(levels: &Vec<OrderBookLevel>, venue: Venue, depth: usize) -> LevelMap {
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
                },
            )
        })
        .collect::<BTreeMap<Decimal, Level>>()
}

pub fn merge_orderbooks(a: &LevelMap, b: &LevelMap) -> LevelMap {
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
