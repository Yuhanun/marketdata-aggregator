use std::collections::BTreeMap;

use crate::connectors::{Exchange, LevelUpdate, OrderbookEvent, OrderbookUpdate, Price, Volume};

type SideBook = BTreeMap<Price, Volume>;

pub struct ExchangeOrderbook {
    pub bids: SideBook,
    pub asks: SideBook,
}

pub struct Orderbooks {
    pub binance_orderbook: ExchangeOrderbook,
    pub bitstamp_orderbook: ExchangeOrderbook,
}

impl Default for ExchangeOrderbook {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }
}

impl Orderbooks {
    pub fn new() -> Self {
        Self {
            binance_orderbook: ExchangeOrderbook::default(),
            bitstamp_orderbook: ExchangeOrderbook::default(),
        }
    }

    pub fn binance(&self) -> &ExchangeOrderbook {
        &self.binance_orderbook
    }

    pub fn bitstamp(&self) -> &ExchangeOrderbook {
        &self.bitstamp_orderbook
    }

    fn side_book_from_updates(updates: Vec<LevelUpdate>) -> SideBook {
        updates
            .into_iter()
            .map(|update| (update.price, update.volume))
            .collect()
    }

    pub fn process_orderbook_event(&mut self, event: OrderbookEvent) {
        let exchange = event.exchange;
        let orderbook_update = event.orderbook_update;

        match orderbook_update {
            OrderbookUpdate::Snapshot(orderbook) => match exchange {
                Exchange::Binance => {
                    self.binance_orderbook = ExchangeOrderbook {
                        bids: Self::side_book_from_updates(orderbook.bids_update),
                        asks: Self::side_book_from_updates(orderbook.asks_update),
                    };
                }
                Exchange::Bitstamp => {
                    self.bitstamp_orderbook = ExchangeOrderbook {
                        bids: Self::side_book_from_updates(orderbook.bids_update),
                        asks: Self::side_book_from_updates(orderbook.asks_update),
                    };
                }
            },
            OrderbookUpdate::Update(orderbook) => {
                tracing::error!("Update orderbook updates are not supported: {orderbook:?}");
            }
        }
    }
}
