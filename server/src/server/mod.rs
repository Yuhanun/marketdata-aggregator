use std::{pin::Pin, time::Instant};

use futures_util::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;

use crate::{
    connectors::{Exchange, MarketDataChannelReceiver, Price, Volume},
    state::Orderbooks,
};

pub mod protos {
    tonic::include_proto!("orderbook");
}

type SerializedSummarySender = tokio::sync::broadcast::Sender<protos::Summary>;

const MAXIMUM_LEVELS: usize = 10;

fn merge_levels<'a, F>(
    mut binance_iter: impl Iterator<Item = (&'a Price, &'a Volume)>,
    mut bitstamp_iter: impl Iterator<Item = (&'a Price, &'a Volume)>,
    should_prioritize_first: F,
) -> Vec<protos::Level>
where
    F: Fn(&Price, &Price) -> bool,
{
    let mut binance_current = binance_iter.next();
    let mut bitstamp_current = bitstamp_iter.next();
    let mut levels = Vec::new();
    let mut level_count = 0;

    while level_count < MAXIMUM_LEVELS && (binance_current.is_some() || bitstamp_current.is_some())
    {
        match (binance_current, bitstamp_current) {
            (Some((binance_price, binance_volume)), Some((bitstamp_price, bitstamp_volume))) => {
                if should_prioritize_first(binance_price, bitstamp_price) {
                    levels.push(protos::Level {
                        exchange: Exchange::Binance.to_string(),
                        price: binance_price.as_f64(),
                        amount: binance_volume.as_f64(),
                    });
                    binance_current = binance_iter.next();
                    level_count += 1;
                } else if binance_price == bitstamp_price {
                    levels.push(protos::Level {
                        exchange: Exchange::Binance.to_string(),
                        price: binance_price.as_f64(),
                        amount: binance_volume.as_f64(),
                    });
                    levels.push(protos::Level {
                        exchange: Exchange::Bitstamp.to_string(),
                        price: bitstamp_price.as_f64(),
                        amount: bitstamp_volume.as_f64(),
                    });
                    binance_current = binance_iter.next();
                    bitstamp_current = bitstamp_iter.next();
                    level_count += 1;
                } else {
                    levels.push(protos::Level {
                        exchange: Exchange::Bitstamp.to_string(),
                        price: bitstamp_price.as_f64(),
                        amount: bitstamp_volume.as_f64(),
                    });
                    bitstamp_current = bitstamp_iter.next();
                    level_count += 1;
                }
            }
            (Some((binance_price, binance_volume)), None) => {
                levels.push(protos::Level {
                    exchange: Exchange::Binance.to_string(),
                    price: binance_price.as_f64(),
                    amount: binance_volume.as_f64(),
                });
                binance_current = binance_iter.next();
                level_count += 1;
            }
            (None, Some((bitstamp_price, bitstamp_volume))) => {
                levels.push(protos::Level {
                    exchange: Exchange::Bitstamp.to_string(),
                    price: bitstamp_price.as_f64(),
                    amount: bitstamp_volume.as_f64(),
                });
                bitstamp_current = bitstamp_iter.next();
                level_count += 1;
            }
            (None, None) => {
                break;
            }
        }
    }

    levels
}

// Note: You would probably recompile this out in production and perform these checks elsewhere.
fn orderbooks_sanity_checks(orderbooks: &Orderbooks) {
    let bitstamp = orderbooks.bitstamp();
    let binance = orderbooks.binance();

    let bitstamp_top_bid = bitstamp.bids.iter().rev().next().map(|bid| *bid.0);
    let bitstamp_top_ask = bitstamp.asks.iter().next().map(|ask| *ask.0);
    if let (Some(bitstamp_top_bid), Some(bitstamp_top_ask)) = (bitstamp_top_bid, bitstamp_top_ask) {
        if bitstamp_top_ask <= bitstamp_top_bid {
            panic!("Bitstamp is crossing???");
        }
    }

    let binance_top_bid = binance.bids.iter().rev().next().map(|bid| *bid.0);
    let binance_top_ask = binance.asks.iter().next().map(|ask| *ask.0);
    if let (Some(binance_top_bid), Some(binance_top_ask)) = (binance_top_bid, binance_top_ask) {
        if binance_top_ask <= binance_top_bid {
            panic!("Binance is crossing???");
        }
    }
}

pub fn generate_summary(orderbooks: &Orderbooks) -> protos::Summary {
    // We need AT MOST 10 DISTINCT levels from both sides
    let binance = orderbooks.binance();
    let bitstamp = orderbooks.bitstamp();

    orderbooks_sanity_checks(orderbooks);

    // For bids, we want higher prices first (binance > bitstamp)
    let top_bids = merge_levels(
        binance.bids.iter().rev(),
        bitstamp.bids.iter().rev(),
        std::cmp::PartialOrd::gt,
    );

    // For asks, we want lower prices first (binance < bitstamp)
    let top_asks = merge_levels(
        binance.asks.iter(),
        bitstamp.asks.iter(),
        std::cmp::PartialOrd::lt,
    );

    let best_bid = top_bids.first();
    let best_ask = top_asks.first();

    let spread = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => {
            let bid_price = bid.price;
            let ask_price = ask.price;
            ask_price - bid_price
        }
        // No complete data available.
        _ => 0.0,
    };

    // While possible to have a negative spread, it should
    // get completely arbitraged out by makers faster than our little application
    // can run, so we don't need to worry about it too much, so if we get a negative spread
    // it's probably just a "bug".
    if spread < 0.0 {
        tracing::warn!("Spread is negative: {spread}");
    }

    let top_bid_string = top_bids
        .first()
        .map(|bid| format!("{} @ {:.8} - {}", bid.amount, bid.price, bid.exchange))
        .unwrap_or_else(|| "XXX".to_string());
    let top_ask_string = top_asks
        .first()
        .map(|ask| format!("{} @ {:.8} - {}", ask.amount, ask.price, ask.exchange))
        .unwrap_or_else(|| "XXX".to_string());
    tracing::info!("Spread: {spread:.8}, Top bid: {top_bid_string}, Top ask: {top_ask_string}");

    protos::Summary {
        spread,
        bids: top_bids,
        asks: top_asks,
    }
}

pub struct SummaryServer {
    summary_sender: tokio::sync::RwLock<SerializedSummarySender>,
}

impl SummaryServer {
    pub fn new() -> Self {
        let (summary_sender, _) = tokio::sync::broadcast::channel(10);
        let summary_sender = tokio::sync::RwLock::new(summary_sender);
        Self { summary_sender }
    }

    pub async fn run(&self, mut receiver: MarketDataChannelReceiver) -> anyhow::Result<()> {
        let mut orderbooks = Orderbooks::new();
        let mut last_bitstamp_update = Instant::now();
        let mut last_binance_update = Instant::now();
        loop {
            let event = receiver.recv().await;
            match event {
                Some(event) => {
                    tracing::info!("Received event from exchange: {:?}", event.exchange);
                    match event.exchange {
                        Exchange::Bitstamp => last_bitstamp_update = Instant::now(),
                        Exchange::Binance => last_binance_update = Instant::now(),
                    }
                    tracing::info!("Event: {:?}", serde_json::to_string(&event).unwrap());
                    orderbooks.process_orderbook_event(event);
                    let summary = generate_summary(&orderbooks);
                    let sender = self.summary_sender.write().await;
                    if let Err(_) = sender.send(summary) {
                        // https://docs.rs/tokio/latest/tokio/sync/broadcast/error/struct.SendError.html
                        // A send operation can only fail if there are no active receivers
                        tracing::debug!("No active receivers, skipping update");
                        continue;
                    }

                    if last_bitstamp_update.elapsed() > std::time::Duration::from_secs(10) {
                        tracing::warn!("Bitstamp has not updated in 10 seconds");
                    }
                    if last_binance_update.elapsed() > std::time::Duration::from_secs(10) {
                        tracing::warn!("Binance has not updated in 10 seconds");
                    }
                }
                None => {
                    tracing::error!("Receiver disconnected");
                    anyhow::bail!("Receiver disconnected");
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl protos::orderbook_aggregator_server::OrderbookAggregator for SummaryServer {
    // It's a shame that we need to serialize for each client...
    type BookSummaryStream =
        Pin<Box<dyn Stream<Item = Result<protos::Summary, tonic::Status>> + Send>>;

    async fn book_summary(
        &self,
        _request: tonic::Request<protos::Empty>,
    ) -> Result<tonic::Response<Self::BookSummaryStream>, tonic::Status> {
        let mut marketdata_receiver = {
            let sender = self.summary_sender.read().await;
            sender.subscribe()
        };

        let (sender, receiver) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            loop {
                let summary = match marketdata_receiver.recv().await {
                    Ok(summary) => summary,
                    Err(e) => {
                        tracing::debug!("Error receiving summary, client disconnected: {:?}", e);
                        break;
                    }
                };

                match sender.send(Ok(summary)).await {
                    Ok(_) => (),
                    Err(_) => {
                        tracing::debug!("Error sending summary, client disconnected");
                        return;
                    }
                }
            }
        });

        let receiver = ReceiverStream::new(receiver);

        Ok(Response::new(Box::pin(receiver) as Self::BookSummaryStream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::{
        Exchange, LevelUpdate, Orderbook, OrderbookEvent, OrderbookUpdate, Price, Volume,
    };
    use rstest::*;

    // Helper function to create test orderbooks using string literals
    fn create_test_orderbooks_flexible(
        binance_bids: Vec<(&str, f64)>,
        binance_asks: Vec<(&str, f64)>,
        bitstamp_bids: Vec<(&str, f64)>,
        bitstamp_asks: Vec<(&str, f64)>,
    ) -> Orderbooks {
        let mut orderbooks = Orderbooks::new();
        
        // Process Binance snapshot
        let binance_event = OrderbookEvent {
            exchange: Exchange::Binance,
            orderbook_update: OrderbookUpdate::Snapshot(Orderbook {
                bids_update: binance_bids
                    .into_iter()
                    .map(|(price, volume)| LevelUpdate {
                        price: Price::try_from(price).unwrap(),
                        volume: Volume::from(volume),
                    })
                    .collect(),
                asks_update: binance_asks
                    .into_iter()
                    .map(|(price, volume)| LevelUpdate {
                        price: Price::try_from(price).unwrap(),
                        volume: Volume::from(volume),
                    })
                    .collect(),
            }),
        };
        orderbooks.process_orderbook_event(binance_event);

        // Process Bitstamp snapshot
        let bitstamp_event = OrderbookEvent {
            exchange: Exchange::Bitstamp,
            orderbook_update: OrderbookUpdate::Snapshot(Orderbook {
                bids_update: bitstamp_bids
                    .into_iter()
                    .map(|(price, volume)| LevelUpdate {
                        price: Price::try_from(price).unwrap(),
                        volume: Volume::from(volume),
                    })
                    .collect(),
                asks_update: bitstamp_asks
                    .into_iter()
                    .map(|(price, volume)| LevelUpdate {
                        price: Price::try_from(price).unwrap(),
                        volume: Volume::from(volume),
                    })
                    .collect(),
            }),
        };
        orderbooks.process_orderbook_event(bitstamp_event);

        orderbooks
    }

    // Test case data structures for parameterized testing
    #[derive(Debug, Clone)]
    struct OrderbookTestData<'a> {
        binance_bids: Vec<(&'a str, f64)>,
        binance_asks: Vec<(&'a str, f64)>,
        bitstamp_bids: Vec<(&'a str, f64)>,
        bitstamp_asks: Vec<(&'a str, f64)>,
    }


    #[derive(Debug, Clone)]
    struct ExpectedResult {
        spread: f64,
        bid_count: usize,
        ask_count: usize,
        should_panic: bool,
    }

    // Test case fixtures using string literals
    fn normal_market_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![
                ("50000.00000000", 1.0),
                ("49999.00000000", 2.0),
                ("49998.00000000", 3.0),
            ],
            binance_asks: vec![
                ("50001.00000000", 1.0),
                ("50002.00000000", 2.0),
                ("50003.00000000", 3.0),
            ],
            bitstamp_bids: vec![
                ("49995.00000000", 1.5),
                ("49994.00000000", 2.5),
                ("49993.00000000", 3.5),
            ],
            bitstamp_asks: vec![
                ("50005.00000000", 1.5),
                ("50006.00000000", 2.5),
                ("50007.00000000", 3.5),
            ],
        }
    }

    fn binance_only_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![("50000.00000000", 1.0), ("49999.00000000", 2.0)],
            binance_asks: vec![("50001.00000000", 1.0), ("50002.00000000", 2.0)],
            bitstamp_bids: vec![],
            bitstamp_asks: vec![],
        }
    }

    fn bitstamp_only_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![],
            binance_asks: vec![],
            bitstamp_bids: vec![("50000.00000000", 1.0), ("49999.00000000", 2.0)],
            bitstamp_asks: vec![("50001.00000000", 1.0), ("50002.00000000", 2.0)],
        }
    }

    fn empty_market_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![],
            binance_asks: vec![],
            bitstamp_bids: vec![],
            bitstamp_asks: vec![],
        }
    }

    fn identical_prices_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![("50000.00000000", 1.0)],
            binance_asks: vec![("50001.00000000", 1.0)],
            bitstamp_bids: vec![("50000.00000000", 2.0)], // Same price as Binance bid
            bitstamp_asks: vec![("50001.00000000", 2.0)], // Same price as Binance ask
        }
    }

    fn crossed_market_binance_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![("50001.00000000", 1.0)], // Higher bid
            binance_asks: vec![("50000.00000000", 1.0)], // Lower ask (crossed market)
            bitstamp_bids: vec![],
            bitstamp_asks: vec![],
        }
    }

    fn crossed_market_bitstamp_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![],
            binance_asks: vec![],
            bitstamp_bids: vec![("50001.00000000", 1.0)], // Higher bid
            bitstamp_asks: vec![("50000.00000000", 1.0)], // Lower ask (crossed market)
        }
    }

    fn precision_data() -> OrderbookTestData<'static> {
        OrderbookTestData {
            binance_bids: vec![("50000.12345678", 1.12345678)],
            binance_asks: vec![("50001.12345678", 1.12345678)],
            bitstamp_bids: vec![("49999.12345678", 2.12345678)],
            bitstamp_asks: vec![("50002.12345678", 2.12345678)],
        }
    }

    fn max_levels_data() -> OrderbookTestData<'static> {
        // Use static string literals for the max levels test
        OrderbookTestData {
            binance_bids: vec![
                ("50000.00000000", 1.0),
                ("49999.00000000", 1.0),
                ("49998.00000000", 1.0),
                ("49997.00000000", 1.0),
                ("49996.00000000", 1.0),
                ("49995.00000000", 1.0),
                ("49994.00000000", 1.0),
                ("49993.00000000", 1.0),
                ("49992.00000000", 1.0),
                ("49991.00000000", 1.0),
            ],
            binance_asks: vec![
                ("50001.00000000", 1.0),
                ("50002.00000000", 1.0),
                ("50003.00000000", 1.0),
                ("50004.00000000", 1.0),
                ("50005.00000000", 1.0),
                ("50006.00000000", 1.0),
                ("50007.00000000", 1.0),
                ("50008.00000000", 1.0),
                ("50009.00000000", 1.0),
                ("50010.00000000", 1.0),
                ("50011.00000000", 1.0),
            ],
            bitstamp_bids: vec![
                ("49990.00000000", 1.0),
                ("49989.00000000", 1.0),
                ("49988.00000000", 1.0),
                ("49987.00000000", 1.0),
                ("49986.00000000", 1.0),
                ("49985.00000000", 1.0),
                ("49984.00000000", 1.0),
                ("49983.00000000", 1.0),
                ("49982.00000000", 1.0),
                ("49981.00000000", 1.0),
            ],
            bitstamp_asks: vec![
                ("50010.00000000", 1.0),
                ("50011.00000000", 1.0),
                ("50012.00000000", 1.0),
                ("50013.00000000", 1.0),
                ("50014.00000000", 1.0),
                ("50015.00000000", 1.0),
                ("50016.00000000", 1.0),
                ("50017.00000000", 1.0),
                ("50018.00000000", 1.0),
                ("50019.00000000", 1.0),
                ("50020.00000000", 1.0),
            ],
        }
    }

    // Parameterized tests using rstest
    #[rstest]
    #[case(normal_market_data(), ExpectedResult { spread: 1.0, bid_count: 6, ask_count: 6, should_panic: false })]
    #[case(binance_only_data(), ExpectedResult { spread: 1.0, bid_count: 2, ask_count: 2, should_panic: false })]
    #[case(bitstamp_only_data(), ExpectedResult { spread: 1.0, bid_count: 2, ask_count: 2, should_panic: false })]
    #[case(empty_market_data(), ExpectedResult { spread: 0.0, bid_count: 0, ask_count: 0, should_panic: false })]
    #[case(identical_prices_data(), ExpectedResult { spread: 1.0, bid_count: 2, ask_count: 2, should_panic: false })]
    #[case(precision_data(), ExpectedResult { spread: 1.0, bid_count: 2, ask_count: 2, should_panic: false })]
    #[case(max_levels_data(), ExpectedResult { spread: 1.0, bid_count: 10, ask_count: 11, should_panic: false })]
    fn test_generate_summary_parameterized(
        #[case] data: OrderbookTestData<'static>,
        #[case] expected: ExpectedResult,
    ) {
        let orderbooks = create_test_orderbooks_flexible(
            data.binance_bids,
            data.binance_asks,
            data.bitstamp_bids,
            data.bitstamp_asks,
        );

        if expected.should_panic {
            let result = std::panic::catch_unwind(|| generate_summary(&orderbooks));
            assert!(result.is_err(), "Expected panic for test case");
        } else {
            let summary = generate_summary(&orderbooks);

            assert_eq!(summary.spread, expected.spread, "Spread mismatch");
            assert_eq!(summary.bids.len(), expected.bid_count, "Bid count mismatch");
            assert_eq!(summary.asks.len(), expected.ask_count, "Ask count mismatch");
        }
    }

    #[rstest]
    #[case(crossed_market_binance_data())]
    #[case(crossed_market_bitstamp_data())]
    fn test_generate_summary_crossed_market_panics(#[case] data: OrderbookTestData<'static>) {
        let orderbooks = create_test_orderbooks_flexible(
            data.binance_bids,
            data.binance_asks,
            data.bitstamp_bids,
            data.bitstamp_asks,
        );

        let result = std::panic::catch_unwind(|| generate_summary(&orderbooks));
        assert!(result.is_err(), "Expected panic for crossed market");
    }

    // Additional specific tests for ordering and exchange identification
    #[test]
    fn test_generate_summary_ordering_bids() {
        // Using string literals for all prices
        let orderbooks = create_test_orderbooks_flexible(
            vec![("50000.00000000", 1.0), ("49998.00000000", 2.0)],
            vec![("50001.00000000", 1.0), ("50003.00000000", 2.0)],
            vec![("49999.00000000", 1.0), ("49997.00000000", 2.0)],
            vec![("50002.00000000", 1.0), ("50004.00000000", 2.0)],
        );

        let summary = generate_summary(&orderbooks);

        // Bids should be ordered from highest to lowest price
        let expected_bid_prices = vec![50000.0, 49999.0, 49998.0, 49997.0];
        let actual_bid_prices: Vec<f64> = summary.bids.iter().map(|b| b.price).collect();
        assert_eq!(actual_bid_prices, expected_bid_prices);
    }

    #[test]
    fn test_generate_summary_ordering_asks() {
        // Using string literals for all prices
        let orderbooks = create_test_orderbooks_flexible(
            vec![("50000.00000000", 1.0), ("49998.00000000", 2.0)],
            vec![("50001.00000000", 1.0), ("50003.00000000", 2.0)], // Binance: lower ask prices
            vec![("49999.00000000", 1.0), ("49997.00000000", 2.0)],
            vec![("50002.00000000", 1.0), ("50004.00000000", 2.0)], // Bitstamp: higher ask prices
        );

        let summary = generate_summary(&orderbooks);

        // Asks should be ordered from lowest to highest price
        let expected_ask_prices = vec![50001.0, 50002.0, 50003.0, 50004.0];
        let actual_ask_prices: Vec<f64> = summary.asks.iter().map(|a| a.price).collect();
        assert_eq!(actual_ask_prices, expected_ask_prices);
    }

    #[test]
    fn test_generate_summary_exchange_identification() {
        // Using string literals for all prices
        let orderbooks = create_test_orderbooks_flexible(
            vec![("50000.00000000", 1.0)],
            vec![("50001.00000000", 1.0)],
            vec![("49999.00000000", 2.0)],
            vec![("50002.00000000", 2.0)],
        );

        let summary = generate_summary(&orderbooks);

        // Check that exchanges are correctly identified
        let binance_bids: Vec<_> = summary
            .bids
            .iter()
            .filter(|b| b.exchange == "binance")
            .collect();
        let bitstamp_bids: Vec<_> = summary
            .bids
            .iter()
            .filter(|b| b.exchange == "bitstamp")
            .collect();
        let binance_asks: Vec<_> = summary
            .asks
            .iter()
            .filter(|a| a.exchange == "binance")
            .collect();
        let bitstamp_asks: Vec<_> = summary
            .asks
            .iter()
            .filter(|a| a.exchange == "bitstamp")
            .collect();

        assert_eq!(binance_bids.len(), 1);
        assert_eq!(bitstamp_bids.len(), 1);
        assert_eq!(binance_asks.len(), 1);
        assert_eq!(bitstamp_asks.len(), 1);

        assert_eq!(binance_bids[0].price, 50000.0);
        assert_eq!(bitstamp_bids[0].price, 49999.0);
        assert_eq!(binance_asks[0].price, 50001.0);
        assert_eq!(bitstamp_asks[0].price, 50002.0);
    }

    #[test]
    fn test_generate_summary_identical_prices_exchanges() {
        // Using string literals for all prices
        let orderbooks = create_test_orderbooks_flexible(
            vec![("50000.00000000", 1.0)],
            vec![("50001.00000000", 1.0)],
            vec![("50000.00000000", 2.0)], // Same price as Binance bid
            vec![("50001.00000000", 2.0)], // Same price as Binance ask
        );

        let summary = generate_summary(&orderbooks);

        // Should include both exchanges for identical prices
        assert_eq!(summary.bids.len(), 2);
        assert_eq!(summary.asks.len(), 2);

        // Both should have the same price
        assert_eq!(summary.bids[0].price, summary.bids[1].price);
        assert_eq!(summary.asks[0].price, summary.asks[1].price);

        // Should have different exchanges
        let exchanges: std::collections::HashSet<_> =
            summary.bids.iter().map(|b| &b.exchange).collect();
        assert_eq!(exchanges.len(), 2);
    }
}
