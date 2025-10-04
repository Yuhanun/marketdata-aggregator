use std::{net::ToSocketAddrs, sync::Arc};

use crate::{
    connectors::{binance::BinanceConnector, bitstamp::BitstampConnector},
    server::SummaryServer,
};

pub mod connectors;
pub mod server;
pub mod state;

const SYMBOL: &str = "ethbtc";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    color_eyre::install().expect("Failed to install panic hook");
    tracing_subscriber::fmt::init();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    let (sender, receiver) = tokio::sync::mpsc::channel(10);

    let binance_sender = sender.clone();
    let bitstamp_sender = sender.clone();

    tracing::info!("Starting Binance connector");
    let binance_connector = BinanceConnector::new(binance_sender, SYMBOL.to_string()).await?;
    let mut binance_future = tokio::spawn(binance_connector.run());

    tracing::info!("Starting Bitstamp connector");
    let bitstamp_connector = BitstampConnector::new(bitstamp_sender, SYMBOL.to_string()).await?;
    let mut bitstamp_future = tokio::spawn(bitstamp_connector.run());

    let server = Arc::new(SummaryServer::new());
    let run_server = server.clone();
    let mut server_run_future = tokio::spawn(async move { run_server.run(receiver).await });

    let socket_addrs = "0.0.0.0:50051"
        .to_socket_addrs()?
        .next()
        .expect("Failed to get socket addrs");
    let server_future = tonic::transport::Server::builder()
        .add_service(
            server::protos::orderbook_aggregator_server::OrderbookAggregatorServer::from_arc(
                server,
            ),
        )
        .serve(socket_addrs);

    let mut server_future = tokio::spawn(server_future);

    loop {
        tokio::select! {
            result = &mut bitstamp_future => {
                tracing::error!("Bitstamp connector failed: {result:?}");
            }
            result = &mut binance_future => {
                tracing::error!("Binance connector failed: {result:?}");
            }
            result = &mut server_run_future => {
                tracing::error!("Server run failed: {result:?}");
            }
            result = &mut server_future => {
                tracing::error!("Server failed: {result:?}");
            }
        }
    }
}
