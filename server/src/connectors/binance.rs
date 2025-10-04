use crate::connectors::{
    Exchange, LevelUpdate, Orderbook, OrderbookEvent, OrderbookUpdate, Price, Volume,
};

use super::{MarketDataChannelSender, WebsocketClient};
use anyhow::Context;
use futures_util::SinkExt;
use serde::Deserialize;
use tokio_stream::StreamExt;

pub struct BinanceConnector {
    symbol: String,
    marketdata_channel: MarketDataChannelSender,
}

#[derive(Deserialize, Debug)]
pub struct BinanceOrderbookUpdate {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<(Price, Volume)>,
    pub asks: Vec<(Price, Volume)>,
}

impl BinanceConnector {
    async fn connect(symbol: &str) -> anyhow::Result<WebsocketClient> {
        let (websocket_client, _) = tokio_tungstenite::connect_async(format!(
            "wss://stream.binance.com:9443/ws/{symbol}@depth20@100ms"
        ))
        .await
        .context("Failed to connect to Binance")?;

        Ok(websocket_client)
    }

    pub async fn new(
        marketdata_channel: MarketDataChannelSender,
        symbol: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            symbol,
            marketdata_channel,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let mut websocket_client = None;

        loop {
            let mut connected_websocket_client = match websocket_client.take() {
                Some(websocket_client) => websocket_client,
                None => match Self::connect(&self.symbol).await {
                    Ok(websocket_client) => websocket_client,
                    Err(e) => {
                        tracing::error!("Error connecting, retrying in 100ms...: {:?}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                },
            };

            let message = match connected_websocket_client.next().await {
                Some(message) => message,
                None => {
                    tracing::error!("Error: disconnected from Binance");
                    websocket_client = None;
                    continue;
                }
            };

            let message = match message {
                Ok(message) => message,
                Err(e) => {
                    tracing::error!("Error: {:?}", e);
                    websocket_client = None;
                    continue;
                }
            };

            tracing::debug!("Message: {:?}", message);

            if message.is_ping() {
                let data = message.into_data();
                tracing::info!("Received ping, sending pong with data: {:?}", data);
                match connected_websocket_client
                    .send(tokio_tungstenite::tungstenite::Message::Pong(data))
                    .await
                {
                    Ok(_) => {
                        // If the first message we get on this connection is a ping,
                        // we need to set the websocket client to the connected one
                        // because we don't want to reconnect
                        websocket_client = Some(connected_websocket_client);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Error: {:?}", e);
                        websocket_client = None;
                        continue;
                    }
                }
            }

            // We seem to have a properly connected websocket client now :)
            websocket_client = Some(connected_websocket_client);

            let message = match message.to_text() {
                Ok(message) => message,
                Err(e) => {
                    tracing::error!("Error: {:?}", e);
                    continue;
                }
            };

            tracing::debug!("Message: {:?}", message);

            let orderbook_update: BinanceOrderbookUpdate = match serde_json::from_str(&message) {
                Ok(orderbook_update) => orderbook_update,
                Err(e) => {
                    // We are still connected, we just got garbage somehow...
                    tracing::error!("Parsing {message} error -> {:?}", e);
                    continue;
                }
            };

            tracing::debug!("Orderbook update: {:?}", orderbook_update);

            self.marketdata_channel
                .send(Self::create_orderbook_event(orderbook_update))
                .await?;
        }
    }

    fn create_side_update(data: Vec<(Price, Volume)>) -> Vec<LevelUpdate> {
        data.into_iter().map(LevelUpdate::from).collect()
    }

    fn create_orderbook_event(data: BinanceOrderbookUpdate) -> OrderbookEvent {
        OrderbookEvent {
            exchange: Exchange::Binance,
            orderbook_update: OrderbookUpdate::Snapshot(Orderbook {
                bids_update: Self::create_side_update(data.bids),
                asks_update: Self::create_side_update(data.asks),
            }),
        }
    }
}
