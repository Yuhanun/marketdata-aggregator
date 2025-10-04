use anyhow::Context;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::connectors::{
    Exchange, LevelUpdate, Orderbook, OrderbookEvent, OrderbookUpdate, Price, Volume,
};

use super::{MarketDataChannelSender, WebsocketClient};

pub struct BitstampConnector {
    symbol: String,
    marketdata_channel: MarketDataChannelSender,
}

#[derive(Deserialize, Debug)]
pub struct BitstampOrderbookUpdate {
    pub timestamp: String,
    pub microtimestamp: String,
    pub bids: Vec<(Price, Volume)>,
    pub asks: Vec<(Price, Volume)>,
}

#[derive(Serialize, Debug)]
pub struct BitstampSubscribeData {
    pub channel: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitstampMessage<T: Serialize> {
    pub event: String,
    pub data: T,
}

impl BitstampConnector {
    fn create_side_update(data: Vec<(Price, Volume)>) -> Vec<LevelUpdate> {
        data.into_iter().map(LevelUpdate::from).collect()
    }

    fn create_orderbook_event(data: BitstampOrderbookUpdate) -> OrderbookEvent {
        OrderbookEvent {
            exchange: Exchange::Bitstamp,
            orderbook_update: OrderbookUpdate::Snapshot(Orderbook {
                bids_update: Self::create_side_update(data.bids),
                asks_update: Self::create_side_update(data.asks),
            }),
        }
    }

    async fn connect(symbol: &str) -> anyhow::Result<WebsocketClient> {
        let (mut websocket_client, _) =
            tokio_tungstenite::connect_async("wss://ws.bitstamp.net/v2/")
                .await
                .context("Failed to connect to Bitstamp")?;

        Self::subscribe(&mut websocket_client, symbol).await?;

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

    async fn subscribe(websocket_client: &mut WebsocketClient, symbol: &str) -> anyhow::Result<()> {
        Self::send_message(
            websocket_client,
            &BitstampMessage {
                event: "bts:subscribe".to_string(),
                data: BitstampSubscribeData {
                    channel: format!("order_book_{}", symbol),
                },
            },
        )
        .await?;
        Ok(())
    }

    async fn send_message<T: Serialize>(
        websocket_client: &mut WebsocketClient,
        message: &T,
    ) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(message)?;
        tracing::info!("Sending message: {:?}", serialized);
        websocket_client
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serialized.into(),
            ))
            .await?;
        Ok(())
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
                    tracing::error!("Error: disconnected from Bitstamp");
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

            if message.is_ping() {
                let data = message.into_data();
                tracing::info!("Received ping, sending pong: {data:?}",);
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

            let message = match message.to_text() {
                Ok(message) => message,
                Err(e) => {
                    tracing::error!("Error: {:?}", e);
                    continue;
                }
            };

            // We seem to have a properly connected websocket client now :)
            websocket_client = Some(connected_websocket_client);

            tracing::info!("Message: {}", message);

            let message: BitstampMessage<serde_json::Value> = serde_json::from_str(&message)
                .with_context(|| format!("Failed to parse Bitstamp message: {message}"))?;

            let data: BitstampOrderbookUpdate = match message.event.as_str() {
                "data" => {
                    serde_json::from_value(message.data).context("Failed to parse Bitstamp data")?
                }
                other => {
                    tracing::warn!("Unknown event: {:?}", other);
                    continue;
                }
            };

            tracing::debug!("Bitstamp data: {:?}", data);

            let data = Self::create_orderbook_event(data);
            self.marketdata_channel.send(data).await?;
        }
    }
}
