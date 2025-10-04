use anyhow::Context;
use std::str::FromStr;

use num_traits::ToPrimitive;
use serde::{Deserialize, Deserializer, Serialize};

pub mod binance;
pub mod bitstamp;

type WebsocketClient =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub type MarketDataChannelSender = tokio::sync::mpsc::Sender<OrderbookEvent>;
pub type MarketDataChannelReceiver = tokio::sync::mpsc::Receiver<OrderbookEvent>;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Serialize)]
#[serde(transparent)]
pub struct Price(i64);

// 8 Decimals = 1 satoshi
const PRICE_SCALING_FACTOR: i64 = 100000000;

impl Price {
    // Note: Instead of using bigdecimal we could do a more efficient parsing here
    // by ignoring the decimal point since these floats are basically fixed point integers
    fn from_bigdecimal(value: bigdecimal::BigDecimal) -> anyhow::Result<Self> {
        let value_scaled = value * PRICE_SCALING_FACTOR;
        // Truncate all the decimals.
        let value_scaled = value_scaled.with_scale(0);
        value_scaled
            .to_i64()
            .context("Failed to convert BigDecimal to i64")
            .map(Self)
    }

    // Honestly: Not 100% accurate
    pub fn as_f64(&self) -> f64 {
        self.0 as f64 / PRICE_SCALING_FACTOR as f64
    }
}

impl TryFrom<&str> for Price {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let floating = bigdecimal::BigDecimal::from_str(value)?;
        Self::from_bigdecimal(floating)
    }
}

impl<'de> Deserialize<'de> for Price {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_string = String::deserialize(deserializer)?;
        Price::try_from(raw_string.as_str()).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Volume(f64);

impl Volume {
    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

impl From<f64> for Volume {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl TryFrom<&str> for Volume {
    type Error = std::num::ParseFloatError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self(value.parse::<f64>()?))
    }
}

impl<'de> Deserialize<'de> for Volume {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_string = String::deserialize(deserializer)?;
        Volume::try_from(raw_string.as_str()).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LevelUpdate {
    pub price: Price,
    pub volume: Volume,
}

impl From<(Price, Volume)> for LevelUpdate {
    fn from((price, volume): (Price, Volume)) -> Self {
        Self { price, volume }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Orderbook {
    pub bids_update: Vec<LevelUpdate>,
    pub asks_update: Vec<LevelUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum Exchange {
    Binance,
    Bitstamp,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::Binance => write!(f, "binance"),
            Exchange::Bitstamp => write!(f, "bitstamp"),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum OrderbookUpdate {
    // Note: Neither exchange does incremental updates, so we only have snapshot updates
    Update(Orderbook),
    Snapshot(Orderbook),
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct OrderbookEvent {
    pub exchange: Exchange,
    pub orderbook_update: OrderbookUpdate,
}
