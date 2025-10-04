use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Frame, Terminal,
};
use tokio::sync::Mutex;

pub mod protos;

#[derive(PartialEq, Clone)]
pub enum ConnectionStatus {
    Connecting,
    Reconnecting,
    Connected,
    Disconnected,
}

impl ConnectionStatus {
    pub fn to_display_string(&self) -> String {
        match self {
            ConnectionStatus::Connecting => "Connecting...".to_string(),
            ConnectionStatus::Reconnecting => "Reconnecting...".to_string(),
            ConnectionStatus::Connected => "Connected".to_string(),
            ConnectionStatus::Disconnected => "Disconnected".to_string(),
        }
    }

    pub fn to_color(&self) -> Color {
        match self {
            ConnectionStatus::Connecting => Color::Yellow,
            ConnectionStatus::Reconnecting => Color::Yellow,
            ConnectionStatus::Connected => Color::Green,
            ConnectionStatus::Disconnected => Color::Red,
        }
    }
}

#[derive(Clone)]
struct AppState {
    summary: Option<protos::Summary>,
    last_update: Option<Instant>,
    status: ConnectionStatus,
}

#[derive(PartialEq)]
pub enum DataStatus {
    Reliable,
    Unreliable,
}

impl AppState {
    fn new() -> Self {
        Self {
            summary: None,
            last_update: None,
            status: ConnectionStatus::Connecting,
        }
    }

    fn update_summary(&mut self, summary: protos::Summary) {
        self.summary = Some(summary);
        self.last_update = Some(Instant::now());
        self.status = ConnectionStatus::Connected;
    }

    fn update_status(&mut self, status: ConnectionStatus, data_status: DataStatus) {
        self.status = status;
        if data_status == DataStatus::Unreliable {
            self.summary = None;
        }
    }
}

struct App {
    state: Arc<Mutex<AppState>>,
}

const FRAMES_PER_SECOND: u32 = 60;

fn frame_duration() -> Duration {
    Duration::from_secs(1) / FRAMES_PER_SECOND
}

fn ui(f: &mut Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Length(3), // Header
                Constraint::Length(3), // Spread info
                Constraint::Min(10),   // Orderbook tables
                Constraint::Length(3), // Footer
            ]
            .as_ref(),
        )
        .split(f.area());

    let header = Paragraph::new("Orderbook Aggregator TUI")
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(header, chunks[0]);

    let current_time = chrono::Local::now().format("%H:%M:%S").to_string();

    let spread_info = if let Ok(state) = app.state.try_lock() {
        let status = state.status.clone();
        if let Some(ref summary) = state.summary {
            let (last_update_text, time_color) = if let Some(last_update) = state.last_update {
                let elapsed = last_update.elapsed();
                let elapsed_ms = elapsed.as_millis();

                let (text, color) = if elapsed_ms < 1000 {
                    (format!("{}ms ago", elapsed_ms), Color::Green)
                } else if elapsed_ms < 5000 {
                    (
                        format!("{:.1}s ago", elapsed_ms as f32 / 1000.0),
                        Color::Yellow,
                    )
                } else {
                    (
                        format!("{:.1}s ago", elapsed_ms as f32 / 1000.0),
                        Color::Red,
                    )
                };

                (text, color)
            } else {
                ("No updates yet".to_string(), Color::Red)
            };

            Paragraph::new(vec![Line::from(vec![
                Span::styled("Spread: ", Style::default().fg(Color::White)),
                Span::styled(
                    format!("{:.8}", summary.spread),
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" | ", Style::default().fg(Color::Gray)),
                Span::styled("Time: ", Style::default().fg(Color::White)),
                Span::styled(
                    current_time,
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" | ", Style::default().fg(Color::Gray)),
                Span::styled("Last: ", Style::default().fg(Color::White)),
                Span::styled(
                    last_update_text,
                    Style::default().fg(time_color).add_modifier(Modifier::BOLD),
                ),
                Span::styled(" | ", Style::default().fg(Color::Gray)),
                Span::styled("Status: ", Style::default().fg(Color::White)),
                Span::styled(
                    status.to_display_string(),
                    Style::default()
                        .fg(status.to_color())
                        .add_modifier(Modifier::BOLD),
                ),
            ])])
            .block(Block::default().borders(Borders::ALL).title("Market Info"))
        } else {
            Paragraph::new(vec![Line::from(vec![
                Span::styled("Spread: ", Style::default().fg(Color::White)),
                Span::styled(
                    "N/A",
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" | ", Style::default().fg(Color::Gray)),
                Span::styled("Time: ", Style::default().fg(Color::White)),
                Span::styled(
                    current_time,
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" | ", Style::default().fg(Color::Gray)),
                Span::styled("Last: ", Style::default().fg(Color::White)),
                Span::styled(
                    "N/A",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Span::styled(" | ", Style::default().fg(Color::Gray)),
                Span::styled("Status: ", Style::default().fg(Color::White)),
                Span::styled(
                    status.to_display_string(),
                    Style::default()
                        .fg(status.to_color())
                        .add_modifier(Modifier::BOLD),
                ),
            ])])
            .block(Block::default().borders(Borders::ALL).title("Market Info"))
        }
    } else {
        Paragraph::new(vec![Line::from(vec![
            Span::styled("Spread: ", Style::default().fg(Color::White)),
            Span::styled(
                "N/A",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
        ])])
        .block(Block::default().borders(Borders::ALL).title("Market Info"))
    };
    f.render_widget(spread_info, chunks[1]);

    let orderbook_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(chunks[2]);

    let bids_table = {
        let mut bid_rows = Vec::new();
        if let Ok(state) = app.state.try_lock() {
            if let Some(summary) = state.summary.clone() {
                let bids = summary.bids;
                bid_rows = bids
                    .into_iter()
                    .map(|bid| {
                        Row::new(vec![
                            Cell::from(bid.exchange.as_str().to_string()),
                            Cell::from(format!("{:.8}", bid.price)),
                            Cell::from(format!("{:.8}", bid.amount)),
                        ])
                    })
                    .collect();
            }
        }

        Table::new::<Vec<Row>, [Constraint; 3]>(
            bid_rows,
            [
                Constraint::Percentage(30),
                Constraint::Percentage(35),
                Constraint::Percentage(35),
            ],
        )
        .header(
            Row::new(vec!["Exchange", "Price", "Amount"]).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Bids (Buy Orders)"),
        )
    };
    f.render_widget(bids_table, orderbook_chunks[0]);

    let asks_table = {
        let mut ask_rows = Vec::new();
        if let Ok(state) = app.state.try_lock() {
            if let Some(summary) = state.summary.clone() {
                let asks = summary.asks;
                ask_rows = asks
                    .into_iter()
                    .map(|ask| {
                        Row::new(vec![
                            Cell::from(ask.exchange.as_str().to_string()),
                            Cell::from(format!("{:.8}", ask.price)),
                            Cell::from(format!("{:.8}", ask.amount)),
                        ])
                    })
                    .collect();
            }
        }

        Table::new::<Vec<Row>, [Constraint; 3]>(
            ask_rows,
            [
                Constraint::Percentage(30),
                Constraint::Percentage(35),
                Constraint::Percentage(35),
            ],
        )
        .header(
            Row::new(vec!["Exchange", "Price", "Amount"]).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Asks (Sell Orders)"),
        )
    };
    f.render_widget(asks_table, orderbook_chunks[1]);

    let footer = Paragraph::new("Press 'q' to quit")
        .style(Style::default().fg(Color::Gray))
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, chunks[3]);
}

async fn connect_to_server() -> Result<(
    protos::orderbook_aggregator_client::OrderbookAggregatorClient<tonic::transport::Channel>,
    impl futures_util::Stream<Item = Result<protos::Summary, tonic::Status>> + Unpin + Send,
)> {
    let mut client = match protos::orderbook_aggregator_client::OrderbookAggregatorClient::connect(
        "http://0.0.0.0:50051",
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            return Err(e.into());
        }
    };

    let stream = client
        .book_summary(tonic::Request::new(protos::Empty {}))
        .await?
        .into_inner();

    Ok((client, stream))
}

async fn data_consumer(
    mut stream: impl futures_util::Stream<Item = Result<protos::Summary, tonic::Status>> + Unpin + Send,
    state: Arc<Mutex<AppState>>,
) {
    while let Some(summary_result) = stream.next().await {
        match summary_result {
            Ok(summary) => {
                let mut app_state = state.lock().await;
                app_state.update_summary(summary);
            }
            Err(e) => {
                tracing::debug!("Error receiving summary: {:?}", e);
            }
        }
    }
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: App,
) -> Result<()> {
    let frame_duration = frame_duration();
    let state = app.state.clone();
    let mut data_task: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        if event::poll(Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Char('Q') {
                    break;
                }
            }
        }

        let data_task_finished = data_task
            .as_ref()
            .map(|task| task.is_finished())
            .unwrap_or(false);

        if data_task.is_none() || data_task_finished {
            if data_task.is_some() {
                if let Ok(mut app_state) = state.try_lock() {
                    app_state.update_status(ConnectionStatus::Disconnected, DataStatus::Unreliable);
                }
            }

            if let Some(task) = data_task.take() {
                task.abort();
            }

            let state_clone = state.clone();
            data_task = Some(tokio::spawn(async move {
                loop {
                    if let Ok(mut app_state) = state_clone.try_lock() {
                        app_state
                            .update_status(ConnectionStatus::Connecting, DataStatus::Unreliable);
                    }

                    match connect_to_server().await {
                        Ok((_client, stream)) => {
                            tracing::debug!("Connected to server successfully");
                            if let Ok(mut app_state) = state_clone.try_lock() {
                                app_state.update_status(
                                    ConnectionStatus::Connected,
                                    DataStatus::Reliable,
                                );
                            }
                            let state_clone = state_clone.clone();
                            data_consumer(stream, state_clone).await;
                            tracing::debug!("Data stream ended, reconnecting...");
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Failed to connect to server, retrying in 1s...: {:?}",
                                e
                            );
                            if let Ok(mut app_state) = state_clone.try_lock() {
                                app_state.update_status(
                                    ConnectionStatus::Reconnecting,
                                    DataStatus::Unreliable,
                                );
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }));
        }

        terminal.draw(|f| ui(f, &app))?;

        tokio::time::sleep(frame_duration).await;
    }

    if let Some(task) = data_task {
        task.abort();
    }

    disable_raw_mode()?;
    execute!(std::io::stdout(), LeaveAlternateScreen, DisableMouseCapture)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install().expect("Failed to install panic hook");
    tracing_subscriber::fmt::init();

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let app = App {
        state: Arc::new(Mutex::new(AppState::new())),
    };
    let res = run_app(&mut terminal, app).await;

    let _ = disable_raw_mode();
    let _ = execute!(std::io::stdout(), LeaveAlternateScreen, DisableMouseCapture);

    if let Err(err) = res {
        println!("{err:?}");
    }

    Ok(())
}
