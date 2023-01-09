use crate::orderbook::{Level, Side};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use rust_decimal_macros::dec;
use std::io;
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Layout},
    style::{Color, Style},
    text::{Span, Spans},
    widgets::{Cell, Paragraph, Row, Table, TableState},
    Frame, Terminal,
};

pub fn setup_terminal_ui() -> Terminal<CrosstermBackend<io::Stdout>> {
    let _ = enable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(stdout, EnterAlternateScreen, EnableMouseCapture);
    let backend = CrosstermBackend::new(stdout);
    return Terminal::new(backend).unwrap();
}

pub fn check_for_exit_signal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> bool {
    if let Event::Key(key) = event::read().unwrap() {
        match key.code {
            KeyCode::Char('q') => {
                disable_raw_mode().unwrap();
                execute!(
                    terminal.backend_mut(),
                    LeaveAlternateScreen,
                    DisableMouseCapture
                )
                .unwrap();
                terminal.show_cursor().unwrap();
                return true;
            }
            _ => return false,
        }
    }

    false
}

pub fn render_orderbook<B: Backend>(f: &mut Frame<B>, symbol: &String, levels: Vec<&Level>) {
    let chunks = Layout::default()
        .constraints([Constraint::Min(4), Constraint::Percentage(90)].as_ref())
        .margin(1)
        .split(f.size());

    let best_bid = levels.iter().find(|x| x.side == Side::Bid).unwrap();
    let best_ask = levels.iter().find(|x| x.side == Side::Ask).unwrap();
    let spread = best_ask.price - best_bid.price;
    let spread_pct = spread / best_ask.price * dec!(100);

    let text = Paragraph::new(vec![
        Spans::from(Span::styled(
            format!("Symbol: {}", symbol),
            Style::default().fg(Color::Blue),
        )),
        Spans::from(Span::styled(
            format!("Spread: ${} ({:.5} %)", spread, spread_pct),
            Style::default().fg(Color::Blue),
        )),
    ]);
    f.render_widget(text, chunks[0]);

    let rows = levels.iter().map(|l| {
        Row::new(vec![
            Cell::from(format!("{:.2}", l.price)),
            Cell::from(format!("{:.10}", l.quantity)),
            Cell::from(format!("{}", l.venue)),
        ])
        .style(Style::default().fg(match l.side {
            Side::Bid => Color::Green,
            Side::Ask => Color::Red,
        }))
    });

    let table = Table::new(rows)
        .header(
            Row::new(vec!["Price", "Quantity", "Venue"])
                .style(Style::default().fg(Color::White))
                .bottom_margin(1),
        )
        .widths(&[
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Min(20),
        ])
        .column_spacing(2);

    f.render_stateful_widget(table, chunks[1], &mut TableState::default());
}
