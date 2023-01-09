use crate::orderbook::{Level, Side};
use crossterm::{
    event::EnableMouseCapture,
    execute,
    terminal::{enable_raw_mode, EnterAlternateScreen},
};
use std::io;
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Cell, Row, Table, TableState},
    Frame, Terminal,
};

pub fn setup_terminal_ui() -> Terminal<CrosstermBackend<io::Stdout>> {
    let _ = enable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(stdout, EnterAlternateScreen, EnableMouseCapture);
    let backend = CrosstermBackend::new(stdout);
    return Terminal::new(backend).unwrap();
}

pub fn render_orderbook<B: Backend>(f: &mut Frame<B>, _: &String, levels: Vec<&Level>) {
    let chunks = Layout::default()
        .constraints([Constraint::Percentage(100)].as_ref())
        .margin(1)
        .split(f.size());

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

    f.render_stateful_widget(table, chunks[0], &mut TableState::default());
}