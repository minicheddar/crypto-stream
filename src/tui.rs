use crate::orderbook::{Level, Side};
use tui::{
    backend::Backend,
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Cell, Row, Table, TableState},
    Frame,
};

pub fn render_orderbook<B: Backend>(f: &mut Frame<B>, levels: Vec<&Level>) {
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
