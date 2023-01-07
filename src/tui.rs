use tui::{
    backend::Backend,
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Cell, Row, Table, TableState},
    Frame,
};

use crate::orderbook::{Level, Side};

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

// struct App {
//     state: TableState,
// }

// impl App {
//     fn new() -> App {
//         App {
//             state: TableState::default(),
//         }
//     }
// }

// fn main() -> Result<(), Box<dyn Error>> {
//     // setup terminal
//     enable_raw_mode()?;
//     let mut stdout = io::stdout();
//     execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
//     let backend = CrosstermBackend::new(stdout);
//     let mut terminal = Terminal::new(backend)?;

//     // create app and run it
//     let app = App::new();
//     let res = run_app(&mut terminal, app);

//     terminal.draw(|f| render_orderbook(f))?;

//     if let Event::Key(key) = event::read()? {
//         match key.code {
//             KeyCode::Char('q') => return Ok(()),
//             // KeyCode::Down => app.next(),
//             // KeyCode::Up => app.previous(),
//             _ => {}
//         }
//     }

//     // restore terminal
//     disable_raw_mode()?;
//     execute!(
//         terminal.backend_mut(),
//         LeaveAlternateScreen,
//         DisableMouseCapture
//     )?;
//     terminal.show_cursor()?;

//     if let Err(err) = res {
//         println!("{:?}", err)
//     }

//     Ok(())
// }

// fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> io::Result<()> {
//     loop {
//         terminal.draw(|f| render_orderbook(f))?;

//         if let Event::Key(key) = event::read()? {
//             match key.code {
//                 KeyCode::Char('q') => return Ok(()),
//                 // KeyCode::Down => app.next(),
//                 // KeyCode::Up => app.previous(),
//                 _ => {}
//             }
//         }
//     }
// }
