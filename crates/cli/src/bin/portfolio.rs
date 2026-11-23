use clap::Parser;
use reliant_cli::load_portfolio;
use reliant_datamodel::Event;
use reliant_messaging::{InMemoryMessageBus, MessageBus, NatsMessageBus, subject};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "reliant-portfolio", about = "Portfolio node: schedules fetches for its instruments")]
struct Cli {
    #[arg(long, default_value = "portfolio")]
    name: String,
    #[arg(long, default_value = "AUDCAD")]
    instrument: String,
    #[arg(long)]
    portfolio: Option<String>,
    #[arg(long, default_value = "1d")]
    timeframes: String,
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    bus: String,
    #[arg(long, default_value = "3600")]
    interval: u64,
}

fn main() {
    let cli = Cli::parse();
    let instruments: Vec<String> = if let Some(p) = &cli.portfolio {
        let loaded = load_portfolio(p);
        if loaded.is_empty() { vec![cli.instrument.clone()] } else { loaded }
    } else {
        vec![cli.instrument.clone()]
    };
    let tfs: Vec<String> = cli.timeframes.split(',').map(|s| s.to_string()).collect();

    let shutdown = reliant_cli::shutdown_flag();
    let mut bus = NatsMessageBus::new(&cli.bus);
    let topic = subject("", "fetch");
    println!("portfolio '{}': requesting fetch every {}s for {} instruments", cli.name, cli.interval, instruments.len());

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) { break; }
        let req = Event::FetchRequested {
            symbols: instruments.clone(),
            timeframes: tfs.clone(),
        };
        bus.publish(&topic, req);
        println!("portfolio '{}': published FetchRequested", cli.name);
        std::thread::sleep(Duration::from_secs(cli.interval));
    }
    println!("portfolio: shutting down");
}
