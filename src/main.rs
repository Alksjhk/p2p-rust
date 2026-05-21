mod config;
mod signal;
mod protocol;
mod crypto;
mod server;
mod host;
mod client;
mod punch;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = config::Cli::parse();
    match cli.mode {
        config::Mode::Server { listen } => server::run(listen).await,
        config::Mode::Host { server, target, secret } => host::run(server, target, secret).await,
        config::Mode::Client { server, room, local_port, secret } => {
            client::run(server, room, local_port, secret).await
        }
    }
}
