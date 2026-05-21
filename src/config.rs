use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ptwop", version, about = "P2P TCP tunnel over UDP")]
pub struct Cli {
    #[command(subcommand)]
    pub mode: Mode,
}

#[derive(clap::Subcommand, Debug)]
pub enum Mode {
    /// Run in signaling-server mode (public IP required)
    Server {
        #[arg(long, default_value = "0.0.0.0:7788")]
        listen: String,
    },
    /// Run in host mode — exposes a local TCP service to the tunnel
    Host {
        #[arg(long)]
        server: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        secret: Option<String>,
    },
    /// Run in client mode — connects to a room and forwards to local port
    Client {
        #[arg(long)]
        server: String,
        #[arg(long)]
        room: String,
        #[arg(long)]
        local_port: u16,
        #[arg(long)]
        secret: Option<String>,
    },
}
