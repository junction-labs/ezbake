use clap::Parser;

#[derive(clap::ValueEnum, Clone, Default, Debug)]
pub enum IngestScope {
    #[default]
    Cluster,
    DefaultNamespace,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short = 'i', long, default_value_t, value_enum)]
    pub ingest_scope: IngestScope,
}
