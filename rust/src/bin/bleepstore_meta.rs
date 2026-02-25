//! CLI entry point for bleepstore-meta: metadata export/import tool.

use bleepstore::serialization::{
    export_metadata, import_metadata, ExportOptions, ImportOptions, ALL_TABLES,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bleepstore-meta", about = "BleepStore metadata export/import tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export metadata to JSON
    Export {
        #[arg(long, default_value = "bleepstore.yaml")]
        config: PathBuf,
        #[arg(long)]
        db: Option<String>,
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long, default_value = "-")]
        output: String,
        #[arg(long)]
        tables: Option<String>,
        #[arg(long, default_value_t = false)]
        include_credentials: bool,
    },
    /// Import metadata from JSON
    Import {
        #[arg(long, default_value = "bleepstore.yaml")]
        config: PathBuf,
        #[arg(long)]
        db: Option<String>,
        #[arg(long, default_value = "-")]
        input: String,
        #[arg(long, default_value_t = false)]
        replace: bool,
    },
}

fn resolve_db_path(config_path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(config_path)?;
    let raw: serde_yaml::Value = serde_yaml::from_str(&content)?;
    let path = raw
        .get("metadata")
        .and_then(|m| m.get("sqlite"))
        .and_then(|s| s.get("path"))
        .and_then(|p| p.as_str())
        .unwrap_or("./data/metadata.db");
    Ok(path.to_string())
}

fn main() {
    let cli = Cli::parse();
    let rc = match cli.command {
        Commands::Export {
            config,
            db,
            format,
            output,
            tables,
            include_credentials,
        } => run_export(config, db, format, output, tables, include_credentials),
        Commands::Import {
            config,
            db,
            input,
            replace,
        } => run_import(config, db, input, replace),
    };
    std::process::exit(rc);
}

fn run_export(
    config: PathBuf,
    db: Option<String>,
    format: String,
    output: String,
    tables: Option<String>,
    include_credentials: bool,
) -> i32 {
    if format != "json" {
        eprintln!("Error: unsupported format: {}", format);
        return 1;
    }

    let db_path = match db {
        Some(p) => p,
        None => match resolve_db_path(&config) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Error reading config: {}", e);
                return 1;
            }
        },
    };

    let table_list: Vec<String> = match tables {
        Some(t) => {
            let list: Vec<String> = t.split(',').map(|s| s.trim().to_string()).collect();
            for name in &list {
                if !ALL_TABLES.contains(&name.as_str()) {
                    eprintln!("Error: invalid table name: {}", name);
                    return 1;
                }
            }
            list
        }
        None => ALL_TABLES.iter().map(|s| s.to_string()).collect(),
    };

    let opts = ExportOptions {
        tables: table_list,
        include_credentials,
    };

    match export_metadata(&db_path, &opts) {
        Ok(result) => {
            if output == "-" {
                println!("{}", result);
            } else {
                if let Err(e) = std::fs::write(&output, format!("{}\n", result)) {
                    eprintln!("Error writing output: {}", e);
                    return 1;
                }
                eprintln!("Exported to {}", output);
            }
            0
        }
        Err(e) => {
            eprintln!("Error exporting: {}", e);
            1
        }
    }
}

fn run_import(config: PathBuf, db: Option<String>, input: String, replace: bool) -> i32 {
    let db_path = match db {
        Some(p) => p,
        None => match resolve_db_path(&config) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Error reading config: {}", e);
                return 1;
            }
        },
    };

    let json_str = if input == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf).unwrap();
        buf
    } else {
        match std::fs::read_to_string(&input) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                return 1;
            }
        }
    };

    let opts = ImportOptions { replace };

    match import_metadata(&db_path, &json_str, &opts) {
        Ok(result) => {
            for table in ALL_TABLES {
                if let Some(count) = result.counts.get(*table) {
                    let skip = result.skipped.get(*table).unwrap_or(&0);
                    let mut msg = format!("  {}: {} imported", table, count);
                    if *skip > 0 {
                        msg.push_str(&format!(", {} skipped", skip));
                    }
                    eprintln!("{}", msg);
                }
            }
            for w in &result.warnings {
                eprintln!("  WARNING: {}", w);
            }
            0
        }
        Err(e) => {
            eprintln!("Error importing: {}", e);
            1
        }
    }
}
