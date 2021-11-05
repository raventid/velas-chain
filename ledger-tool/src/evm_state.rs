use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{value_t_or_exit, App, AppSettings, Arg, ArgMatches, SubCommand};
use log::*;
use solana_clap_utils::ArgConstant;

use evm_state::storage::Storage;
use evm_state::H256;

use solana_runtime::evm_snapshot::{cleaner, copy_and_purge, inspectors, Walker};

pub trait EvmStateSubCommand {
    fn evm_state_subcommand(self) -> Self;
}

const ROOT_ARG: ArgConstant<'static> = ArgConstant {
    name: "root",
    long: "root",
    help: "EVM state root hash",
};

impl EvmStateSubCommand for App<'_, '_> {
    fn evm_state_subcommand(self) -> Self {
        self.subcommand(
            SubCommand::with_name("evm_state")
                .about("EVM state utilities")
                .setting(AppSettings::ArgRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("purge")
                        .about("Cleanup EVM state data unreachable from state root")
                        .arg(
                            Arg::with_name(ROOT_ARG.name)
                                .long(ROOT_ARG.long)
                                .required(true)
                                .takes_value(true)
                                .help(ROOT_ARG.help),
                        )
                        .arg(
                            Arg::with_name("dry_run")
                                .long("dry-run")
                                .help("Do nothing, just collect hashes and print them"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("copy")
                        .about("Copy EVM accounts state into destination RocksDB")
                        .setting(AppSettings::ArgRequiredElseHelp)
                        .arg(
                            Arg::with_name(ROOT_ARG.name)
                                .long(ROOT_ARG.long)
                                .required(true)
                                .takes_value(true)
                                .help(ROOT_ARG.help),
                        )
                        .arg(
                            Arg::with_name("destination")
                                .long("destination")
                                .required(true)
                                .takes_value(true)
                                .help("Path to destination RocksDB"),
                        ),
                ),
        )
    }
}

pub fn process_evm_state_command(ledger_path: &Path, matches: &ArgMatches<'_>) -> Result<()> {
    let pool = rayon::ThreadPoolBuilder::new().build()?; // make panic abort, by using default threadpool config

    pool.install(|| match matches.subcommand() {
        ("purge", Some(matches)) => {
            let evm_state_path = ledger_path.join("evm-state");
            let storage = Storage::open_persistent(evm_state_path)?;

            let root = value_t_or_exit!(matches, ROOT_ARG.name, H256);
            let is_dry_run = matches.is_present("dry_run");

            assert!(storage.check_root_exist(root));
            let db = storage.db();

            if is_dry_run {
                info!("Dry run, do nothing after collecting keys ...");
            }

            let accounts_state_walker =
                Walker::new(db, inspectors::memorizer::AccountsKeysCollector::default());
            accounts_state_walker.traverse(root)?;
            accounts_state_walker.inspector.summarize();

            let storages_walker =
                Walker::new(db, inspectors::memorizer::StoragesKeysCollector::default());
            for storage_root in accounts_state_walker.inspector.storage_roots.iter() {
                storages_walker.traverse(*storage_root)?;
            }
            storages_walker.inspector.summarize();

            if !is_dry_run {
                let cleaner = cleaner::Cleaner::new_with(
                    db,
                    accounts_state_walker.inspector,
                    storages_walker.inspector,
                );
                cleaner.cleanup()?;
            }
            Ok(())
        }
        ("copy", Some(matches)) => {
            let root = value_t_or_exit!(matches, ROOT_ARG.name, H256);
            let destination = value_t_or_exit!(matches, "destination", PathBuf);

            let evm_state_path = ledger_path.join("evm-state");
            copy_and_purge(&evm_state_path, &destination, root)
        }
        unhandled => panic!("Unhandled {:?}", unhandled),
    })
}
