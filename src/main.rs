mod app;
pub mod cancellation;
pub mod connection;
pub mod diagnostics;
pub mod exec_plugin;
pub mod kubeconfig;
mod simulated_data;
mod simulation;
pub mod settings;
pub mod secure_store;
pub mod target;
mod ui;

fn main() {
    app::run();
}
