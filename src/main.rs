mod app;
pub mod cancellation;
pub mod connection;
pub mod diagnostics;
pub mod exec_plugin;
pub mod fact_service;
pub mod kubeconfig;
pub mod resource_browser;
mod simulated_data;
mod simulation;
pub mod settings;
pub mod secure_store;
pub mod target;
mod ui;

fn main() {
    app::run();
}
