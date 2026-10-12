mod app;
pub mod diagnostics;
mod simulated_data;
mod simulation;
pub mod settings;
pub mod secure_store;
mod ui;

fn main() {
    app::run();
}
