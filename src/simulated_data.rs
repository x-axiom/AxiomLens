#[derive(Clone, Copy)]
pub struct SimulatedSnapshot {
    pub target: &'static str,
    pub resource_summary: &'static str,
}

pub fn initial_snapshot() -> SimulatedSnapshot {
    SimulatedSnapshot {
        target: "No cluster connected",
        resource_summary: "No live resources loaded",
    }
}