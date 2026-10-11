#[derive(Clone, Copy)]
pub struct SimulatedSnapshot {
    pub target: &'static str,
    pub connection_status: &'static str,
    pub context: &'static str,
    pub namespace: &'static str,
    pub resource_summary: &'static str,
}

pub fn initial_snapshot() -> SimulatedSnapshot {
    SimulatedSnapshot {
        target: "local-demo",
        connection_status: "Disconnected",
        context: "demo-context",
        namespace: "payment-platform",
        resource_summary: "5 sample resources",
    }
}