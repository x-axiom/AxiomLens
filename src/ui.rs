use gpui_kit::{Context, IntoElement, ParentElement, Render, Window, div};

use crate::simulated_data::SimulatedSnapshot;

pub struct AxiomLensPrototype {
    snapshot: SimulatedSnapshot,
}

impl AxiomLensPrototype {
    pub fn new(snapshot: SimulatedSnapshot) -> Self {
        Self { snapshot }
    }
}

impl Render for AxiomLensPrototype {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
            .child("AxiomLens")
            .child("SIMULATED DATA - no cluster connection")
            .child(self.snapshot.target)
            .child(self.snapshot.resource_summary)
    }
}