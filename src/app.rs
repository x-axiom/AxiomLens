use gpui_kit::{AppContext, WindowOptions};

use crate::{simulated_data, ui::AxiomLensPrototype};

pub fn run() {
    gpui_kit::application().run(|cx| {
        gpui_kit::init(cx);
        cx.on_window_closed(|cx, _| {
            if cx.windows().is_empty() {
                cx.quit();
            }
        })
        .detach();

        let snapshot = simulated_data::initial_snapshot();
        gpui_kit::open_window(WindowOptions::default(), cx, |_, cx| {
            cx.new(|_| AxiomLensPrototype::new(snapshot))
        })
        .expect("failed to open AxiomLens main window");
    });
}