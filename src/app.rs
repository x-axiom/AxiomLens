use gpui_kit::{AppContext, Bounds, WindowBounds, WindowOptions, px, size};
use gpui_kit::component::{Theme, ThemeMode};

use crate::{simulated_data, ui::{self, AxiomLensPrototype}};

pub fn run() {
    gpui_kit::application().run(|cx| {
        gpui_kit::init(cx);
        Theme::change(ThemeMode::Dark, None, cx);
        cx.on_window_closed(|cx, _| {
            if cx.windows().is_empty() {
                cx.quit();
            }
        })
        .detach();

        let snapshot = simulated_data::initial_snapshot();
        let window_options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                None,
                size(px(1440.0), px(900.0)),
                cx,
            ))),
            window_min_size: Some(size(px(1280.0), px(800.0))),
            ..WindowOptions::default()
        };
        gpui_kit::open_window(window_options, cx, |window, cx| {
            let workbench = ui::build_workbench(snapshot, window, cx);
            cx.new(move |cx| AxiomLensPrototype::new(workbench, cx))
        })
        .expect("failed to open AxiomLens main window");
    });
}