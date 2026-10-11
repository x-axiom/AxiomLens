use gpui_kit::{AppContext, Context, IntoElement, ParentElement, Render, Window, WindowOptions, div};

struct AxiomLensPrototype;

impl Render for AxiomLensPrototype {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().child("AxiomLens dependency smoke test")
    }
}

fn main() {
    gpui_kit::application().run(|cx| {
        gpui_kit::init(cx);
        gpui_kit::open_window(WindowOptions::default(), cx, |_, cx| {
            cx.new(|_| AxiomLensPrototype)
        })
        .expect("failed to open AxiomLens prototype");
    });
}
