use gpui_kit::{
    App, AppContext as _, Context, Div, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement as _, IntoElement, MouseButton, ParentElement as _, Render, SharedString,
    Size, Styled as _, Task, Window, div, px, rgb, size,
};
use gpui_kit::component::button::{Button, ButtonVariants as _};
use gpui_kit::component::dock::{
    BasePanel, DockArea, DockLayout, DockPlacement, DockSkin, Panel as DockPanel, PanelEvent,
    panel_handle,
};
use gpui_kit::component::input::{Editor, EditorState};
use gpui_kit::component::scroll::ScrollableElement as _;
use gpui_kit::component::v_virtual_list;
use std::rc::Rc;
use std::{collections::VecDeque, sync::{Arc, atomic::{AtomicBool, Ordering}}};

use crate::{simulated_data::SimulatedSnapshot, simulation::{self, SimulatedTaskMessage, SimulatedTaskMode}};

const BG: u32 = 0x0b0e13;
const PANEL: u32 = 0x11151c;
const RAISED: u32 = 0x0e1218;
const ELEVATED: u32 = 0x161b23;
const BORDER: u32 = 0x232a35;
const TEXT_1: u32 = 0xe7ebf2;
const TEXT_2: u32 = 0xc7ceda;
const TEXT_3: u32 = 0x9aa4b2;
const TEXT_4: u32 = 0x8b95a3;
const TEXT_5: u32 = 0x646e7e;
const ACCENT: u32 = 0x4c8fd6;
const ACCENT_SOFT: u32 = 0x16222f;
const ACCENT_SOFT_2: u32 = 0x18283a;
const OK: u32 = 0x3fb97e;
const WARN: u32 = 0xe0a24e;
const DANGER: u32 = 0xe5564d;
const INFO: u32 = 0x5aa9e6;
const SAMPLE_YAML: &str = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: payment-api\n  namespace: payment-platform\nspec:\n  replicas: 6\n  strategy:\n    type: RollingUpdate\n  template:\n    spec:\n      containers:\n        - name: payment-api\n          image: nginx:1.25-alpine\n          ports:\n            - containerPort: 8080\n";

pub struct AxiomLensPrototype {
    dock_area: Entity<DockArea>,
    snapshot: SimulatedSnapshot,
}

impl AxiomLensPrototype {
    pub fn new(dock_area: Entity<DockArea>, snapshot: SimulatedSnapshot) -> Self {
        Self {
            dock_area,
            snapshot,
        }
    }
}

impl Render for AxiomLensPrototype {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(BG))
            .child(topbar(self.snapshot))
            .child(div().flex_1().child(self.dock_area.clone()))
    }
}

#[derive(Clone, Copy)]
enum PanelKind {
    Sidebar,
    Resources,
    Details,
    Activity,
    Agent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DetailView {
    Overview,
    Yaml,
    Diff,
}

struct WorkspacePanel {
    panel_id: &'static str,
    title: &'static str,
    kind: PanelKind,
    snapshot: SimulatedSnapshot,
    focus_handle: FocusHandle,
    resource_rows: Option<Rc<Vec<ResourceEntry>>>,
    resource_row_sizes: Option<Rc<Vec<Size<gpui_kit::Pixels>>>>,
    selected_resource: usize,
    detail_view: DetailView,
    yaml_editor: Option<Entity<EditorState>>,
    activity_events: VecDeque<SimulatedTaskMessage>,
    activity_status: SharedString,
    activity_running: bool,
    cancellation: Option<Arc<AtomicBool>>,
    run_id: u64,
    worker_task: Option<Task<()>>,
    consumer_task: Option<Task<()>>,
}

impl WorkspacePanel {
    fn new(
        panel_id: &'static str,
        title: &'static str,
        kind: PanelKind,
        snapshot: SimulatedSnapshot,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let yaml_editor = matches!(kind, PanelKind::Details).then(|| {
            cx.new(|cx| EditorState::new(window, cx).default_value(SAMPLE_YAML).language("yaml"))
        });
        cx.new(|cx| Self {
            panel_id,
            title,
            kind,
            snapshot,
            focus_handle: cx.focus_handle(),
            resource_rows: matches!(kind, PanelKind::Resources).then(resource_entries),
            resource_row_sizes: matches!(kind, PanelKind::Resources).then(|| {
                Rc::new(vec![size(px(1.0), px(38.0)); RESOURCE_COUNT])
            }),
            selected_resource: 0,
            detail_view: DetailView::Overview,
            yaml_editor,
            activity_events: VecDeque::new(),
            activity_status: SharedString::from("IDLE / LOCAL FAKE TASK"),
            activity_running: false,
            cancellation: None,
            run_id: 0,
            worker_task: None,
            consumer_task: None,
        })
    }

    fn start_simulated_task(&mut self, mode: SimulatedTaskMode, cx: &mut Context<Self>) {
        if self.activity_running {
            return;
        }

        self.run_id += 1;
        let run_id = self.run_id;
        let cancellation = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = async_channel::bounded(simulation::CHANNEL_CAPACITY);
        self.activity_events.clear();
        self.activity_status = SharedString::from(format!("RUNNING / {}", mode.label()));
        self.activity_running = true;
        self.cancellation = Some(cancellation.clone());

        self.worker_task = Some(simulation::spawn(
            cx.background_executor().clone(),
            sender,
            cancellation,
            mode,
        ));

        self.consumer_task = Some(cx.spawn(async move |panel, cx| {
            while let Ok(first) = receiver.recv().await {
                let mut batch = Vec::with_capacity(simulation::UI_BATCH_LIMIT);
                batch.push(first);
                while batch.len() < simulation::UI_BATCH_LIMIT {
                    match receiver.try_recv() {
                        Ok(message) => batch.push(message),
                        Err(_) => break,
                    }
                }

                let terminal_status = batch.iter().rev().find_map(task_terminal_status);
                let is_terminal = batch.iter().any(SimulatedTaskMessage::is_terminal);
                let update = panel.update(cx, move |panel, cx| {
                    if panel.run_id != run_id {
                        return;
                    }
                    for message in batch {
                        if panel.activity_events.len() == simulation::FEED_LIMIT {
                            panel.activity_events.pop_front();
                        }
                        panel.activity_events.push_back(message);
                    }
                    if let Some(status) = terminal_status {
                        panel.activity_status = status;
                        panel.activity_running = false;
                        panel.cancellation = None;
                        panel.worker_task = None;
                    }
                    cx.notify();
                });
                if update.is_err() || is_terminal {
                    break;
                }
            }
        }));

        cx.notify();
    }

    fn cancel_simulated_task(&mut self, cx: &mut Context<Self>) {
        if let Some(cancellation) = &self.cancellation {
            cancellation.store(true, Ordering::Release);
            self.activity_status = SharedString::from("CANCELLING / waiting for mock task");
            cx.notify();
        }
    }

    fn render_activity(&mut self, cx: &mut Context<Self>) -> Div {
        let success_button = Button::new("simulate-success")
            .label("Delay success")
            .compact()
            .secondary()
            .on_click(cx.listener(|panel, _, _, cx| {
                panel.start_simulated_task(SimulatedTaskMode::DelayedSuccess, cx);
            }));
        let failure_button = Button::new("simulate-failure")
            .label("Simulate 503")
            .compact()
            .secondary()
            .on_click(cx.listener(|panel, _, _, cx| {
                panel.start_simulated_task(SimulatedTaskMode::DelayedFailure, cx);
            }));
        let burst_button = Button::new("simulate-burst")
            .label("Burst 64")
            .compact()
            .secondary()
            .on_click(cx.listener(|panel, _, _, cx| {
                panel.start_simulated_task(SimulatedTaskMode::Burst, cx);
            }));
        let cancel_button = Button::new("simulate-cancel")
            .label("Cancel")
            .compact()
            .ghost()
            .on_click(cx.listener(|panel, _, _, cx| panel.cancel_simulated_task(cx)));

        let feed_rows = if self.activity_events.is_empty() && self.activity_running {
            vec![log_line("TASK", "INFO", "Waiting for simulated events...")]
        } else if self.activity_events.is_empty() {
            LOG_ENTRIES.iter().copied().map(log_row).collect::<Vec<_>>()
        } else {
            self.activity_events
                .iter()
                .map(render_simulated_task_message)
                .collect::<Vec<_>>()
        };
        let task_status = self.activity_status.clone();
        let status_color = if self.activity_running { WARN } else { TEXT_5 };

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(RAISED))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .px_2()
                    .bg(rgb(RAISED))
                    .child(detail_tab("Logs", true))
                    .child(detail_tab("Events", false))
                    .child(div().flex_1())
                    .child(div().text_xs().text_color(rgb(TEXT_5)).child("SIMULATED FEED")),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .px_2()
                    .py_1()
                    .bg(rgb(RAISED))
                    .child(success_button)
                    .child(failure_button)
                    .child(burst_button)
                    .child(cancel_button)
                    .child(div().flex_1())
                    .child(div().text_xs().text_color(rgb(status_color)).child(task_status)),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .px_3()
                    .py_1()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .text_xs()
                            .font_family("Menlo")
                            .text_color(rgb(TEXT_5))
                            .child("queue 16 / batch 8 / retain 64 / overflow drops newest"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .px_3()
                    .py_2()
                    .bg(rgb(BG))
                    .children(feed_rows)
                    .overflow_y_scrollbar(),
            )
    }

    fn render_resources(&mut self, cx: &mut Context<Self>) -> Div {
        let item_sizes = self
            .resource_row_sizes
            .as_ref()
            .expect("resource panel has virtual row sizes")
            .clone();
        let rows = v_virtual_list(
            cx.entity(),
            "simulated-resources",
            item_sizes,
            |panel, visible_range, _, cx| {
                visible_range
                    .map(|index| panel.render_resource_row(index, cx))
                    .collect::<Vec<_>>()
            },
        );

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(BG))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .px_2()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .py_2()
                            .bg(rgb(BG))
                            .child(div().w(px(12.0)).h(px(12.0)).rounded_md().border_1().border_color(rgb(TEXT_4)))
                            .child(div().text_sm().text_color(rgb(TEXT_1)).child("Deployments")),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .px_3()
                    .py_2()
                    .bg(rgb(RAISED))
                    .child(div().text_sm().text_color(rgb(TEXT_1)).child("Deployments"))
                    .child(pill("5,000", 0x7fb4e6, ACCENT_SOFT_2))
                    .child(div().flex_1())
                    .child(div().text_xs().text_color(rgb(TEXT_5)).child("SIMULATED")),
            )
            .child(render_resource_header())
            .child(div().flex_1().child(rows))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .py_2()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child(self.snapshot.resource_summary),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child(self.selected_resource_name()),
                    ),
            )
    }

    fn render_resource_row(&self, index: usize, cx: &mut Context<Self>) -> Div {
        let entry = self
            .resource_rows
            .as_ref()
            .and_then(|rows| rows.get(index))
            .expect("virtual range is within resource data")
            .clone();
        let is_selected = self.selected_resource == index;
        let status_color = match entry.status {
            "READY" => OK,
            "DEGRADED" => WARN,
            _ => DANGER,
        };

        div()
            .flex()
            .items_center()
            .gap_2()
            .px_3()
            .h(px(38.0))
            .bg(rgb(if is_selected { ACCENT_SOFT } else { BG }))
            .child(
                div()
                    .flex_1()
                    .truncate()
                    .text_sm()
                    .text_color(rgb(if is_selected { TEXT_1 } else { TEXT_2 }))
                    .child(entry.name),
            )
            .child(fixed_cell(entry.status, 76.0, status_color, false))
            .child(fixed_cell(entry.replicas, 54.0, TEXT_2, true))
            .child(fixed_cell(entry.age, 42.0, TEXT_4, true))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |panel, _, _, cx| {
                    panel.selected_resource = index;
                    cx.notify();
                }),
            )
    }

    fn selected_resource_name(&self) -> SharedString {
        self.resource_rows
            .as_ref()
            .and_then(|rows| rows.get(self.selected_resource))
            .map(|row| row.name.clone())
            .unwrap_or_else(|| SharedString::from("no selection"))
    }

    fn detail_view_tab(
        &self,
        label: &'static str,
        view: DetailView,
        cx: &mut Context<Self>,
    ) -> Div {
        div()
            .px_2()
            .py_2()
            .rounded_md()
            .bg(rgb(if self.detail_view == view { RAISED } else { BG }))
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(if self.detail_view == view { TEXT_1 } else { TEXT_4 }))
                    .child(label),
            )
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |panel, _, _, cx| {
                    panel.detail_view = view;
                    cx.notify();
                }),
            )
    }

    fn render_details(&mut self, cx: &mut Context<Self>) -> Div {
        let body = match self.detail_view {
            DetailView::Overview => render_detail_overview(self.snapshot),
            DetailView::Yaml => self
                .yaml_editor
                .as_ref()
                .map(|state| {
                    div()
                        .flex_1()
                        .p_2()
                        .bg(rgb(BG))
                        .child(Editor::new(state).readonly(true).h(px(480.0)))
                })
                .unwrap_or_else(|| div().child("YAML fixture unavailable")),
            DetailView::Diff => render_simulated_diff(),
        };

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(RAISED))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .p_3()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(pill("Deployment", 0x7fb4e6, ACCENT_SOFT_2))
                            .child(div().text_sm().text_color(rgb(TEXT_1)).child("payment-api"))
                            .child(div().flex_1())
                            .child(pill("READY", OK, 0x14261c)),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child("payment-platform / sample resource"),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .px_2()
                    .bg(rgb(BG))
                    .child(self.detail_view_tab("Overview", DetailView::Overview, cx))
                    .child(self.detail_view_tab("YAML", DetailView::Yaml, cx))
                    .child(self.detail_view_tab("Diff", DetailView::Diff, cx)),
            )
            .child(body)
    }
}

impl BasePanel for WorkspacePanel {
    fn panel_name(&self) -> &'static str {
        self.panel_id
    }

    fn closable(&self, _: &App) -> bool {
        false
    }

    fn zoomable(&self, _: &App) -> bool {
        false
    }
}

impl DockPanel for WorkspacePanel {
    fn tab_name(&self, _: &App) -> Option<SharedString> {
        Some(SharedString::from(self.title.to_owned()))
    }

    fn title(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        self.title
    }

    fn title_bar(&self, _: &App) -> bool {
        false
    }
}

impl EventEmitter<PanelEvent> for WorkspacePanel {}

impl Focusable for WorkspacePanel {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for WorkspacePanel {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match self.kind {
            PanelKind::Sidebar => render_sidebar(self.snapshot),
            PanelKind::Resources => self.render_resources(cx),
            PanelKind::Details => self.render_details(cx),
            PanelKind::Activity => self.render_activity(cx),
            PanelKind::Agent => render_agent(self.snapshot),
        }
    }
}

fn topbar(snapshot: SimulatedSnapshot) -> Div {
    div()
        .flex()
        .items_center()
        .gap_3()
        .px_3()
        .py_2()
        .bg(rgb(PANEL))
        .child(
            div()
                    .w(px(28.0))
                    .h(px(28.0))
                .flex()
                .items_center()
                .justify_center()
                .rounded_md()
                .bg(rgb(ACCENT))
                .child(div().text_sm().text_color(rgb(TEXT_1)).child("A")),
        )
        .child(div().text_sm().text_color(rgb(TEXT_1)).child("AxiomLens"))
        .child(div().text_xs().text_color(rgb(TEXT_5)).child("/"))
        .child(div().text_sm().text_color(rgb(TEXT_2)).child(snapshot.target))
        .child(div().text_xs().text_color(rgb(TEXT_5)).child("/"))
        .child(div().text_sm().text_color(rgb(TEXT_4)).child(snapshot.context))
        .child(div().text_xs().text_color(rgb(TEXT_5)).child("/"))
        .child(div().text_sm().text_color(rgb(TEXT_4)).child(snapshot.namespace))
        .child(div().flex_1())
        .child(
            div()
                    .w(px(300.0))
                .h(px(30.0))
                .flex()
                .items_center()
                .gap_2()
                .px_3()
                .rounded_md()
                .border_1()
                .border_color(rgb(BORDER))
                .bg(rgb(RAISED))
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(TEXT_5))
                        .child("Search resources, clusters or commands"),
                )
                .child(div().text_xs().font_family("Menlo").text_color(rgb(TEXT_4)).child("CMD+K")),
        )
        .child(pill("OFFLINE / SIMULATED", TEXT_3, ACCENT_SOFT))
}

fn pill(label: &'static str, foreground: u32, background: u32) -> Div {
    div()
        .px_2()
        .py_1()
        .rounded_lg()
        .bg(rgb(background))
        .child(div().text_xs().text_color(rgb(foreground)).child(label))
}

#[derive(Clone, Copy)]
struct NavItem {
    label: &'static str,
    count: &'static str,
    active: bool,
}

fn nav_item(item: NavItem) -> Div {
    div()
        .flex()
        .items_center()
        .gap_2()
        .px_2()
        .py_1()
        .rounded_md()
        .bg(rgb(if item.active { ACCENT_SOFT } else { PANEL }))
        .child(
            div()
                .flex_1()
                .text_sm()
                .text_color(rgb(if item.active { TEXT_1 } else { TEXT_4 }))
                .child(item.label),
        )
        .child(
            div()
                .text_xs()
                .font_family("Menlo")
                .text_color(rgb(TEXT_5))
                .child(item.count),
        )
}

fn nav_group(title: &'static str, items: &[NavItem]) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_1()
        .child(
            div()
                .px_2()
                .py_1()
                .text_xs()
                .text_color(rgb(TEXT_4))
                .child(title),
        )
        .children(items.iter().copied().map(nav_item))
}

fn render_sidebar(snapshot: SimulatedSnapshot) -> Div {
    let workloads = [
        NavItem {
            label: "Deployments",
            count: "5",
            active: true,
        },
        NavItem {
            label: "StatefulSets",
            count: "1",
            active: false,
        },
        NavItem {
            label: "DaemonSets",
            count: "0",
            active: false,
        },
        NavItem {
            label: "Pods",
            count: "6",
            active: false,
        },
    ];
    let network = [
        NavItem {
            label: "Services",
            count: "2",
            active: false,
        },
        NavItem {
            label: "Ingresses",
            count: "0",
            active: false,
        },
    ];
    let config = [
        NavItem {
            label: "ConfigMaps",
            count: "2",
            active: false,
        },
        NavItem {
            label: "Secrets",
            count: "1",
            active: false,
        },
    ];
    let cluster = [
        NavItem {
            label: "Nodes",
            count: "1",
            active: false,
        },
        NavItem {
            label: "Events",
            count: "3",
            active: false,
        },
    ];

    let sidebar = div()
        .size_full()
        .flex()
        .flex_col()
        .gap_3()
        .p_3()
        .bg(rgb(PANEL))
        .child(
            div()
                .flex()
                .flex_col()
                .gap_2()
                .p_3()
                .rounded_lg()
                .border_1()
                .border_color(rgb(BORDER))
                .bg(rgb(ELEVATED))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            div()
                                .w(px(8.0))
                                .h(px(8.0))
                                .rounded_lg()
                                .bg(rgb(DANGER)),
                        )
                        .child(
                            div()
                                .flex_1()
                                .text_sm()
                                .text_color(rgb(TEXT_1))
                                .child(snapshot.target),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(rgb(DANGER))
                                .child(snapshot.connection_status),
                        ),
                )
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(TEXT_5))
                        .child("SIMULATED / OFFLINE"),
                ),
        )
        .child(
            div()
                .flex()
                .items_center()
                .gap_2()
                .px_2()
                .py_2()
                .rounded_md()
                .border_1()
                .border_color(rgb(BORDER))
                .bg(rgb(RAISED))
                .child(
                    div()
                        .flex_1()
                        .text_sm()
                        .text_color(rgb(TEXT_2))
                        .child(snapshot.namespace),
                )
                .child(div().text_xs().font_family("Menlo").text_color(rgb(TEXT_5)).child("42")),
        )
        .child(nav_group("WORKLOADS", &workloads))
        .child(nav_group("NETWORK", &network))
        .child(nav_group("CONFIG", &config))
        .child(nav_group("CLUSTER", &cluster))
        .overflow_y_scrollbar();

    div().size_full().bg(rgb(PANEL)).child(sidebar)
}

const RESOURCE_COUNT: usize = 5_000;

#[derive(Clone)]
struct ResourceEntry {
    name: SharedString,
    status: &'static str,
    replicas: SharedString,
    age: SharedString,
}

fn resource_entries() -> Rc<Vec<ResourceEntry>> {
    Rc::new(
        (0..RESOURCE_COUNT)
            .map(|index| {
                let (name, status, replicas, age) = match index {
                    0 => ("payment-api".to_owned(), "READY", "6 / 6".to_owned(), "164d".to_owned()),
                    1 => ("checkout-svc".to_owned(), "READY", "4 / 4".to_owned(), "42d".to_owned()),
                    2 => ("catalog-api".to_owned(), "DEGRADED", "2 / 3".to_owned(), "87d".to_owned()),
                    3 => ("cart-worker".to_owned(), "READY", "3 / 3".to_owned(), "12d".to_owned()),
                    4 => ("recommend-engine".to_owned(), "ERROR", "0 / 2".to_owned(), "5d".to_owned()),
                    _ => {
                        let status = match index % 19 {
                            0 => "ERROR",
                            1..=3 => "DEGRADED",
                            _ => "READY",
                        };
                        let replicas = if status == "ERROR" { "0 / 2" } else if status == "DEGRADED" { "2 / 3" } else { "3 / 3" };
                        (
                            format!("sample-deployment-{index:04}"),
                            status,
                            replicas.to_owned(),
                            format!("{}d", index % 180 + 1),
                        )
                    }
                };

                ResourceEntry {
                    name: SharedString::from(name),
                    status,
                    replicas: SharedString::from(replicas),
                    age: SharedString::from(age),
                }
            })
            .collect(),
    )
}

fn fixed_cell(text: impl Into<SharedString>, width: f32, color: u32, mono: bool) -> Div {
    let text = text.into();
    let content = if mono {
        div()
            .text_xs()
            .font_family("Menlo")
            .text_color(rgb(color))
            .child(text)
    } else {
        div().text_xs().text_color(rgb(color)).child(text)
    };
    div()
        .flex_none()
        .w(px(width))
        .h(px(16.0))
        .child(content)
}

fn render_resource_header() -> Div {
    div()
        .flex()
        .items_center()
        .gap_2()
        .px_3()
        .py_2()
        .bg(rgb(RAISED))
        .child(div().flex_1().text_xs().text_color(rgb(TEXT_5)).child("NAME"))
        .child(fixed_cell("STATUS", 76.0, TEXT_5, false))
        .child(fixed_cell("REPL.", 54.0, TEXT_5, false))
        .child(fixed_cell("AGE", 42.0, TEXT_5, false))
}

fn info_row(label: &'static str, value: &'static str) -> Div {
    div()
        .flex()
        .items_center()
        .justify_between()
        .py_1()
        .child(div().text_xs().text_color(rgb(TEXT_5)).child(label))
        .child(
            div()
                .text_xs()
                .font_family("Menlo")
                .text_color(rgb(TEXT_2))
                .child(value),
        )
}

fn detail_tab(label: &'static str, active: bool) -> Div {
    div()
        .px_2()
        .py_2()
        .rounded_md()
        .bg(rgb(if active { RAISED } else { BG }))
        .child(
            div()
                .text_xs()
                .text_color(rgb(if active { TEXT_1 } else { TEXT_4 }))
                .child(label),
        )
}

fn render_detail_overview(snapshot: SimulatedSnapshot) -> Div {
    let detail_body = div()
        .flex_1()
        .flex()
        .flex_col()
        .gap_3()
        .p_3()
        .bg(rgb(BG))
        .child(div().text_xs().text_color(rgb(TEXT_4)).child("BASIC INFORMATION"))
        .child(info_row("Namespace", snapshot.namespace))
        .child(info_row("Replicas", "6 / 6"))
        .child(info_row("Update strategy", "RollingUpdate"))
        .child(info_row("Image", "nginx:1.25-alpine"))
        .child(info_row("UID", "sample-uid-001"))
        .child(div().text_xs().text_color(rgb(TEXT_4)).child("STATUS CONDITIONS"))
        .child(info_row("Available", "Normal"))
        .child(info_row("Progressing", "Normal"))
        .child(
            div()
                .flex()
                .flex_col()
                .gap_2()
                .p_3()
                .rounded_lg()
                .border_1()
                .border_color(rgb(BORDER))
                .bg(rgb(PANEL))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .justify_between()
                        .child(div().text_sm().text_color(rgb(TEXT_1)).child("Container"))
                        .child(div().text_xs().text_color(rgb(OK)).child("RUNNING")),
                )
                .child(info_row("Image", "nginx:1.25-alpine"))
                .child(info_row("Ports", "8080/TCP / 8443/TCP"))
                .child(info_row("Resources", "cpu 100m / mem 128Mi"))
                .child(info_row("Restarts", "0")),
        );

    detail_body
}

fn diff_line(prefix: &'static str, text: &'static str, color: u32, background: u32) -> Div {
    div()
        .flex()
        .items_center()
        .gap_2()
        .px_2()
        .py_1()
        .rounded_md()
        .bg(rgb(background))
        .font_family("Menlo")
        .text_xs()
        .child(div().text_color(rgb(color)).child(prefix))
        .child(div().text_color(rgb(color)).child(text))
}

fn render_simulated_diff() -> Div {
    div()
        .flex_1()
        .flex()
        .flex_col()
        .gap_2()
        .p_3()
        .bg(rgb(BG))
        .child(div().text_xs().text_color(rgb(TEXT_5)).child("SIMULATED DIFF / STATIC FIXTURE"))
        .child(diff_line(" ", "spec:", TEXT_5, BG))
        .child(diff_line("-", "replicas: 6", DANGER, 0x251617))
        .child(diff_line("+", "replicas: 8", OK, 0x14261c))
        .child(diff_line(" ", "readinessProbe:", TEXT_5, BG))
        .child(diff_line("-", "initialDelaySeconds: 0", DANGER, 0x251617))
        .child(diff_line("+", "initialDelaySeconds: 10", OK, 0x14261c))
        .child(
            div()
                .pt_2()
                .text_xs()
                .text_color(rgb(TEXT_5))
                .child("Illustrative lines only; not computed from a resource."),
        )
}

#[derive(Clone, Copy)]
struct LogEntry {
    time: &'static str,
    level: &'static str,
    message: &'static str,
}

const LOG_ENTRIES: [LogEntry; 8] = [
    LogEntry {
        time: "14:22:01",
        level: "INFO",
        message: "sample server listening on :8080",
    },
    LogEntry {
        time: "14:22:02",
        level: "INFO",
        message: "sample database pool established",
    },
    LogEntry {
        time: "14:22:05",
        level: "WARN",
        message: "sample upstream latency 312ms",
    },
    LogEntry {
        time: "14:22:18",
        level: "ERROR",
        message: "sample GET /v1/charge -> 503",
    },
    LogEntry {
        time: "14:22:19",
        level: "WARN",
        message: "sample retry 1/3 for /v1/charge",
    },
    LogEntry {
        time: "14:22:20",
        level: "ERROR",
        message: "sample GET /v1/charge -> 503",
    },
    LogEntry {
        time: "14:22:21",
        level: "INFO",
        message: "sample circuit breaker opened",
    },
    LogEntry {
        time: "14:22:24",
        level: "WARN",
        message: "sample EndpointSlice has 0 ready endpoints",
    },
];

fn log_row(entry: LogEntry) -> Div {
    log_line(entry.time, entry.level, entry.message)
}

fn log_line(
    time: impl Into<SharedString>,
    level: impl Into<SharedString>,
    message: impl Into<SharedString>,
) -> Div {
    let time = time.into();
    let level = level.into();
    let message = message.into();
    let color = match level.as_ref() {
        "ERROR" => DANGER,
        "WARN" => WARN,
        _ => TEXT_4,
    };
    div()
        .flex()
        .flex_none()
        .items_center()
        .gap_2()
        .h(px(18.0))
        .font_family("Menlo")
        .text_xs()
        .child(div().text_color(rgb(TEXT_5)).child(time))
        .child(div().text_color(rgb(color)).child(level))
        .child(div().flex_1().truncate().text_color(rgb(TEXT_3)).child(message))
}

fn render_simulated_task_message(message: &SimulatedTaskMessage) -> Div {
    match message {
        SimulatedTaskMessage::Output {
            sequence,
            level,
            message,
        } => log_line(format!("T+{sequence:02}"), *level, message.clone()),
        SimulatedTaskMessage::Finished { dropped } => {
            log_line("TASK", "INFO", format!("mock completed; {dropped} newest messages dropped"))
        }
        SimulatedTaskMessage::Failed { dropped } => {
            log_line("TASK", "ERROR", format!("mock failed with 503; {dropped} newest messages dropped"))
        }
        SimulatedTaskMessage::Cancelled { dropped } => {
            log_line("TASK", "WARN", format!("mock cancelled; {dropped} newest messages dropped"))
        }
    }
}

fn task_terminal_status(message: &SimulatedTaskMessage) -> Option<SharedString> {
    match message {
        SimulatedTaskMessage::Finished { dropped } => Some(SharedString::from(format!(
            "COMPLETED / {dropped} messages dropped by bounded queue"
        ))),
        SimulatedTaskMessage::Failed { dropped } => Some(SharedString::from(format!(
            "FAILED / simulated upstream 503 / {dropped} messages dropped"
        ))),
        SimulatedTaskMessage::Cancelled { dropped } => Some(SharedString::from(format!(
            "CANCELLED / not reported as success / {dropped} messages dropped"
        ))),
        SimulatedTaskMessage::Output { .. } => None,
    }
}

fn context_chip(label: &'static str) -> Div {
    div()
        .px_2()
        .py_1()
        .rounded_md()
        .border_1()
        .border_color(rgb(BORDER))
        .bg(rgb(ELEVATED))
        .child(div().text_xs().text_color(rgb(TEXT_2)).child(label))
}

fn render_agent(snapshot: SimulatedSnapshot) -> Div {
    div()
        .size_full()
        .flex()
        .flex_col()
        .bg(rgb(PANEL))
        .child(
            div()
                .flex()
                .items_center()
                .gap_2()
                .px_3()
                .py_3()
                .bg(rgb(RAISED))
                .child(
                    div()
                        .w(px(22.0))
                        .h(px(22.0))
                        .flex()
                        .items_center()
                        .justify_center()
                        .rounded_md()
                        .bg(rgb(ACCENT_SOFT))
                        .child(div().text_xs().text_color(rgb(INFO)).child("AI")),
                )
                .child(div().text_sm().text_color(rgb(TEXT_1)).child("AI Agent"))
                .child(div().flex_1())
                .child(pill("SIMULATED", 0x7fb4e6, ACCENT_SOFT)),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .gap_2()
                .p_3()
                .bg(rgb(BG))
                .child(div().text_xs().text_color(rgb(TEXT_5)).child("CONTEXT / 4 ITEMS"))
                .child(context_chip("payment-api / Deployment"))
                .child(context_chip(snapshot.namespace))
                .child(context_chip("2 sample Pods"))
                .child(context_chip("3 sample Events")),
        )
        .child(
            div()
                .flex_1()
                .flex()
                .flex_col()
                .gap_3()
                .p_3()
                .bg(rgb(BG))
                .child(div().text_xs().text_color(rgb(TEXT_4)).child("DIAGNOSTIC PREVIEW"))
                .child(
                    div()
                        .flex()
                        .justify_end()
                        .child(
                            div()
                                .max_w(px(284.0))
                                .p_3()
                                .rounded_lg()
                                .bg(rgb(ACCENT_SOFT))
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(rgb(TEXT_1))
                                        .child("Why does the sample payment-api show 503?"),
                                ),
                        ),
                )
                .child(div().text_xs().text_color(rgb(TEXT_3)).child("SAMPLE RESPONSE / NOT GENERATED"))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .p_3()
                        .rounded_lg()
                        .border_1()
                        .border_color(rgb(BORDER))
                        .bg(rgb(PANEL))
                        .child(div().text_sm().text_color(rgb(TEXT_1)).child("Sample observations"))
                        .child(div().text_xs().text_color(rgb(TEXT_2)).child("H1  EndpointSlice has no ready backend"))
                        .child(div().text_xs().text_color(rgb(TEXT_2)).child("H2  Example upstream latency is elevated"))
                        .child(div().text_xs().text_color(rgb(WARN)).child("H3  Readiness threshold needs validation")),
                    )
                    .overflow_y_scrollbar(),
                )
        .child(
            div()
                .flex()
                .items_center()
                .gap_2()
                .p_3()
                .bg(rgb(RAISED))
                .child(
                    div()
                        .flex_1()
                        .px_3()
                        .py_2()
                        .rounded_md()
                        .border_1()
                        .border_color(rgb(BORDER))
                        .bg(rgb(BG))
                        .child(
                            div()
                                .text_xs()
                                .text_color(rgb(TEXT_5))
                                .child("Agent input disabled in Stage 1"),
                        ),
                )
                .child(pill("DISABLED", TEXT_5, ELEVATED)),
        )
}

pub fn build_workbench(
    snapshot: SimulatedSnapshot,
    window: &mut Window,
    cx: &mut App,
) -> Entity<DockArea> {
    let (dock_area, _skin) = DockSkin::dock_area("axiomlens-workspace", Some(1), window, cx);

    let sidebar =
        WorkspacePanel::new("SidebarPanel", "Sidebar", PanelKind::Sidebar, snapshot, window, cx);
    let resources = WorkspacePanel::new(
        "ResourceListPanel",
        "Resources",
        PanelKind::Resources,
        snapshot,
        window,
        cx,
    );
    let details = WorkspacePanel::new(
        "ResourceDetailsPanel",
        "Resource details",
        PanelKind::Details,
        snapshot,
        window,
        cx,
    );
    let agent = WorkspacePanel::new("AgentPanel", "Agent", PanelKind::Agent, snapshot, window, cx);
    let activity =
        WorkspacePanel::new("ActivityPanel", "Activity", PanelKind::Activity, snapshot, window, cx);

    dock_area.update(cx, |area, cx| {
        area.set_center(
            DockLayout::h_split()
                .child(
                    DockLayout::tabs().panel_view(panel_handle(resources), cx),
                    None,
                )
                .child(
                    DockLayout::tabs().panel_view(panel_handle(details), cx),
                    Some(px(340.0)),
                ),
            window,
            cx,
        );
        area.set_dock(
            DockPlacement::Left,
            DockLayout::tabs().panel_view(panel_handle(sidebar), cx),
            window,
            cx,
        );
        area.set_dock(
            DockPlacement::Right,
            DockLayout::tabs().panel_view(panel_handle(agent), cx),
            window,
            cx,
        );
        area.set_dock(
            DockPlacement::Bottom,
            DockLayout::tabs().panel_view(panel_handle(activity), cx),
            window,
            cx,
        );
        area.set_dock_size(DockPlacement::Left, px(248.0), window, cx);
        area.set_dock_size(DockPlacement::Right, px(340.0), window, cx);
        area.set_dock_size(DockPlacement::Bottom, px(220.0), window, cx);
    });

    dock_area
}