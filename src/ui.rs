use gpui_kit::{
    App, AppContext as _, Context, Div, Entity, EventEmitter, FocusHandle, Focusable,
    IntoElement, ParentElement as _, Render, SharedString, Styled as _, Window, div, px, rgb,
};
use gpui_kit::component::dock::{
    BasePanel, DockArea, DockLayout, DockPlacement, DockSkin, Panel as DockPanel, PanelEvent,
    panel_handle,
};
use gpui_kit::component::scroll::ScrollableElement as _;

use crate::simulated_data::SimulatedSnapshot;

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

struct WorkspacePanel {
    panel_id: &'static str,
    title: &'static str,
    kind: PanelKind,
    snapshot: SimulatedSnapshot,
    focus_handle: FocusHandle,
}

impl WorkspacePanel {
    fn new(
        panel_id: &'static str,
        title: &'static str,
        kind: PanelKind,
        snapshot: SimulatedSnapshot,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| Self {
            panel_id,
            title,
            kind,
            snapshot,
            focus_handle: cx.focus_handle(),
        })
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
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        match self.kind {
            PanelKind::Sidebar => render_sidebar(self.snapshot),
            PanelKind::Resources => render_resources(self.snapshot),
            PanelKind::Details => render_details(self.snapshot),
            PanelKind::Activity => render_activity(),
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

#[derive(Clone, Copy)]
struct ResourceEntry {
    name: &'static str,
    status: &'static str,
    replicas: &'static str,
    age: &'static str,
}

const RESOURCE_ENTRIES: [ResourceEntry; 5] = [
    ResourceEntry {
        name: "payment-api",
        status: "READY",
        replicas: "6 / 6",
        age: "164d",
    },
    ResourceEntry {
        name: "checkout-svc",
        status: "READY",
        replicas: "4 / 4",
        age: "42d",
    },
    ResourceEntry {
        name: "catalog-api",
        status: "DEGRADED",
        replicas: "2 / 3",
        age: "87d",
    },
    ResourceEntry {
        name: "cart-worker",
        status: "READY",
        replicas: "3 / 3",
        age: "12d",
    },
    ResourceEntry {
        name: "recommend-engine",
        status: "ERROR",
        replicas: "0 / 2",
        age: "5d",
    },
];

fn fixed_cell(text: &'static str, width: f32, color: u32, mono: bool) -> Div {
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

fn render_resource_row(entry: ResourceEntry, active: bool) -> Div {
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
        .py_2()
        .bg(rgb(if active { ACCENT_SOFT } else { BG }))
        .child(
            div()
                .flex_1()
                .truncate()
                .text_sm()
                .text_color(rgb(if active { TEXT_1 } else { TEXT_2 }))
                .child(entry.name),
        )
        .child(fixed_cell(entry.status, 76.0, status_color, false))
        .child(fixed_cell(entry.replicas, 54.0, TEXT_2, true))
        .child(fixed_cell(entry.age, 42.0, TEXT_4, true))
}

fn render_resources(snapshot: SimulatedSnapshot) -> Div {
    let rows = RESOURCE_ENTRIES
        .iter()
        .copied()
        .enumerate()
        .map(|(index, entry)| render_resource_row(entry, index == 0));

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
                        .child(
                            div()
                                    .w(px(12.0))
                                .h(px(12.0))
                                .rounded_md()
                                .border_1()
                                .border_color(rgb(TEXT_4)),
                        )
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
                .child(pill("5", 0x7fb4e6, ACCENT_SOFT_2))
                .child(div().flex_1())
                .child(div().text_xs().text_color(rgb(TEXT_5)).child("SIMULATED")),
        )
        .child(render_resource_header())
        .child(div().flex_1().flex_col().children(rows))
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
                        .child(snapshot.resource_summary),
                )
                .child(div().text_xs().text_color(rgb(TEXT_5)).child("selected: payment-api")),
        )
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

fn render_details(snapshot: SimulatedSnapshot) -> Div {
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
                .child(detail_tab("Info", true))
                .child(detail_tab("YAML", false))
                .child(detail_tab("Events", false))
                .child(detail_tab("Logs", false))
                .child(detail_tab("Rel.", false)),
        )
        .child(detail_body.overflow_y_scrollbar())
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
    let color = match entry.level {
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
        .child(div().text_color(rgb(TEXT_5)).child(entry.time))
        .child(div().text_color(rgb(color)).child(entry.level))
        .child(div().flex_1().truncate().text_color(rgb(TEXT_3)).child(entry.message))
}

fn render_activity() -> Div {
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
                .flex_1()
                .flex()
                .flex_col()
                .gap_1()
                .px_3()
                .py_2()
                .bg(rgb(BG))
                .children(LOG_ENTRIES.iter().copied().map(log_row))
                .overflow_y_scrollbar(),
        )
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

    let sidebar = WorkspacePanel::new("SidebarPanel", "Sidebar", PanelKind::Sidebar, snapshot, cx);
    let resources = WorkspacePanel::new(
        "ResourceListPanel",
        "Resources",
        PanelKind::Resources,
        snapshot,
        cx,
    );
    let details = WorkspacePanel::new(
        "ResourceDetailsPanel",
        "Resource details",
        PanelKind::Details,
        snapshot,
        cx,
    );
    let agent = WorkspacePanel::new("AgentPanel", "Agent", PanelKind::Agent, snapshot, cx);
    let activity = WorkspacePanel::new("ActivityPanel", "Activity", PanelKind::Activity, snapshot, cx);

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