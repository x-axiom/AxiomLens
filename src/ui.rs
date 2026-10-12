use gpui_kit::{
    App, AppContext as _, Context, Div, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement as _, IntoElement, MouseButton, ParentElement as _, Render, SharedString,
    Styled as _, Subscription, Task, Window, div, px, rgb, size,
};
use gpui_kit::component::button::{Button, ButtonVariants as _};
use gpui_kit::component::dock::{
    BasePanel, DockArea, DockLayout, DockPlacement, DockSkin, Panel as DockPanel, PanelEvent,
    panel_handle,
};
use gpui_kit::component::input::{Editor, EditorState, Input, InputState};
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
pub(crate) struct Workbench {
    dock_area: Entity<DockArea>,
    navigation: Entity<ResourceNavigation>,
    search_input: Entity<InputState>,
}

pub struct AxiomLensPrototype {
    dock_area: Entity<DockArea>,
    navigation: Entity<ResourceNavigation>,
    search_input: Entity<InputState>,
    _navigation_subscription: Subscription,
    _search_subscription: Subscription,
}

impl AxiomLensPrototype {
    pub(crate) fn new(workbench: Workbench, cx: &mut Context<Self>) -> Self {
        let navigation = workbench.navigation;
        let search_input = workbench.search_input;
        let search_navigation = navigation.clone();
        let navigation_subscription = cx.observe(&navigation, |_, _, cx| cx.notify());
        let search_subscription = cx.observe(&search_input, move |_, input, cx| {
            let query = input.read(cx).value().to_string();
            search_navigation.update(cx, |navigation, cx| {
                navigation.set_search_query(query);
                cx.notify();
            });
        });

        Self {
            dock_area: workbench.dock_area,
            navigation,
            search_input,
            _navigation_subscription: navigation_subscription,
            _search_subscription: search_subscription,
        }
    }
}

impl Render for AxiomLensPrototype {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let snapshot = self.navigation.read(cx).active_snapshot();
        let context_control = Button::new("switch-simulated-context")
            .label(format!("{} v", snapshot.context))
            .compact()
            .ghost()
            .on_click(cx.listener(|app, _, _, cx| {
                app.navigation.update(cx, |navigation, cx| {
                    navigation.switch_context();
                    cx.notify();
                });
            }));

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(BG))
            .child(topbar(snapshot, &self.search_input, context_control))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResourceTab {
    context_index: usize,
    resource_index: usize,
}

struct ResourceScope {
    snapshot: SimulatedSnapshot,
    resources: Rc<Vec<ResourceEntry>>,
}

struct ResourceNavigation {
    scopes: Vec<ResourceScope>,
    active_context: usize,
    tabs: Vec<ResourceTab>,
    active_tab: Option<ResourceTab>,
    search_query: String,
    filtered_resources: Vec<usize>,
}

impl ResourceNavigation {
    fn new(snapshot: SimulatedSnapshot) -> Self {
        let staging_snapshot = SimulatedSnapshot {
            target: "local-staging-demo",
            connection_status: "Disconnected",
            context: "staging-context",
            namespace: "checkout-staging",
            resource_summary: "5,000 sample resources",
        };
        let scopes = vec![
            ResourceScope {
                snapshot,
                resources: resource_entries(0),
            },
            ResourceScope {
                snapshot: staging_snapshot,
                resources: resource_entries(1),
            },
        ];
        let initial_tab = ResourceTab {
            context_index: 0,
            resource_index: 0,
        };
        let mut navigation = Self {
            scopes,
            active_context: 0,
            tabs: vec![initial_tab],
            active_tab: Some(initial_tab),
            search_query: String::new(),
            filtered_resources: Vec::new(),
        };
        navigation.refresh_filter();
        navigation
    }

    fn active_snapshot(&self) -> SimulatedSnapshot {
        self.scopes[self.active_context].snapshot
    }

    fn active_resource(&self) -> Option<&ResourceEntry> {
        self.active_tab.and_then(|tab| self.resource_for_tab(tab))
    }

    fn resource_for_tab(&self, tab: ResourceTab) -> Option<&ResourceEntry> {
        self.scopes
            .get(tab.context_index)?
            .resources
            .get(tab.resource_index)
    }

    fn open_resource(&mut self, resource_index: usize) {
        let tab = ResourceTab {
            context_index: self.active_context,
            resource_index,
        };
        if self.resource_for_tab(tab).is_none() {
            return;
        }
        if !self.tabs.contains(&tab) {
            self.tabs.push(tab);
        }
        self.active_tab = Some(tab);
    }

    fn select_tab(&mut self, tab: ResourceTab) {
        if !self.tabs.contains(&tab) {
            return;
        }
        self.active_context = tab.context_index;
        self.active_tab = Some(tab);
        self.refresh_filter();
    }

    fn close_tab(&mut self, tab: ResourceTab) {
        let Some(index) = self.tabs.iter().position(|candidate| *candidate == tab) else {
            return;
        };
        self.tabs.remove(index);
        if self.active_tab == Some(tab) {
            self.active_tab = self.tabs.get(index.min(self.tabs.len().saturating_sub(1))).copied();
            if let Some(active_tab) = self.active_tab {
                self.active_context = active_tab.context_index;
            }
        }
        self.refresh_filter();
    }

    fn switch_context(&mut self) {
        self.switch_context_to((self.active_context + 1) % self.scopes.len());
    }

    fn switch_context_to(&mut self, context_index: usize) {
        if context_index >= self.scopes.len() {
            return;
        }
        self.active_context = context_index;
        let tab = self
            .tabs
            .iter()
            .find(|tab| tab.context_index == context_index)
            .copied();
        let tab = match tab {
            Some(tab) => tab,
            None => {
                let tab = ResourceTab {
                    context_index,
                    resource_index: 0,
                };
                self.tabs.push(tab);
                tab
            }
        };
        self.active_tab = Some(tab);
        self.refresh_filter();
    }

    fn set_search_query(&mut self, query: String) {
        self.search_query = query;
        self.refresh_filter();
    }

    fn refresh_filter(&mut self) {
        let query = self.search_query.trim().to_lowercase();
        self.filtered_resources = self.scopes[self.active_context]
            .resources
            .iter()
            .enumerate()
            .filter_map(|(index, resource)| {
                let matches = query.is_empty()
                    || resource.name.to_string().to_lowercase().contains(&query)
                    || resource.kind.to_lowercase().contains(&query);
                matches.then_some(index)
            })
            .collect();
    }
}

struct WorkspacePanel {
    panel_id: &'static str,
    title: &'static str,
    kind: PanelKind,
    navigation: Entity<ResourceNavigation>,
    focus_handle: FocusHandle,
    detail_view: DetailView,
    yaml_editor: Option<Entity<EditorState>>,
    yaml_tab: Option<ResourceTab>,
    _navigation_subscription: Option<Subscription>,
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
        navigation: Entity<ResourceNavigation>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let yaml_editor = matches!(kind, PanelKind::Details).then(|| {
            cx.new(|cx| EditorState::new(window, cx).default_value("").language("yaml"))
        });
        let observed_navigation = navigation.clone();
        let panel = cx.new(|cx| Self {
            panel_id,
            title,
            kind,
            navigation,
            focus_handle: cx.focus_handle(),
            detail_view: DetailView::Overview,
            yaml_editor,
            yaml_tab: None,
            _navigation_subscription: None,
            activity_events: VecDeque::new(),
            activity_status: SharedString::from("IDLE / LOCAL FAKE TASK"),
            activity_running: false,
            cancellation: None,
            run_id: 0,
            worker_task: None,
            consumer_task: None,
        });
        panel.update(cx, |panel, cx| {
            panel._navigation_subscription = Some(cx.observe(&observed_navigation, |_, _, cx| {
                cx.notify();
            }));
        });
        panel
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
        let (
            context_index,
            total_count,
            filtered_resources,
            query,
            selected_name,
            context_name,
            resource_summary,
        ) = {
            let navigation = self.navigation.read(cx);
            let scope = &navigation.scopes[navigation.active_context];
            (
                navigation.active_context,
                scope.resources.len(),
                navigation.filtered_resources.clone(),
                navigation.search_query.clone(),
                navigation
                    .active_resource()
                    .map(|resource| resource.name.clone()),
                scope.snapshot.context,
                scope.snapshot.resource_summary,
            )
        };
        let filtered_count = filtered_resources.len();
        let list = if filtered_resources.is_empty() {
            div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .bg(rgb(BG))
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(TEXT_4))
                        .child(format!("No simulated resources match \"{query}\".")),
                )
        } else {
            let row_indices = Rc::new(filtered_resources);
            let item_sizes = Rc::new(vec![size(px(1.0), px(38.0)); row_indices.len()]);
            let rows = v_virtual_list(
                cx.entity(),
                "simulated-resources",
                item_sizes,
                move |panel, visible_range, _, cx| {
                    visible_range
                        .map(|index| panel.render_resource_row(context_index, row_indices[index], cx))
                        .collect::<Vec<_>>()
                },
            );
            div().flex_1().child(rows)
        };

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(BG))
            .child(self.render_resource_tabs(cx))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .px_3()
                    .py_2()
                    .bg(rgb(RAISED))
                    .child(div().text_sm().text_color(rgb(TEXT_1)).child("Resources"))
                    .child(pill(
                        format!("{filtered_count} / {total_count}"),
                        0x7fb4e6,
                        ACCENT_SOFT_2,
                    ))
                    .child(div().flex_1())
                    .child(div().text_xs().text_color(rgb(TEXT_5)).child("SIMULATED")),
            )
            .child(render_resource_header())
            .child(list)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .px_3()
                    .py_2()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child(format!("SIMULATED / {context_name} / {resource_summary}")),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child(selected_name.map_or_else(
                                || SharedString::from("No resource open"),
                                |name| SharedString::from(format!("Selected: {name}")),
                            )),
                    ),
            )
    }

    fn render_resource_tabs(&self, cx: &mut Context<Self>) -> Div {
        let tabs = {
            let navigation = self.navigation.read(cx);
            navigation
                .tabs
                .iter()
                .filter_map(|tab| {
                    let resource = navigation.resource_for_tab(*tab)?;
                    let snapshot = navigation.scopes[tab.context_index].snapshot;
                    Some((
                        *tab,
                        format!("{} / {} / {}", resource.name, snapshot.target, snapshot.context),
                        navigation.active_tab == Some(*tab),
                    ))
                })
                .collect::<Vec<_>>()
        };

        let tab_elements = tabs.into_iter().map(|(tab, label, active)| {
            div()
                .flex_none()
                .flex()
                .items_center()
                .gap_1()
                .h(px(34.0))
                .px_2()
                .bg(rgb(if active { BG } else { RAISED }))
                .border_1()
                .border_color(rgb(if active { BORDER } else { RAISED }))
                .child(
                    div()
                        .h(px(32.0))
                        .flex()
                        .items_center()
                        .text_xs()
                        .text_color(rgb(if active { TEXT_1 } else { TEXT_4 }))
                        .child(label)
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |panel, _, _, cx| {
                                panel.navigation.update(cx, |navigation, cx| {
                                    navigation.select_tab(tab);
                                    cx.notify();
                                });
                            }),
                        ),
                )
                .child(
                    Button::new(format!(
                        "close-resource-tab-{}-{}",
                        tab.context_index, tab.resource_index
                    ))
                    .label("x")
                    .compact()
                    .ghost()
                    .on_click(cx.listener(move |panel, _, _, cx| {
                        panel.navigation.update(cx, |navigation, cx| {
                            navigation.close_tab(tab);
                            cx.notify();
                        });
                    })),
                )
        });

        div().w_full().child(
            div()
                .flex()
                .items_center()
                .gap_1()
                .h(px(36.0))
                .px_2()
                .bg(rgb(RAISED))
                .children(tab_elements)
                .overflow_x_scrollbar(),
        )
    }

    fn render_resource_row(
        &self,
        context_index: usize,
        resource_index: usize,
        cx: &mut Context<Self>,
    ) -> Div {
        let tab = ResourceTab {
            context_index,
            resource_index,
        };
        let (entry, is_selected) = {
            let navigation = self.navigation.read(cx);
            (
                navigation
                    .resource_for_tab(tab)
                    .expect("virtual range is within resource data")
                    .clone(),
                navigation.active_tab == Some(tab),
            )
        };
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
                    .child(entry.name.clone()),
            )
            .child(fixed_cell(entry.kind, 70.0, TEXT_4, false))
            .child(fixed_cell(entry.status, 68.0, status_color, false))
            .child(fixed_cell(entry.replicas, 54.0, TEXT_2, true))
            .child(fixed_cell(entry.age, 42.0, TEXT_4, true))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |panel, _, _, cx| {
                    panel.navigation.update(cx, |navigation, cx| {
                        navigation.open_resource(resource_index);
                        cx.notify();
                    });
                }),
            )
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

    fn render_details(&mut self, window: &mut Window, cx: &mut Context<Self>) -> Div {
        let (snapshot, active_tab, selected_resource) = {
            let navigation = self.navigation.read(cx);
            (
                navigation.active_snapshot(),
                navigation.active_tab,
                navigation.active_resource().cloned(),
            )
        };
        if self.yaml_tab != active_tab {
            let yaml = selected_resource
                .as_ref()
                .map(|resource| resource_yaml(snapshot, resource))
                .unwrap_or_default();
            if let Some(editor) = &self.yaml_editor {
                editor.update(cx, |editor, cx| editor.set_value(&yaml, window, cx));
            }
            self.yaml_tab = active_tab;
        }

        let body = match self.detail_view {
            DetailView::Overview => render_detail_overview(snapshot, selected_resource.as_ref()),
            DetailView::Yaml => match (self.yaml_editor.as_ref(), selected_resource.as_ref()) {
                (Some(state), Some(_)) => {
                    div()
                        .flex_1()
                        .p_2()
                        .bg(rgb(BG))
                        .child(Editor::new(state).readonly(true).h(px(480.0)))
                }
                _ => render_empty_detail("Open a simulated resource to inspect YAML."),
            },
            DetailView::Diff if selected_resource.is_some() => render_simulated_diff(),
            DetailView::Diff => render_empty_detail("Open a simulated resource to inspect Diff."),
        };
        let scope_summary = format!(
            "SIMULATED / {} / {} / {}",
            snapshot.target, snapshot.context, snapshot.namespace
        );
        let resource_header = selected_resource.as_ref().map_or_else(
            || {
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(div().text_sm().text_color(rgb(TEXT_4)).child("No resource open"))
                    .child(div().flex_1())
                    .child(pill("SIMULATED", TEXT_3, ACCENT_SOFT))
            },
            |resource| {
                let status_color = match resource.status {
                    "READY" => OK,
                    "DEGRADED" => WARN,
                    _ => DANGER,
                };
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(pill(resource.kind, 0x7fb4e6, ACCENT_SOFT_2))
                    .child(div().text_sm().text_color(rgb(TEXT_1)).child(resource.name.clone()))
                    .child(div().flex_1())
                    .child(pill(resource.status, status_color, ELEVATED))
            },
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
                    .child(resource_header)
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child(scope_summary),
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
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let snapshot = self.navigation.read(cx).active_snapshot();
        match self.kind {
            PanelKind::Sidebar => render_sidebar(snapshot),
            PanelKind::Resources => self.render_resources(cx),
            PanelKind::Details => self.render_details(window, cx),
            PanelKind::Activity => self.render_activity(cx),
            PanelKind::Agent => render_agent(snapshot),
        }
    }
}

fn topbar(
    snapshot: SimulatedSnapshot,
    search_input: &Entity<InputState>,
    context_control: impl IntoElement,
) -> Div {
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
        .child(context_control)
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
                .child(div().text_xs().text_color(rgb(TEXT_5)).child("NAME / TYPE"))
                .child(
                    Input::new(search_input)
                        .id("resource-search")
                        .aria_label("Search simulated resources by name or type")
                        .cleanable(true)
                        .appearance(false)
                        .bordered(false)
                        .focus_bordered(false)
                        .flex_1(),
                )
        )
        .child(pill("OFFLINE / SIMULATED", TEXT_3, ACCENT_SOFT))
}

fn pill(label: impl Into<SharedString>, foreground: u32, background: u32) -> Div {
    let label = label.into();
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
    kind: &'static str,
    status: &'static str,
    replicas: SharedString,
    age: SharedString,
}

fn resource_entries(context_index: usize) -> Rc<Vec<ResourceEntry>> {
    Rc::new(
        (0..RESOURCE_COUNT)
            .map(|index| {
                let (name, kind, status, replicas, age) = match index {
                    0 if context_index == 0 => (
                        "payment-api".to_owned(),
                        "Deployment",
                        "READY",
                        "6 / 6".to_owned(),
                        "164d".to_owned(),
                    ),
                    0 => (
                        "payment-api".to_owned(),
                        "Deployment",
                        "DEGRADED",
                        "1 / 2".to_owned(),
                        "12d".to_owned(),
                    ),
                    1 => (
                        "checkout-svc".to_owned(),
                        "Service",
                        "READY",
                        "1 / 1".to_owned(),
                        "42d".to_owned(),
                    ),
                    2 => (
                        "catalog-api".to_owned(),
                        "Deployment",
                        "DEGRADED",
                        "2 / 3".to_owned(),
                        "87d".to_owned(),
                    ),
                    3 => (
                        "cart-worker".to_owned(),
                        "StatefulSet",
                        "READY",
                        "3 / 3".to_owned(),
                        "12d".to_owned(),
                    ),
                    4 => (
                        "recommend-engine".to_owned(),
                        "CronJob",
                        "ERROR",
                        "0 active".to_owned(),
                        "5d".to_owned(),
                    ),
                    _ => {
                        let status = match index % 19 {
                            0 => "ERROR",
                            1..=3 => "DEGRADED",
                            _ => "READY",
                        };
                        let kind = match index % 4 {
                            0 => "Deployment",
                            1 => "StatefulSet",
                            2 => "DaemonSet",
                            _ => "CronJob",
                        };
                        let replicas = if status == "ERROR" {
                            "0 / 2"
                        } else if status == "DEGRADED" {
                            "2 / 3"
                        } else {
                            "3 / 3"
                        };
                        (
                            format!("sample-deployment-{index:04}"),
                            kind,
                            status,
                            replicas.to_owned(),
                            format!("{}d", index % 180 + 1),
                        )
                    }
                };

                ResourceEntry {
                    name: SharedString::from(name),
                    kind,
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
        .child(fixed_cell("TYPE", 70.0, TEXT_5, false))
        .child(fixed_cell("STATUS", 68.0, TEXT_5, false))
        .child(fixed_cell("REPL.", 54.0, TEXT_5, false))
        .child(fixed_cell("AGE", 42.0, TEXT_5, false))
}

fn info_row(label: &'static str, value: impl Into<SharedString>) -> Div {
    let value = value.into();
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

fn render_empty_detail(message: impl Into<SharedString>) -> Div {
    div()
        .flex_1()
        .flex()
        .items_center()
        .justify_center()
        .bg(rgb(BG))
        .child(div().text_sm().text_color(rgb(TEXT_4)).child(message.into()))
}

fn render_detail_overview(
    snapshot: SimulatedSnapshot,
    resource: Option<&ResourceEntry>,
) -> Div {
    let Some(resource) = resource else {
        return render_empty_detail("Open a simulated resource to inspect its details.");
    };

    div()
        .flex_1()
        .flex()
        .flex_col()
        .gap_3()
        .p_3()
        .bg(rgb(BG))
        .child(div().text_xs().text_color(rgb(TEXT_4)).child("BASIC INFORMATION"))
        .child(info_row("Target", snapshot.target))
        .child(info_row("Context", snapshot.context))
        .child(info_row("Namespace", snapshot.namespace))
        .child(info_row("Resource", resource.name.clone()))
        .child(info_row("Type", resource.kind))
        .child(info_row("Status", resource.status))
        .child(info_row("Ready", resource.replicas.clone()))
        .child(info_row("Age", resource.age.clone()))
        .child(div().text_xs().text_color(rgb(TEXT_4)).child("DATA SOURCE"))
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
                .child(info_row("Source", "Local simulated fixture"))
                .child(info_row("Cluster access", "None"))
                .child(info_row("Writes", "Disabled")),
        )
}

fn resource_yaml(snapshot: SimulatedSnapshot, resource: &ResourceEntry) -> String {
    let api_version = match resource.kind {
        "Service" => "v1",
        "CronJob" => "batch/v1",
        _ => "apps/v1",
    };
    format!(
        "apiVersion: {api_version}\nkind: {}\nmetadata:\n  name: {}\n  namespace: {}\n  labels:\n    axiomlens.dev/data-source: simulated\nstatus:\n  state: {}\n  ready: \"{}\"\n  target: {}\n  context: {}\n",
        resource.kind,
        resource.name,
        snapshot.namespace,
        resource.status,
        resource.replicas,
        snapshot.target,
        snapshot.context,
    )
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

pub(crate) fn build_workbench(
    snapshot: SimulatedSnapshot,
    window: &mut Window,
    cx: &mut App,
) -> Workbench {
    let (dock_area, _skin) = DockSkin::dock_area("axiomlens-workspace", Some(1), window, cx);
    let navigation = cx.new(|_| ResourceNavigation::new(snapshot));
    let search_input = cx.new(|cx| InputState::new(window, cx));

    let sidebar = WorkspacePanel::new(
        "SidebarPanel",
        "Sidebar",
        PanelKind::Sidebar,
        navigation.clone(),
        window,
        cx,
    );
    let resources = WorkspacePanel::new(
        "ResourceListPanel",
        "Resources",
        PanelKind::Resources,
        navigation.clone(),
        window,
        cx,
    );
    let details = WorkspacePanel::new(
        "ResourceDetailsPanel",
        "Resource details",
        PanelKind::Details,
        navigation.clone(),
        window,
        cx,
    );
    let agent = WorkspacePanel::new(
        "AgentPanel",
        "Agent",
        PanelKind::Agent,
        navigation.clone(),
        window,
        cx,
    );
    let activity = WorkspacePanel::new(
        "ActivityPanel",
        "Activity",
        PanelKind::Activity,
        navigation.clone(),
        window,
        cx,
    );

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

    Workbench {
        dock_area,
        navigation,
        search_input,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_named_resources_remain_scoped_to_their_context() {
        let mut navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        let local = navigation.active_resource().unwrap().clone();
        let local_snapshot = navigation.active_snapshot();

        navigation.switch_context();
        let staging = navigation.active_resource().unwrap();
        let staging_snapshot = navigation.active_snapshot();

        assert_eq!(local.name.to_string(), staging.name.to_string());
        assert_eq!(local_snapshot.namespace, "payment-platform");
        assert_eq!(staging_snapshot.namespace, "checkout-staging");
        assert_ne!(local.status, staging.status);
        assert_eq!(navigation.active_tab.unwrap().context_index, 1);
    }

    #[test]
    fn opening_a_resource_twice_does_not_duplicate_its_tab() {
        let mut navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        navigation.open_resource(1);
        navigation.open_resource(1);

        assert_eq!(navigation.tabs.len(), 2);
        assert_eq!(navigation.active_tab.unwrap().resource_index, 1);
    }

    #[test]
    fn search_matches_names_and_types_only_in_the_active_context() {
        let mut navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        navigation.set_search_query("PAYMENT-API".to_owned());
        assert_eq!(navigation.filtered_resources.len(), 1);

        navigation.set_search_query("statefulset".to_owned());
        assert!(!navigation.filtered_resources.is_empty());
        assert!(navigation.filtered_resources.iter().all(|index| {
            navigation.scopes[navigation.active_context].resources[*index].kind == "StatefulSet"
        }));

        navigation.switch_context();
        assert!(navigation.filtered_resources.iter().all(|index| {
            navigation.scopes[navigation.active_context].resources[*index].kind == "StatefulSet"
        }));
    }

    #[test]
    fn unmatched_search_has_no_results_and_closing_tabs_clears_selection() {
        let mut navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        navigation.set_search_query("no-such-resource".to_owned());
        assert!(navigation.filtered_resources.is_empty());

        navigation.set_search_query(String::new());
        navigation.open_resource(1);
        let selected = navigation.active_tab.unwrap();
        navigation.close_tab(selected);
        assert_eq!(navigation.active_tab.unwrap().resource_index, 0);
        navigation.close_tab(navigation.active_tab.unwrap());
        assert!(navigation.active_tab.is_none());
    }
}