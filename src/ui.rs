use gpui_kit::{
    App, AppContext as _, Context, Div, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement as _, IntoElement, MouseButton, ParentElement as _, Render, SharedString,
    KeyDownEvent, Modifiers, Styled as _, Subscription, Task, Window, div, hsla, px,
    rgb as gpui_rgb, size,
};
use gpui_kit::component::{Theme, ThemeMode};
use gpui_kit::component::button::{Button, ButtonVariants as _};
use gpui_kit::component::dock::{
    BasePanel, DockArea, DockAreaState, DockEvent, DockLayout, DockPlacement, DockSkin,
    Panel as DockPanel, PanelEvent, PanelInfo, PanelState, panel_handle,
};
use gpui_kit::component::input::{Editor, EditorState, Input, InputState};
use gpui_kit::component::scroll::ScrollableElement as _;
use gpui_kit::component::v_virtual_list;
use std::{
    collections::VecDeque,
    path::PathBuf,
    rc::Rc,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Duration,
};

use crate::{simulated_data::SimulatedSnapshot, simulation::{self, SimulatedTaskMessage, SimulatedTaskMode}};
use crate::connection::{
    self, ActiveConnection, ApiCapabilitySnapshot, ApiResourceDiscoveryState, ConnectionFailure,
    ConnectionPolicy,
};
use crate::kubeconfig::{ExecPluginSummary, ImportedKubeconfig, KubeconfigImportError};
use crate::settings::{AppearancePreference, SettingsDocument, SettingsStore};
use crate::target::{ContextName, SessionRegistry};
use crate::diagnostics::{DiagnosticEvent, Diagnostics, next_correlation_id};

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
static LIGHT_THEME_ACTIVE: AtomicBool = AtomicBool::new(false);

pub(crate) fn initialize_theme_palette(mode: ThemeMode) {
    LIGHT_THEME_ACTIVE.store(!mode.is_dark(), Ordering::Release);
}

fn theme_mode_for_appearance(appearance: AppearancePreference) -> ThemeMode {
    match appearance {
        AppearancePreference::Light => ThemeMode::Light,
        AppearancePreference::System | AppearancePreference::Dark => ThemeMode::Dark,
    }
}

fn rgb(color: u32) -> gpui_kit::Rgba {
    let color = if LIGHT_THEME_ACTIVE.load(Ordering::Acquire) {
        light_theme_color(color)
    } else {
        color
    };
    gpui_rgb(color)
}

fn light_theme_color(color: u32) -> u32 {
    match color {
        BG => 0xf7f9fc,
        PANEL => 0xffffff,
        RAISED => 0xf0f3f7,
        ELEVATED => 0xe8edf3,
        BORDER => 0xd2dae4,
        TEXT_1 => 0x17212f,
        TEXT_2 => 0x344052,
        TEXT_3 => 0x4c5a6c,
        TEXT_4 => 0x586779,
        TEXT_5 => 0x626e7c,
        ACCENT => 0x28659b,
        ACCENT_SOFT => 0xe8f1fa,
        ACCENT_SOFT_2 => 0xe2edf8,
        OK => 0x236b49,
        WARN => 0x875400,
        DANGER => 0xb42318,
        INFO => 0x28659b,
        0x7fb4e6 => 0x28659b,
        0x14261c => 0xe6f4eb,
        0x251617 => 0xfcebea,
        _ => color,
    }
}

pub(crate) struct Workbench {
    dock_area: Entity<DockArea>,
    navigation: Entity<ResourceNavigation>,
    search_input: Entity<InputState>,
    palette_input: Entity<InputState>,
    kubeconfig_path_input: Entity<InputState>,
    namespace_override_input: Entity<InputState>,
    panel_focus_handles: Vec<(PanelKind, FocusHandle)>,
    settings_store: Option<SettingsStore>,
    settings: SettingsDocument,
    settings_notice: Option<String>,
    diagnostics: Option<Diagnostics>,
    diagnostics_notice: Option<String>,
}

enum KubeconfigImportSource {
    Path(PathBuf),
    Environment,
}

#[derive(Clone)]
struct ExecTrustPrompt {
    context_name: String,
    namespace_override: Option<String>,
    plugin: ExecPluginSummary,
}

#[derive(Clone, Copy)]
enum ConnectionViewState {
    Disconnected,
    Connecting,
    Connected { attempts: u8 },
    Failed(ConnectionFailure),
}

impl ConnectionViewState {
    fn label(self, has_target: bool) -> String {
        match self {
            Self::Disconnected if has_target => "TARGET SELECTED / DISCONNECTED".to_string(),
            Self::Disconnected => "OFFLINE / SIMULATED".to_owned(),
            Self::Connecting => "CONNECTING".to_owned(),
            Self::Connected { attempts } => format!("CONNECTED / {attempts} ATTEMPT(S)"),
            Self::Failed(_) => "CONNECTION FAILED".to_owned(),
        }
    }
}

#[derive(Clone, Copy)]
enum ConnectionPolicyField {
    ConnectTimeout,
    RequestTimeout,
    RetryLimit,
}

impl ConnectionPolicyField {
    fn id(self) -> &'static str {
        match self {
            Self::ConnectTimeout => "connect-timeout",
            Self::RequestTimeout => "request-timeout",
            Self::RetryLimit => "retry-limit",
        }
    }
}

struct SettingsSession {
    store: Option<SettingsStore>,
    settings: SettingsDocument,
    notice: Option<String>,
}

impl Default for SettingsSession {
    fn default() -> Self {
        Self {
            store: None,
            settings: SettingsDocument::default(),
            notice: None,
        }
    }
}

pub struct AxiomLensPrototype {
    dock_area: Entity<DockArea>,
    navigation: Entity<ResourceNavigation>,
    search_input: Entity<InputState>,
    palette_input: Entity<InputState>,
    kubeconfig_path_input: Entity<InputState>,
    namespace_override_input: Entity<InputState>,
    panel_focus_handles: Vec<(PanelKind, FocusHandle)>,
    focus_handle: FocusHandle,
    palette_open: bool,
    palette_query: String,
    selected_palette_result: usize,
    kubeconfig_import_open: bool,
    kubeconfig_selection_open: bool,
    kubeconfig_import_loading: bool,
    kubeconfig_import_error: Option<SharedString>,
    imported_kubeconfig: Option<ImportedKubeconfig>,
    selected_context_name: Option<ContextName>,
    selected_namespace: Option<String>,
    exec_trust_prompt: Option<ExecTrustPrompt>,
    exec_trust_saving: bool,
    exec_trust_error: Option<SharedString>,
    session_registry: SessionRegistry,
    connection_view_state: ConnectionViewState,
    resource_capabilities: Option<ApiCapabilitySnapshot>,
    _active_connection: Option<ActiveConnection>,
    previous_focus: Option<FocusHandle>,
    theme_mode: ThemeMode,
    settings_store: Option<SettingsStore>,
    settings: SettingsDocument,
    settings_notice: Option<SharedString>,
    settings_write_generation: u64,
    diagnostics: Option<Diagnostics>,
    diagnostics_notice: Option<SharedString>,
    _navigation_subscription: Subscription,
    _search_subscription: Subscription,
    _palette_subscription: Subscription,
    _layout_subscription: Subscription,
}

impl AxiomLensPrototype {
    pub(crate) fn new(workbench: Workbench, cx: &mut Context<Self>) -> Self {
        let Workbench {
            dock_area,
            navigation,
            search_input,
            palette_input,
            kubeconfig_path_input,
            namespace_override_input,
            panel_focus_handles,
            settings_store,
            settings,
            settings_notice,
            diagnostics,
            diagnostics_notice,
        } = workbench;
        let theme_mode = theme_mode_for_appearance(settings.preferences.appearance);
        initialize_theme_palette(theme_mode);
        let search_navigation = navigation.clone();
        let layout_dock_area = dock_area.clone();
        let navigation_subscription = cx.observe(&navigation, |_, _, cx| cx.notify());
        let search_subscription = cx.observe(&search_input, move |_, input, cx| {
            let query = input.read(cx).value().to_string();
            search_navigation.update(cx, |navigation, cx| {
                navigation.set_search_query(query);
                cx.notify();
            });
        });
        let palette_subscription = cx.observe(&palette_input, |app, input, cx| {
            app.palette_query = input.read(cx).value().to_string();
            app.selected_palette_result = 0;
            cx.notify();
        });
        let layout_subscription = cx.subscribe(
            &layout_dock_area,
            |app, area, event: &DockEvent, cx| {
                if matches!(event, DockEvent::LayoutChanged) {
                    app.persist_dock_layout(area, cx);
                }
            },
        );

        Self {
            dock_area,
            navigation,
            search_input,
            palette_input,
            kubeconfig_path_input,
            namespace_override_input,
            panel_focus_handles,
            focus_handle: cx.focus_handle(),
            palette_open: false,
            palette_query: String::new(),
            selected_palette_result: 0,
            kubeconfig_import_open: false,
            kubeconfig_selection_open: false,
            kubeconfig_import_loading: false,
            kubeconfig_import_error: None,
            imported_kubeconfig: None,
            selected_context_name: None,
            selected_namespace: None,
            exec_trust_prompt: None,
            exec_trust_saving: false,
            exec_trust_error: None,
            session_registry: SessionRegistry::default(),
            connection_view_state: ConnectionViewState::Disconnected,
            resource_capabilities: None,
            _active_connection: None,
            previous_focus: None,
            theme_mode,
            settings_store,
            settings,
            settings_notice: settings_notice.map(SharedString::from),
            settings_write_generation: 0,
            diagnostics,
            diagnostics_notice: diagnostics_notice.map(SharedString::from),
            _navigation_subscription: navigation_subscription,
            _search_subscription: search_subscription,
            _palette_subscription: palette_subscription,
            _layout_subscription: layout_subscription,
        }
    }

    fn persist_dock_layout(&mut self, area: Entity<DockArea>, cx: &mut Context<Self>) {
        let layout = area.read(cx).dump(cx);
        match serde_json::to_value(layout) {
            Ok(layout) => {
                let mut settings = self.settings.clone();
                settings.workspace_layout = Some(layout);
                self.persist_settings(settings, cx);
            }
            Err(error) => {
                if let Some(diagnostics) = &self.diagnostics {
                    diagnostics.record(
                        DiagnosticEvent::SettingsSaveFailed,
                        "settings",
                        None,
                        Some("layout_serialization"),
                        None,
                    );
                }
                self.settings_notice = Some(SharedString::from(format!(
                    "Layout could not be saved: {error}"
                )));
                cx.notify();
            }
        }
    }

    fn persist_settings(&mut self, settings: SettingsDocument, cx: &mut Context<Self>) {
        self.settings = settings.clone();
        self.settings_write_generation = self.settings_write_generation.wrapping_add(1);
        let generation = self.settings_write_generation;
        let Some(store) = self.settings_store.clone() else {
            if let Some(diagnostics) = &self.diagnostics {
                diagnostics.record(
                    DiagnosticEvent::SettingsSaveFailed,
                    "settings",
                    None,
                    Some("store_unavailable"),
                    None,
                );
            }
            self.settings_notice = Some(SharedString::from(
                "Settings are session-only; configuration storage is unavailable.",
            ));
            cx.notify();
            return;
        };

        self.settings_notice = Some(SharedString::from("Saving settings..."));
        cx.notify();
        let executor = cx.background_executor().clone();
        let debounce = executor.timer(Duration::from_millis(250));
        let app = cx.weak_entity();
        let diagnostics = self.diagnostics.clone();
        cx.spawn(async move |_, cx| {
            debounce.await;
            let is_current = app
                .update(cx, |app, _| app.settings_write_generation == generation)
                .unwrap_or(false);
            if !is_current {
                return;
            }

            let result = executor.spawn(async move { store.save(&settings) }).await;
            if let (Some(diagnostics), Err(error)) = (&diagnostics, &result) {
                diagnostics.record(
                    DiagnosticEvent::SettingsSaveFailed,
                    "settings",
                    None,
                    Some(error.diagnostic_code()),
                    None,
                );
            }
            app.update(cx, |app, cx| {
                if app.settings_write_generation == generation {
                    app.settings_notice = result.err().map(|error| {
                        SharedString::from(format!("Settings were not saved: {error}"))
                    });
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
    }

    fn command_items(&self, cx: &App) -> Vec<PaletteItem> {
        let navigation = self.navigation.read(cx);
        let dock_area = self.dock_area.read(cx);
        build_palette_items(
            &navigation,
            &self.palette_query,
            [
                dock_area.is_dock_open(DockPlacement::Left),
                dock_area.is_dock_open(DockPlacement::Right),
                dock_area.is_dock_open(DockPlacement::Bottom),
            ],
        )
    }

    fn panel_is_visible(&self, panel: PanelKind, cx: &App) -> bool {
        let dock_area = self.dock_area.read(cx);
        match panel {
            PanelKind::Sidebar => dock_area.is_dock_open(DockPlacement::Left),
            PanelKind::Agent => dock_area.is_dock_open(DockPlacement::Right),
            PanelKind::Activity => dock_area.is_dock_open(DockPlacement::Bottom),
            PanelKind::Resources | PanelKind::Details => true,
        }
    }

    fn panel_focus_handle(&self, panel: PanelKind) -> Option<FocusHandle> {
        self.panel_focus_handles
            .iter()
            .find(|(candidate, _)| *candidate == panel)
            .map(|(_, handle)| handle.clone())
    }

    fn open_command_palette(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.palette_open {
            return;
        }
        self.previous_focus = window.focused(cx);
        self.palette_open = true;
        self.palette_query.clear();
        self.selected_palette_result = 0;
        self.palette_input.update(cx, |input, cx| {
            input.set_value("", window, cx);
        });
        cx.on_next_frame(window, |app, window, cx| {
            if app.palette_open {
                app.palette_input.update(cx, |input, cx| input.focus(window, cx));
            }
        });
        cx.notify();
    }

    fn open_kubeconfig_import(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.kubeconfig_import_open = true;
        self.kubeconfig_selection_open = false;
        self.exec_trust_prompt = None;
        self.exec_trust_error = None;
        self.kubeconfig_import_error = None;
        self.kubeconfig_path_input.update(cx, |input, cx| {
            input.set_value("", window, cx);
        });
        cx.on_next_frame(window, |app, window, cx| {
            if app.kubeconfig_import_open {
                app.kubeconfig_path_input
                    .update(cx, |input, cx| input.focus(window, cx));
            }
        });
        cx.notify();
    }

    fn begin_kubeconfig_import(&mut self, source: KubeconfigImportSource, cx: &mut Context<Self>) {
        if self.kubeconfig_import_loading {
            return;
        }
        self.cancel_active_session();
        self.selected_context_name = None;
        self.kubeconfig_import_loading = true;
        self.kubeconfig_import_error = None;
        cx.notify();

        let executor = cx.background_executor().clone();
        let app = cx.weak_entity();
        cx.spawn(async move |_, cx| {
            let result = executor
                .spawn(async move {
                    match source {
                        KubeconfigImportSource::Path(path) => {
                            ImportedKubeconfig::import_path(path)
                        }
                        KubeconfigImportSource::Environment => {
                            ImportedKubeconfig::import_from_environment()
                        }
                    }
                })
                .await;
            let _ = app.update(cx, |app, cx| {
                app.kubeconfig_import_loading = false;
                match result {
                    Ok(imported) => {
                        app.cancel_active_session();
                        app.imported_kubeconfig = Some(imported);
                        app.selected_context_name = None;
                        app.selected_namespace = None;
                        app.exec_trust_prompt = None;
                        app.exec_trust_error = None;
                        app.kubeconfig_import_error = None;
                        app.kubeconfig_import_open = false;
                        app.kubeconfig_selection_open = true;
                    }
                    Err(error) => {
                        app.kubeconfig_import_error = Some(SharedString::from(error.to_string()));
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn select_kubeconfig_context(
        &mut self,
        name: String,
        namespace_override: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if self.exec_trust_saving {
            return;
        }
        let Some(imported) = self.imported_kubeconfig.clone() else {
            self.kubeconfig_import_error = Some(SharedString::from(
                KubeconfigImportError::UnknownContext.to_string(),
            ));
            cx.notify();
            return;
        };
        let plugin = imported
            .contexts()
            .iter()
            .find(|context| context.name().as_str() == name)
            .and_then(|context| context.exec_plugin())
            .cloned();
        if let Some(plugin) = plugin
            && !self.settings.trusted_exec_plugins.contains(plugin.trust_fingerprint())
        {
            self.exec_trust_prompt = Some(ExecTrustPrompt {
                context_name: name,
                namespace_override,
                plugin,
            });
            self.exec_trust_error = None;
            self.kubeconfig_selection_open = false;
            cx.notify();
            return;
        }
        self.complete_context_selection(name, namespace_override, imported, cx);
    }

    fn complete_context_selection(
        &mut self,
        name: String,
        namespace_override: Option<String>,
        imported: ImportedKubeconfig,
        cx: &mut Context<Self>,
    ) {
        let selection = match namespace_override.as_deref() {
            Some(namespace) => {
                imported.select_context_in_namespace(&name, namespace, &mut self.session_registry)
            }
            None => imported.select_context(&name, &mut self.session_registry),
        };
        match selection {
            Ok(target) => {
                self._active_connection = None;
                self.selected_context_name = Some(target.context_name().clone());
                self.selected_namespace = target
                    .namespace_scope()
                    .single_namespace()
                    .map(|namespace| namespace.as_str().to_owned());
                self.kubeconfig_selection_open = false;
                self.exec_trust_prompt = None;
                self.kubeconfig_import_error = None;
                self.begin_kubeconfig_connection(imported, target, cx);
            }
            Err(error) => {
                self.kubeconfig_import_error = Some(SharedString::from(error.to_string()));
                self.kubeconfig_selection_open = true;
            }
        }
        cx.notify();
    }

    fn cancel_active_session(&mut self) {
        self.session_registry.end_session();
        self._active_connection = None;
        self.resource_capabilities = None;
        self.connection_view_state = ConnectionViewState::Disconnected;
    }

    fn reject_exec_plugin_trust(&mut self, cx: &mut Context<Self>) {
        if self.exec_trust_saving {
            return;
        }
        self.exec_trust_prompt = None;
        self.exec_trust_error = None;
        self.kubeconfig_selection_open = true;
        cx.notify();
    }

    fn trust_exec_plugin_and_connect(&mut self, cx: &mut Context<Self>) {
        if self.exec_trust_saving {
            return;
        }
        let Some(prompt) = self.exec_trust_prompt.clone() else {
            return;
        };
        let Some(store) = self.settings_store.clone() else {
            self.exec_trust_error = Some(SharedString::from(
                "Trust could not be saved; authentication plugin was not run.",
            ));
            cx.notify();
            return;
        };
        let Some(imported) = self.imported_kubeconfig.clone() else {
            self.exec_trust_error = Some(SharedString::from(
                KubeconfigImportError::UnknownContext.to_string(),
            ));
            cx.notify();
            return;
        };

        let mut settings = self.settings.clone();
        settings
            .trusted_exec_plugins
            .insert(prompt.plugin.trust_fingerprint().to_owned());
        self.exec_trust_saving = true;
        self.exec_trust_error = None;
        cx.notify();

        let executor = cx.background_executor().clone();
        let app = cx.weak_entity();
        let diagnostics = self.diagnostics.clone();
        cx.spawn(async move |_, cx| {
            let result = executor
                .spawn(async move { store.save(&settings).map(|()| settings) })
                .await;
            if let (Some(diagnostics), Err(error)) = (&diagnostics, &result) {
                diagnostics.record(
                    DiagnosticEvent::SettingsSaveFailed,
                    "settings",
                    None,
                    Some(error.diagnostic_code()),
                    None,
                );
            }
            let _ = app.update(cx, |app, cx| {
                app.exec_trust_saving = false;
                match result {
                    Ok(settings) => {
                        app.settings = settings;
                        app.settings_write_generation =
                            app.settings_write_generation.wrapping_add(1);
                        app.settings_notice = None;
                        app.exec_trust_error = None;
                        app.exec_trust_prompt = None;
                        app.complete_context_selection(
                            prompt.context_name,
                            prompt.namespace_override,
                            imported,
                            cx,
                        );
                    }
                    Err(_) => {
                        app.exec_trust_error = Some(SharedString::from(
                            "Trust could not be saved; authentication plugin was not run.",
                        ));
                        cx.notify();
                    }
                }
            });
        })
        .detach();
    }

    fn begin_kubeconfig_connection(
        &mut self,
        imported: ImportedKubeconfig,
        target: crate::target::TargetBinding,
        cx: &mut Context<Self>,
    ) {
        self._active_connection = None;
        self.connection_view_state = ConnectionViewState::Connecting;
        self.resource_capabilities = None;
        let Some(cancellation) = self.session_registry.cancellation_for(&target) else {
            self.connection_view_state = ConnectionViewState::Disconnected;
            return;
        };
        let preferences = &self.settings.preferences;
        let policy = ConnectionPolicy::from_seconds(
            preferences.connect_timeout_seconds,
            preferences.request_timeout_seconds,
            preferences.connection_retry_limit,
        )
        .unwrap_or_default();
        let correlation_id = next_correlation_id();
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(
                DiagnosticEvent::KubernetesConnectionStarted,
                "connection",
                Some(correlation_id),
                None,
                None,
            );
        }

        let target_for_completion = target.clone();
        let task_cancellation = cancellation;
        let discovery_cancellation = task_cancellation.clone();
        let trusted_exec_plugins = self.settings.trusted_exec_plugins.clone();
        let diagnostics = self.diagnostics.clone();
        let executor = cx.background_executor().clone();
        let app = cx.weak_entity();
        cx.spawn(async move |_, cx| {
            let result = executor
                .spawn(async move {
                    let connection = connection::connect(
                        imported,
                        target,
                        policy,
                        trusted_exec_plugins,
                        task_cancellation.clone(),
                    )
                    .await?;
                    let discovery_timeout =
                        Duration::from_secs(u64::from(policy.request_timeout_seconds()));
                    let capabilities = connection
                        .discover_capabilities(discovery_cancellation, discovery_timeout)
                        .await?;
                    Ok::<_, ConnectionFailure>((connection, capabilities))
                })
                .await;
            if let (Some(diagnostics), Err(error)) = (&diagnostics, &result) {
                diagnostics.record(
                    DiagnosticEvent::KubernetesConnectionFailed,
                    "connection",
                    Some(correlation_id),
                    Some(error.error_code()),
                    None,
                );
            }
            if let (Some(diagnostics), Ok((connection, _))) = (&diagnostics, &result) {
                diagnostics.record(
                    DiagnosticEvent::KubernetesConnectionSucceeded,
                    "connection",
                    Some(correlation_id),
                    None,
                    None,
                );
                let _ = connection.attempts();
            }
            let _ = app.update(cx, |app, cx| {
                if !app.session_registry.is_current(&target_for_completion) {
                    return;
                }
                match result {
                    Ok((connection, capabilities)) => {
                        app.connection_view_state = ConnectionViewState::Connected {
                            attempts: connection.attempts(),
                        };
                        app.resource_capabilities = Some(capabilities);
                        app._active_connection = Some(connection);
                    }
                    Err(error) => {
                        app.connection_view_state = ConnectionViewState::Failed(error);
                    }
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn adjust_connection_policy(
        &mut self,
        field: ConnectionPolicyField,
        delta: i32,
        cx: &mut Context<Self>,
    ) {
        let mut settings = self.settings.clone();
        match field {
            ConnectionPolicyField::ConnectTimeout => {
                let value = i32::from(settings.preferences.connect_timeout_seconds);
                settings.preferences.connect_timeout_seconds = (value + delta).clamp(1, 60) as u16;
            }
            ConnectionPolicyField::RequestTimeout => {
                let value = i32::from(settings.preferences.request_timeout_seconds);
                settings.preferences.request_timeout_seconds = (value + delta).clamp(1, 120) as u16;
            }
            ConnectionPolicyField::RetryLimit => {
                let value = i32::from(settings.preferences.connection_retry_limit);
                settings.preferences.connection_retry_limit = (value + delta).clamp(0, 3) as u8;
            }
        }
        self.persist_settings(settings, cx);
    }

    fn close_command_palette(
        &mut self,
        focus_target: Option<FocusHandle>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.palette_open = false;
        let focus_handle = focus_target
            .or_else(|| self.previous_focus.take())
            .unwrap_or_else(|| self.focus_handle.clone());
        self.previous_focus = None;
        window.focus(&focus_handle, cx);
        cx.notify();
    }

    fn focus_next_panel(&self, window: &mut Window, cx: &mut Context<Self>) {
        let visible_panels = self
            .panel_focus_handles
            .iter()
            .filter(|(panel, _)| self.panel_is_visible(*panel, cx))
            .collect::<Vec<_>>();
        if visible_panels.is_empty() {
            return;
        }
        let current_index = visible_panels
            .iter()
            .position(|(_, handle)| handle.is_focused(window));
        let next_index = current_index.map_or(0, |index| (index + 1) % visible_panels.len());
        window.focus(&visible_panels[next_index].1, cx);
    }

    fn handle_key_down(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(action) = palette_key_action(
            &event.keystroke.key,
            event.keystroke.modifiers,
            self.palette_open,
        ) else {
            return;
        };
        window.prevent_default();
        cx.stop_propagation();

        match action {
            PaletteKeyAction::Open => self.open_command_palette(window, cx),
            PaletteKeyAction::Close => self.close_command_palette(None, window, cx),
            PaletteKeyAction::MoveNext | PaletteKeyAction::MovePrevious => {
                let enabled = self
                    .command_items(cx)
                    .iter()
                    .map(|item| item.enabled)
                    .collect::<Vec<_>>();
                let direction = if action == PaletteKeyAction::MoveNext {
                    1
                } else {
                    -1
                };
                if let Some(index) =
                    move_to_enabled_result(&enabled, self.selected_palette_result, direction)
                {
                    self.selected_palette_result = index;
                    cx.notify();
                }
            }
            PaletteKeyAction::Activate => {
                let selected = self
                    .command_items(cx)
                    .get(self.selected_palette_result)
                    .filter(|item| item.enabled)
                    .map(|item| item.action);
                if let Some(action) = selected {
                    self.execute_palette_action(action, window, cx);
                }
            }
            PaletteKeyAction::FocusNextPanel => self.focus_next_panel(window, cx),
        }
    }

    fn execute_palette_action(
        &mut self,
        action: PaletteAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let focus_target = match action {
            PaletteAction::OpenResource(tab) => {
                self.navigation.update(cx, |navigation, cx| {
                    navigation.switch_context_to(tab.context_index);
                    navigation.open_resource(tab.resource_index);
                    cx.notify();
                });
                self.panel_focus_handle(PanelKind::Details)
            }
            PaletteAction::SelectTab(tab) => {
                self.navigation.update(cx, |navigation, cx| {
                    navigation.select_tab(tab);
                    cx.notify();
                });
                self.panel_focus_handle(PanelKind::Details)
            }
            PaletteAction::SwitchContext(context_index) => {
                self.navigation.update(cx, |navigation, cx| {
                    navigation.switch_context_to(context_index);
                    cx.notify();
                });
                self.panel_focus_handle(PanelKind::Resources)
            }
            PaletteAction::CloseActiveTab => {
                if let Some(tab) = self.navigation.read(cx).active_tab {
                    self.navigation.update(cx, |navigation, cx| {
                        navigation.close_tab(tab);
                        cx.notify();
                    });
                }
                self.panel_focus_handle(PanelKind::Details)
            }
            PaletteAction::FocusPanel(panel) => self.panel_focus_handle(panel),
            PaletteAction::ToggleDock(target) => {
                let placement = target.placement();
                self.dock_area.update(cx, |area, cx| {
                    area.toggle_dock(placement, window, cx);
                });
                if self.dock_area.read(cx).is_dock_open(placement) {
                    self.panel_focus_handle(target.panel())
                } else {
                    Some(self.focus_handle.clone())
                }
            }
        };
        self.close_command_palette(focus_target, window, cx);
    }

    fn render_kubeconfig_import(&self, cx: &mut Context<Self>) -> Div {
        let error = self.kubeconfig_import_error.clone();
        let loading = self.kubeconfig_import_loading;
        div()
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .items_start()
            .pt(px(70.0))
            .bg(hsla(0.0, 0.0, 0.0, 0.58))
            .child(
                div()
                    .w(px(620.0))
                    .flex()
                    .flex_col()
                    .rounded_lg()
                    .border_1()
                    .border_color(rgb(BORDER))
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .py_3()
                            .child(div().text_sm().text_color(rgb(TEXT_1)).child("Import kubeconfig"))
                            .child(div().flex_1())
                            .child(
                                Button::new("close-kubeconfig-import")
                                    .label("Close")
                                    .compact()
                                    .ghost()
                                    .on_click(cx.listener(|app, _, _, cx| {
                                        app.kubeconfig_import_open = false;
                                        app.kubeconfig_import_error = None;
                                        cx.notify();
                                    })),
                            ),
                    )
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .px_3()
                            .py_2()
                            .child(div().text_xs().text_color(rgb(TEXT_4)).child("KUBECONFIG FILE")),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .h(px(38.0))
                            .mx_3()
                            .px_2()
                            .rounded_md()
                            .border_1()
                            .border_color(rgb(BORDER))
                            .bg(rgb(BG))
                            .child(
                                Input::new(&self.kubeconfig_path_input)
                                    .id("kubeconfig-path-input")
                                    .aria_label("Kubeconfig file path")
                                    .cleanable(true)
                                    .appearance(false)
                                    .bordered(false)
                                    .focus_bordered(false)
                                    .flex_1(),
                            ),
                    )
                    .child(
                        div()
                            .px_3()
                            .pt_2()
                            .child(div().text_xs().text_color(rgb(TEXT_5)).child(
                                "Import reads one file; credentials stay in source file.",
                            )),
                    )
                    .child(if let Some(error) = error {
                        div()
                            .px_3()
                            .pt_2()
                            .child(div().text_xs().text_color(rgb(DANGER)).child(error))
                    } else {
                        div().h(px(24.0))
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px_3()
                            .py_2()
                            .child(
                                Button::new("load-kubeconfig-environment")
                                    .label("Use KUBECONFIG")
                                    .compact()
                                    .ghost()
                                    .on_click(cx.listener(|app, _, _, cx| {
                                        app.begin_kubeconfig_import(
                                            KubeconfigImportSource::Environment,
                                            cx,
                                        );
                                    })),
                            )
                            .child(
                                Button::new("load-kubeconfig-path")
                                    .label(if loading { "Loading..." } else { "Load file" })
                                    .compact()
                                    .primary()
                                    .on_click(cx.listener(|app, _, _, cx| {
                                        let path = app
                                            .kubeconfig_path_input
                                            .read(cx)
                                            .value()
                                            .to_string();
                                        if path.trim().is_empty() {
                                            app.kubeconfig_import_error = Some(SharedString::from(
                                                "Enter a kubeconfig file path.",
                                            ));
                                            cx.notify();
                                        } else {
                                            app.begin_kubeconfig_import(
                                                KubeconfigImportSource::Path(expand_home_path(&path)),
                                                cx,
                                            );
                                        }
                                    })),
                            ),
                    ),
            )
    }

    fn render_connection_policy_control(
        &self,
        label: &'static str,
        value: String,
        field: ConnectionPolicyField,
        cx: &mut Context<Self>,
    ) -> Div {
        let decrease = field;
        let increase = field;
        div()
            .flex()
            .items_center()
            .gap_1()
            .child(div().text_xs().text_color(rgb(TEXT_4)).child(label))
            .child(
                Button::new(format!("{}-decrease", field.id()))
                    .label("-")
                    .compact()
                    .ghost()
                    .on_click(cx.listener(move |app, _, _, cx| {
                        app.adjust_connection_policy(decrease, -1, cx);
                    })),
            )
            .child(
                div()
                    .w(px(32.0))
                    .text_center()
                    .font_family("Menlo")
                    .text_xs()
                    .text_color(rgb(TEXT_2))
                    .child(value),
            )
            .child(
                Button::new(format!("{}-increase", field.id()))
                    .label("+")
                    .compact()
                    .ghost()
                    .on_click(cx.listener(move |app, _, _, cx| {
                        app.adjust_connection_policy(increase, 1, cx);
                    })),
            )
    }

    fn render_connection_policy_controls(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .items_center()
            .justify_between()
            .gap_2()
            .px_3()
            .py_2()
            .child(self.render_connection_policy_control(
                "CONNECT",
                format!("{}s", self.settings.preferences.connect_timeout_seconds),
                ConnectionPolicyField::ConnectTimeout,
                cx,
            ))
            .child(self.render_connection_policy_control(
                "REQUEST",
                format!("{}s", self.settings.preferences.request_timeout_seconds),
                ConnectionPolicyField::RequestTimeout,
                cx,
            ))
            .child(self.render_connection_policy_control(
                "RETRIES",
                self.settings.preferences.connection_retry_limit.to_string(),
                ConnectionPolicyField::RetryLimit,
                cx,
            ))
    }

    fn render_exec_trust_prompt(&self, cx: &mut Context<Self>) -> Div {
        let Some(prompt) = self.exec_trust_prompt.as_ref() else {
            return div();
        };
        let plugin = &prompt.plugin;
        let environment_names = if plugin.environment_names().is_empty() {
            "None".to_owned()
        } else {
            plugin.environment_names().join(", ")
        };
        let argument_summary = if plugin.argument_summary().is_empty() {
            "None".to_owned()
        } else {
            plugin.argument_summary().join(" ")
        };
        let api_version = plugin.api_version().unwrap_or("Not specified");
        let interactive_mode = plugin.interactive_mode().unwrap_or("Not specified");
        let trust_error = self.exec_trust_error.clone();
        let saving = self.exec_trust_saving;

        div()
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .items_start()
            .pt(px(70.0))
            .bg(hsla(0.0, 0.0, 0.0, 0.62))
            .child(
                div()
                    .w(px(680.0))
                    .flex()
                    .flex_col()
                    .rounded_lg()
                    .border_1()
                    .border_color(rgb(BORDER))
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .py_3()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(TEXT_1))
                                    .child("Trust authentication plugin?"),
                            )
                            .child(div().flex_1())
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(WARN))
                                    .child("LOCAL CODE EXECUTION"),
                            ),
                    )
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .px_3()
                            .py_3()
                            .bg(rgb(ACCENT_SOFT))
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(TEXT_2))
                                    .child("This executable runs locally to obtain Kubernetes credentials. It cannot be used by Agent actions."),
                            ),
                    )
                    .child(self.exec_trust_detail_row("CONTEXT", &prompt.context_name))
                    .child(self.exec_trust_detail_row("EXECUTABLE", plugin.executable()))
                    .child(self.exec_trust_detail_row("API VERSION", api_version))
                    .child(self.exec_trust_detail_row("ARGUMENTS", &argument_summary))
                    .child(self.exec_trust_detail_row("ENVIRONMENT NAMES", &environment_names))
                    .child(self.exec_trust_detail_row("INTERACTIVE MODE", interactive_mode))
                    .child(if let Some(error) = trust_error {
                        div()
                            .px_3()
                            .py_2()
                            .child(div().text_xs().text_color(rgb(DANGER)).child(error))
                    } else {
                        div().h(px(12.0))
                    })
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .gap_2()
                            .px_3()
                            .py_3()
                            .child(
                                Button::new("reject-exec-plugin-trust")
                                    .label("Reject")
                                    .compact()
                                    .ghost()
                                    .on_click(cx.listener(|app, _, _, cx| {
                                        app.reject_exec_plugin_trust(cx);
                                    })),
                            )
                            .child(
                                Button::new("trust-exec-plugin-and-connect")
                                    .label(if saving { "Saving trust..." } else { "Trust and connect" })
                                    .compact()
                                    .primary()
                                    .on_click(cx.listener(|app, _, _, cx| {
                                        app.trust_exec_plugin_and_connect(cx);
                                    })),
                            ),
                    ),
            )
    }

    fn exec_trust_detail_row(&self, label: &'static str, value: &str) -> Div {
        div()
            .flex()
            .items_center()
            .gap_3()
            .px_3()
            .py_2()
            .child(
                div()
                    .w(px(150.0))
                    .text_xs()
                    .text_color(rgb(TEXT_5))
                    .child(label),
            )
            .child(
                div()
                    .flex_1()
                    .font_family("Menlo")
                    .text_xs()
                    .text_color(rgb(TEXT_2))
                    .child(value.to_owned()),
            )
    }

    fn render_kubeconfig_contexts(&self, cx: &mut Context<Self>) -> Div {
        let rows = self
            .imported_kubeconfig
            .as_ref()
            .map(|imported| {
                imported
                    .contexts()
                    .iter()
                    .enumerate()
                    .map(|(index, context)| {
                        let name = context.name().as_str().to_owned();
                        let details = format!(
                            "{} / {} / {}",
                            context.cluster_name(),
                            context.api_server(),
                            context.default_namespace().as_str()
                        );
                        let selected_name = name.clone();
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .px_3()
                            .py_2()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .flex_1()
                                    .child(div().text_sm().text_color(rgb(TEXT_1)).child(name))
                                    .child(div().text_xs().text_color(rgb(TEXT_4)).child(details)),
                            )
                            .child(
                                Button::new(format!("select-kubeconfig-context-{index}"))
                                    .label("Connect")
                                    .compact()
                                    .ghost()
                                    .on_click(cx.listener(move |app, _, _, cx| {
                                        let value = app
                                            .namespace_override_input
                                            .read(cx)
                                            .value()
                                            .to_string();
                                        let namespace_override = (!value.trim().is_empty())
                                            .then(|| value.trim().to_owned());
                                        app.select_kubeconfig_context(
                                            selected_name.clone(),
                                            namespace_override,
                                            cx,
                                        );
                                    })),
                            )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let capability_rows = self
            .resource_capabilities
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .resources()
                    .iter()
                    .map(|capability| {
                        let group = if capability.api_group().is_empty() {
                            "core"
                        } else {
                            capability.api_group()
                        };
                        let state = match capability.state() {
                            ApiResourceDiscoveryState::Served {
                                scope,
                                list_supported,
                                watch_supported,
                                access,
                            } => format!(
                                "SERVED / {} / LIST {} / WATCH {} / LIST RBAC {}",
                                match scope {
                                    connection::ApiResourceScope::Namespaced => "NAMESPACE",
                                    connection::ApiResourceScope::Cluster => "CLUSTER",
                                },
                                if list_supported { "YES" } else { "NO" },
                                if watch_supported { "YES" } else { "NO" },
                                match access {
                                    connection::ApiResourceAccessAssessment::Allowed => "ALLOWED",
                                    connection::ApiResourceAccessAssessment::Denied => "DENIED",
                                    connection::ApiResourceAccessAssessment::NotChecked => "NOT CHECKED",
                                    connection::ApiResourceAccessAssessment::Unavailable => "UNAVAILABLE",
                                }
                            ),
                            ApiResourceDiscoveryState::Unsupported => "UNSUPPORTED".to_owned(),
                            ApiResourceDiscoveryState::DiscoveryDenied => {
                                "DISCOVERY DENIED".to_owned()
                            }
                            ApiResourceDiscoveryState::Unavailable => "UNAVAILABLE".to_owned(),
                            ApiResourceDiscoveryState::TimedOut => "TIMED OUT".to_owned(),
                        };
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .gap_2()
                            .h(px(22.0))
                            .px_3()
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(TEXT_2))
                                    .child(format!(
                                        "{} / {}/{}",
                                        capability.kind(),
                                        group,
                                        capability.api_version()
                                    )),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(match capability.state() {
                                        ApiResourceDiscoveryState::Served {
                                            access: connection::ApiResourceAccessAssessment::Allowed,
                                            list_supported: true,
                                            ..
                                        } => OK,
                                        ApiResourceDiscoveryState::DiscoveryDenied
                                        | ApiResourceDiscoveryState::Served {
                                            access: connection::ApiResourceAccessAssessment::Denied
                                                | connection::ApiResourceAccessAssessment::Unavailable,
                                            ..
                                        }
                                        | ApiResourceDiscoveryState::TimedOut
                                        | ApiResourceDiscoveryState::Unavailable => WARN,
                                        ApiResourceDiscoveryState::Unsupported
                                        | ApiResourceDiscoveryState::Served { .. } => TEXT_5,
                                    }))
                                    .child(state),
                            )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let capability_hint = if self.resource_capabilities.is_some() {
            "Discovery capability only; API authorization is enforced on each read."
        } else {
            "Connect to discover served APIs; no global Namespace listing is required."
        };
        let current_context = self
            .imported_kubeconfig
            .as_ref()
            .and_then(ImportedKubeconfig::current_context)
            .map(|context| format!("Current: {}", context.as_str()))
            .unwrap_or_else(|| "No current Context".to_owned());
        let error = self.kubeconfig_import_error.clone();
        let connection_status = self.connection_view_state.label(self.selected_context_name.is_some());

        div()
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .items_start()
            .pt(px(70.0))
            .bg(hsla(0.0, 0.0, 0.0, 0.58))
            .child(
                div()
                    .w(px(680.0))
                    .flex()
                    .flex_col()
                    .rounded_lg()
                    .border_1()
                    .border_color(rgb(BORDER))
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .py_3()
                            .child(div().text_sm().text_color(rgb(TEXT_1)).child("Select Context"))
                            .child(div().flex_1())
                            .child(div().text_xs().text_color(rgb(TEXT_5)).child(current_context))
                            .child(
                                Button::new("close-kubeconfig-contexts")
                                    .label("Close")
                                    .compact()
                                    .ghost()
                                    .on_click(cx.listener(|app, _, _, cx| {
                                        app.kubeconfig_selection_open = false;
                                        cx.notify();
                                    })),
                            ),
                    )
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .py_2()
                            .child(
                                div()
                                    .w(px(150.0))
                                    .text_xs()
                                    .text_color(rgb(TEXT_5))
                                    .child("NAMESPACE OVERRIDE"),
                            )
                            .child(
                                Input::new(&self.namespace_override_input)
                                    .id("namespace-override-input")
                                    .aria_label("Optional authorized Namespace override; blank uses the Context default")
                                    .cleanable(true)
                                    .appearance(false)
                                    .bordered(true)
                                    .flex_1(),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(TEXT_5))
                                    .child("Blank = Context default; no global scan"),
                            ),
                    )
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(self.render_connection_policy_controls(cx))
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(div().h(px(180.0)).overflow_y_scrollbar().children(rows))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .h(px(26.0))
                            .px_3()
                            .bg(rgb(BG))
                            .child(div().text_xs().text_color(rgb(TEXT_5)).child("API CAPABILITIES")),
                    )
                    .child(div().h(px(200.0)).overflow_y_scrollbar().children(capability_rows))
                    .child(
                        div()
                            .px_3()
                            .py_1()
                            .child(div().text_xs().text_color(rgb(TEXT_5)).child(capability_hint)),
                    )
                    .child(if let Some(error) = error {
                        div()
                            .px_3()
                            .py_2()
                            .child(div().text_xs().text_color(rgb(DANGER)).child(error))
                    } else {
                        div().h(px(24.0))
                    })
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px_3()
                            .py_2()
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(TEXT_5))
                                    .child(format!("{connection_status} · sample data only")),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(if self.selected_context_name.is_some() {
                                        div().child(
                                            Button::new("disconnect-selected-context")
                                                .label("Disconnect")
                                                .compact()
                                                .ghost()
                                                .on_click(cx.listener(|app, _, _, cx| {
                                                    app.cancel_active_session();
                                                    cx.notify();
                                                })),
                                        )
                                    } else {
                                        div()
                                    })
                                    .child(
                                        Button::new("import-another-kubeconfig")
                                            .label("Import another")
                                            .compact()
                                            .ghost()
                                            .on_click(cx.listener(|app, _, window, cx| {
                                                app.open_kubeconfig_import(window, cx);
                                            })),
                                    ),
                            ),
                    ),
            )
    }

    fn render_command_palette(&self, cx: &mut Context<Self>) -> Div {
        let items = self.command_items(cx);
        let mut previous_group = None;
        let rows = items
            .iter()
            .enumerate()
            .flat_map(|(index, item)| {
                let mut rows = Vec::with_capacity(2);
                if previous_group != Some(item.group) {
                    previous_group = Some(item.group);
                    rows.push(
                        div()
                            .px_3()
                            .py_1()
                            .text_xs()
                            .text_color(rgb(TEXT_5))
                            .child(item.group),
                    );
                }

                let selected = index == self.selected_palette_result && item.enabled;
                let detail = if item.enabled {
                    item.detail.clone()
                } else {
                    format!("DISABLED - {}", item.detail)
                };
                let row = div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .h(px(36.0))
                    .px_3()
                    .bg(rgb(if selected { ACCENT_SOFT } else { RAISED }));
                if item.enabled {
                    let action = item.action;
                    let label = if selected {
                        format!("> {}", item.label)
                    } else {
                        item.label.clone()
                    };
                    rows.push(
                        row.child(
                            Button::new(format!("palette-result-{index}"))
                                .label(label)
                                .compact()
                                .ghost()
                                .on_click(cx.listener(move |app, _, window, cx| {
                                    app.execute_palette_action(action, window, cx);
                                })),
                        )
                        .child(
                            div()
                                .flex_1()
                                .text_xs()
                                .text_color(rgb(TEXT_4))
                                .child(detail),
                        ),
                    );
                } else {
                    rows.push(
                        row.child(
                            div()
                                .text_sm()
                                .text_color(rgb(TEXT_5))
                                .child(item.label.clone()),
                        )
                        .child(
                            div()
                                .flex_1()
                                .text_xs()
                                .text_color(rgb(TEXT_5))
                                .child(detail),
                        ),
                    );
                }
                rows
            })
            .collect::<Vec<_>>();
        let result_list = if rows.is_empty() {
            div()
                .h(px(360.0))
                .flex()
                .items_center()
                .justify_center()
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(TEXT_4))
                        .child("No navigation results. Try a resource name or type."),
                )
        } else {
            div()
                .h(px(360.0))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .children(rows)
                        .overflow_y_scrollbar(),
                )
        };

        div()
            .absolute()
            .inset_0()
            .flex()
            .justify_center()
            .items_start()
            .pt(px(70.0))
            .bg(hsla(0.0, 0.0, 0.0, 0.58))
            .child(
                div()
                    .w(px(620.0))
                    .flex()
                    .flex_col()
                    .rounded_lg()
                    .border_1()
                    .border_color(rgb(BORDER))
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .py_3()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(TEXT_1))
                                    .child("Command Palette"),
                            )
                            .child(div().flex_1())
                            .child(
                                div()
                                    .text_xs()
                                    .font_family("Menlo")
                                    .text_color(rgb(TEXT_5))
                                    .child("ESC"),
                            ),
                    )
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .h(px(46.0))
                            .child(div().text_xs().text_color(rgb(ACCENT)).child("SEARCH"))
                            .child(
                                Input::new(&self.palette_input)
                                    .id("command-palette-input")
                                    .aria_label("Search local navigation commands and simulated resources")
                                    .cleanable(true)
                                    .appearance(false)
                                    .bordered(false)
                                    .focus_bordered(false)
                                    .flex_1(),
                            ),
                    )
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(result_list)
                    .child(div().h(px(1.0)).bg(rgb(BORDER)))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px_3()
                            .py_2()
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(TEXT_5))
                                    .child("UP / DOWN  MOVE    ENTER  SELECT    ESC  CLOSE"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .font_family("Menlo")
                                    .text_color(rgb(TEXT_5))
                                    .child("LOCAL ONLY"),
                            ),
                    ),
            )
    }

}

impl Focusable for AxiomLensPrototype {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for AxiomLensPrototype {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let snapshot = self.navigation.read(cx).active_snapshot();
        let selected_context = self.selected_context_name.as_ref().and_then(|name| {
            self.imported_kubeconfig.as_ref()?.contexts().iter().find(|context| {
                context.name().as_str() == name.as_str()
            })
        });
        let context_label = selected_context
            .map(|context| context.name().as_str())
            .unwrap_or("Choose Context");
        let connection_status = self.connection_view_state.label(selected_context.is_some());
        let context_control = Button::new("kubeconfig-context-selector")
            .label(format!("{context_label} v"))
            .compact()
            .ghost()
            .on_click(cx.listener(|app, _, window, cx| {
                if app.imported_kubeconfig.is_some() {
                    app.kubeconfig_import_error = None;
                    app.kubeconfig_selection_open = true;
                    cx.notify();
                } else {
                    app.open_kubeconfig_import(window, cx);
                }
            }));

        let commands_button = div()
            .flex()
            .items_center()
            .gap_1()
            .child(
                Button::new("open-command-palette")
                    .label("Commands")
                    .compact()
                    .ghost()
                    .on_click(cx.listener(|app, _, window, cx| {
                        app.open_command_palette(window, cx);
                    })),
            )
            .child(
                div()
                    .text_xs()
                    .font_family("Menlo")
                    .text_color(rgb(TEXT_5))
                    .child("CMD/CTRL+K"),
            );
        let theme_button = Button::new("toggle-theme")
            .label(if self.theme_mode.is_dark() { "Light" } else { "Dark" })
            .compact()
            .ghost()
            .on_click(cx.listener(|app, _, _, cx| {
                let next_mode = if app.theme_mode.is_dark() {
                    ThemeMode::Light
                } else {
                    ThemeMode::Dark
                };
                app.theme_mode = next_mode;
                let mut settings = app.settings.clone();
                settings.preferences.appearance = if next_mode.is_dark() {
                    AppearancePreference::Dark
                } else {
                    AppearancePreference::Light
                };
                app.persist_settings(settings, cx);
                LIGHT_THEME_ACTIVE.store(!next_mode.is_dark(), Ordering::Release);
                Theme::change(next_mode, None, cx);
                cx.notify();
            }));

        let workspace = div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(BG))
            .track_focus(&self.focus_handle)
            .capture_key_down(cx.listener(|app, event, window, cx| {
                app.handle_key_down(event, window, cx);
            }))
            .child(topbar(
                snapshot,
                selected_context.map(|context| {
                    (
                        context.cluster_name(),
                        self.selected_namespace
                            .as_deref()
                            .unwrap_or(context.default_namespace().as_str()),
                    )
                }),
                connection_status.clone(),
                &self.search_input,
                context_control,
                commands_button,
                theme_button,
            ));
        let notice = self
            .settings_notice
            .clone()
            .or_else(|| self.diagnostics_notice.clone())
            .or_else(|| {
                self.diagnostics
                    .as_ref()
                    .and_then(Diagnostics::health_notice)
                    .map(SharedString::from)
            });
        let workspace = if let Some(notice) = notice {
            workspace.child(
                div()
                    .id("settings-notice")
                    .flex()
                    .items_center()
                    .h(px(28.0))
                    .px_3()
                    .bg(rgb(RAISED))
                    .child(div().text_xs().text_color(rgb(WARN)).child(notice.clone())),
            )
        } else {
            workspace
        };
        let workspace = if let Some(context) = selected_context {
            workspace.child(
                div()
                    .id("selected-kubeconfig-target-notice")
                    .flex()
                    .items_center()
                    .h(px(28.0))
                    .px_3()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(INFO))
                            .child(format!(
                                "Target selected: {} / {} / {} · {}; resource panes remain simulated",
                                context.cluster_name(),
                                context.name().as_str(),
                                self.selected_namespace
                                    .as_deref()
                                    .unwrap_or(context.default_namespace().as_str()),
                                connection_status
                            )),
                    ),
            )
        } else {
            workspace
        };
        let workspace = if let ConnectionViewState::Failed(failure) = self.connection_view_state {
            let retry_context = self
                .selected_context_name
                .as_ref()
                .map(|context| context.as_str().to_owned());
            let retry_namespace = self.selected_namespace.clone();
            workspace.child(
                div()
                    .id("connection-failure-notice")
                    .flex()
                    .items_center()
                    .gap_3()
                    .px_3()
                    .py_2()
                    .bg(rgb(RAISED))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .flex_1()
                            .child(div().text_xs().text_color(rgb(DANGER)).child(failure.summary()))
                            .child(div().text_xs().text_color(rgb(TEXT_4)).child(failure.next_step())),
                    )
                    .child(
                        Button::new("retry-kubeconfig-connection")
                            .label("Retry")
                            .compact()
                            .ghost()
                            .on_click(cx.listener(move |app, _, _, cx| {
                                if let Some(context) = retry_context.clone() {
                                    app.select_kubeconfig_context(
                                        context,
                                        retry_namespace.clone(),
                                        cx,
                                    );
                                }
                            })),
                    ),
            )
        } else {
            workspace
        };
        let workspace = workspace.child(div().flex_1().child(self.dock_area.clone()));

        if self.palette_open {
            workspace.child(self.render_command_palette(cx))
        } else if self.exec_trust_prompt.is_some() {
            workspace.child(self.render_exec_trust_prompt(cx))
        } else if self.kubeconfig_import_open {
            workspace.child(self.render_kubeconfig_import(cx))
        } else if self.kubeconfig_selection_open {
            workspace.child(self.render_kubeconfig_contexts(cx))
        } else {
            workspace
        }
    }
}

impl Drop for AxiomLensPrototype {
    fn drop(&mut self) {
        self.cancel_active_session();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PanelKind {
    Sidebar,
    Resources,
    Details,
    Activity,
    Agent,
}

impl PanelKind {
    fn label(self) -> &'static str {
        match self {
            Self::Sidebar => "Navigation",
            Self::Resources => "Resources",
            Self::Details => "Details",
            Self::Activity => "Activity",
            Self::Agent => "Agent panel",
        }
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DockTarget {
    Sidebar,
    Agent,
    Activity,
}

impl DockTarget {
    fn placement(self) -> DockPlacement {
        match self {
            Self::Sidebar => DockPlacement::Left,
            Self::Agent => DockPlacement::Right,
            Self::Activity => DockPlacement::Bottom,
        }
    }

    fn panel(self) -> PanelKind {
        match self {
            Self::Sidebar => PanelKind::Sidebar,
            Self::Agent => PanelKind::Agent,
            Self::Activity => PanelKind::Activity,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaletteAction {
    OpenResource(ResourceTab),
    SelectTab(ResourceTab),
    SwitchContext(usize),
    CloseActiveTab,
    FocusPanel(PanelKind),
    ToggleDock(DockTarget),
}

struct PaletteItem {
    group: &'static str,
    label: String,
    detail: String,
    action: PaletteAction,
    enabled: bool,
}

fn build_palette_items(
    navigation: &ResourceNavigation,
    query: &str,
    dock_open: [bool; 3],
) -> Vec<PaletteItem> {
    let query = query.trim().to_lowercase();
    let mut items = Vec::new();

    if !query.is_empty() {
        let scope = &navigation.scopes[navigation.active_context];
        let mut resource_matches = 0;
        for (resource_index, resource) in scope.resources.iter().enumerate() {
            let matches = resource.name.to_string().to_lowercase().contains(&query)
                || resource.kind.to_lowercase().contains(&query);
            if !matches {
                continue;
            }
            if resource_matches == 12 {
                break;
            }
            items.push(PaletteItem {
                group: "RESOURCES",
                label: format!("Open {} ({})", resource.name, resource.kind),
                detail: format!(
                    "{} / {} / {} / SIMULATED",
                    scope.snapshot.target, scope.snapshot.context, scope.snapshot.namespace
                ),
                action: PaletteAction::OpenResource(ResourceTab {
                    context_index: navigation.active_context,
                    resource_index,
                }),
                enabled: true,
            });
            resource_matches += 1;
        }
    }

    for tab in navigation
        .tabs
        .iter()
        .filter(|tab| Some(**tab) != navigation.active_tab)
        .take(8)
    {
        if let Some(resource) = navigation.resource_for_tab(*tab) {
            let snapshot = navigation.scopes[tab.context_index].snapshot;
            items.push(PaletteItem {
                group: "TABS",
                label: format!("Switch to {}", resource.name),
                detail: format!(
                    "{} / {} / {} / {} / SIMULATED",
                    resource.kind, snapshot.target, snapshot.context, snapshot.namespace
                ),
                action: PaletteAction::SelectTab(*tab),
                enabled: true,
            });
        }
    }

    for (context_index, scope) in navigation.scopes.iter().enumerate() {
        if context_index != navigation.active_context {
            items.push(PaletteItem {
                group: "CONTEXTS",
                label: format!("Switch to {}", scope.snapshot.context),
                detail: format!("{} / {} / SIMULATED", scope.snapshot.target, scope.snapshot.namespace),
                action: PaletteAction::SwitchContext(context_index),
                enabled: true,
            });
        }
    }

    items.push(PaletteItem {
        group: "TABS",
        label: "Close active resource tab".to_owned(),
        detail: if navigation.active_tab.is_some() {
            "Close selected tab; no cluster data is changed.".to_owned()
        } else {
            "No resource tab is open.".to_owned()
        },
        action: PaletteAction::CloseActiveTab,
        enabled: navigation.active_tab.is_some(),
    });

    for panel in [
        PanelKind::Sidebar,
        PanelKind::Resources,
        PanelKind::Details,
        PanelKind::Agent,
        PanelKind::Activity,
    ] {
        let enabled = match panel {
            PanelKind::Sidebar => dock_open[0],
            PanelKind::Agent => dock_open[1],
            PanelKind::Activity => dock_open[2],
            PanelKind::Resources | PanelKind::Details => true,
        };
        items.push(PaletteItem {
            group: "PANELS",
            label: format!("Focus {} panel", panel.label()),
            detail: if enabled {
                "Move keyboard focus to panel.".to_owned()
            } else {
                "Show this panel first.".to_owned()
            },
            action: PaletteAction::FocusPanel(panel),
            enabled,
        });
    }
    for target in [DockTarget::Sidebar, DockTarget::Agent, DockTarget::Activity] {
        let is_open = match target {
            DockTarget::Sidebar => dock_open[0],
            DockTarget::Agent => dock_open[1],
            DockTarget::Activity => dock_open[2],
        };
        let panel_label = target.panel().label();
        items.push(PaletteItem {
            group: "PANELS",
            label: format!("{} {} panel", if is_open { "Hide" } else { "Show" }, panel_label),
            detail: "Toggle local workbench panel.".to_owned(),
            action: PaletteAction::ToggleDock(target),
            enabled: true,
        });
    }

    if !query.is_empty() {
        items.retain(|item| {
            item.label.to_lowercase().contains(&query)
                || item.detail.to_lowercase().contains(&query)
                || item.group.to_lowercase().contains(&query)
        });
    }

    items
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

    #[cfg(test)]
    fn switch_context(&mut self) {
        self.switch_context_to((self.active_context + 1) % self.scopes.len());
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
    diagnostics: Option<Diagnostics>,
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
    activity_correlation_id: Option<u64>,
    worker_task: Option<Task<()>>,
    consumer_task: Option<Task<()>>,
}

impl WorkspacePanel {
    fn new(
        panel_id: &'static str,
        title: &'static str,
        kind: PanelKind,
        navigation: Entity<ResourceNavigation>,
        diagnostics: Option<Diagnostics>,
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
            diagnostics,
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
            activity_correlation_id: None,
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
        let correlation_id = next_correlation_id();
        let cancellation = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = async_channel::bounded(simulation::CHANNEL_CAPACITY);
        self.activity_events.clear();
        self.activity_status = SharedString::from(format!("RUNNING / {}", mode.label()));
        self.activity_running = true;
        self.cancellation = Some(cancellation.clone());
        self.activity_correlation_id = Some(correlation_id);
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(
                DiagnosticEvent::SimulatedTaskStarted,
                "simulation",
                Some(correlation_id),
                None,
                None,
            );
        }

        self.worker_task = Some(simulation::spawn(
            cx.background_executor().clone(),
            sender,
            cancellation,
            mode,
        ));

        let diagnostics = self.diagnostics.clone();
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
                let terminal_event = batch.iter().rev().find_map(|message| match message {
                    SimulatedTaskMessage::Finished { dropped } => {
                        Some((DiagnosticEvent::SimulatedTaskFinished, *dropped))
                    }
                    SimulatedTaskMessage::Failed { dropped } => {
                        Some((DiagnosticEvent::SimulatedTaskFailed, *dropped))
                    }
                    SimulatedTaskMessage::Cancelled { dropped } => {
                        Some((DiagnosticEvent::SimulatedTaskCancelled, *dropped))
                    }
                    SimulatedTaskMessage::Output { .. } => None,
                });
                let is_terminal = batch.iter().any(SimulatedTaskMessage::is_terminal);
                if let (Some(diagnostics), Some((event, dropped))) =
                    (&diagnostics, terminal_event)
                {
                    diagnostics.record(
                        event,
                        "simulation",
                        Some(correlation_id),
                        None,
                        Some(dropped),
                    );
                }
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
                        panel.activity_correlation_id = None;
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
            if let (Some(diagnostics), Some(correlation_id)) =
                (&self.diagnostics, self.activity_correlation_id)
            {
                diagnostics.record(
                    DiagnosticEvent::SimulatedTaskCancelRequested,
                    "simulation",
                    Some(correlation_id),
                    None,
                    None,
                );
            }
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
        let panel = match self.kind {
            PanelKind::Sidebar => render_sidebar(snapshot),
            PanelKind::Resources => self.render_resources(cx),
            PanelKind::Details => self.render_details(window, cx),
            PanelKind::Activity => self.render_activity(cx),
            PanelKind::Agent => render_agent(snapshot),
        };
        panel.track_focus(&self.focus_handle)
    }
}

fn topbar(
    snapshot: SimulatedSnapshot,
    selected_scope: Option<(&str, &str)>,
    connection_status: String,
    search_input: &Entity<InputState>,
    context_control: impl IntoElement,
    commands_button: impl IntoElement,
    theme_button: impl IntoElement,
) -> impl IntoElement {
    let (target_name, namespace_name) =
        selected_scope.unwrap_or((snapshot.target, snapshot.namespace));
    div()
        .flex()
        .items_center()
        .gap_2()
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
        .child(div().text_sm().text_color(rgb(TEXT_2)).child(target_name.to_owned()))
        .child(div().text_xs().text_color(rgb(TEXT_5)).child("/"))
        .child(context_control)
        .child(div().text_xs().text_color(rgb(TEXT_5)).child("/"))
        .child(div().text_sm().text_color(rgb(TEXT_4)).child(namespace_name.to_owned()))
        .child(div().flex_1())
        .child(
            div()
                    .w(px(240.0))
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
            .child(commands_button)
        .child(theme_button)
        .child(
            div()
                .id("connection-state")
                .child(pill(connection_status, TEXT_3, ACCENT_SOFT)),
        )
        .id("workbench-topbar")
}

fn expand_home_path(path: &str) -> PathBuf {
    if path == "~" {
        return std::env::home_dir().unwrap_or_else(|| PathBuf::from(path));
    }
    if let Some(relative) = path.strip_prefix("~/")
        && let Some(home) = std::env::home_dir()
    {
        return home.join(relative);
    }
    PathBuf::from(path)
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
    diagnostics: Option<Diagnostics>,
    diagnostics_notice: Option<String>,
    window: &mut Window,
    cx: &mut App,
) -> Workbench {
    build_workbench_with_settings(
        snapshot,
        load_settings_session(diagnostics.as_ref()),
        diagnostics,
        diagnostics_notice,
        window,
        cx,
    )
}

fn load_settings_session(diagnostics: Option<&Diagnostics>) -> SettingsSession {
    let store = match SettingsStore::for_current_user() {
        Ok(store) => store,
        Err(error) => {
            if let Some(diagnostics) = diagnostics {
                diagnostics.record(
                    DiagnosticEvent::SettingsLoadFailed,
                    "settings",
                    None,
                    Some(error.diagnostic_code()),
                    None,
                );
            }
            return SettingsSession {
                store: None,
                settings: SettingsDocument::default(),
                notice: Some(format!("Settings unavailable; using defaults: {error}")),
            };
        }
    };

    match store.load() {
        Ok(settings) => SettingsSession {
            store: Some(store),
            settings,
            notice: None,
        },
        Err(error) => {
            if let Some(diagnostics) = diagnostics {
                diagnostics.record(
                    DiagnosticEvent::SettingsLoadFailed,
                    "settings",
                    None,
                    Some(error.diagnostic_code()),
                    None,
                );
            }
            SettingsSession {
                store: None,
                settings: SettingsDocument::default(),
                notice: Some(format!("Settings could not be loaded; using defaults: {error}")),
            }
        }
    }
}

fn build_workbench_with_settings(
    snapshot: SimulatedSnapshot,
    session: SettingsSession,
    diagnostics: Option<Diagnostics>,
    diagnostics_notice: Option<String>,
    window: &mut Window,
    cx: &mut App,
) -> Workbench {
    let SettingsSession {
        store: settings_store,
        settings,
        notice: mut settings_notice,
    } = session;
    let theme_mode = theme_mode_for_appearance(settings.preferences.appearance);
    initialize_theme_palette(theme_mode);
    Theme::change(theme_mode, None, cx);

    let (dock_area, _skin) = DockSkin::dock_area("axiomlens-workspace", Some(1), window, cx);
    let navigation = cx.new(|_| ResourceNavigation::new(snapshot));
    let search_input = cx.new(|cx| InputState::new(window, cx));
    let palette_input = cx.new(|cx| InputState::new(window, cx));
    let kubeconfig_path_input = cx.new(|cx| InputState::new(window, cx));
    let namespace_override_input = cx.new(|cx| InputState::new(window, cx));

    let sidebar = WorkspacePanel::new(
        "SidebarPanel",
        "Sidebar",
        PanelKind::Sidebar,
        navigation.clone(),
        diagnostics.clone(),
        window,
        cx,
    );
    let resources = WorkspacePanel::new(
        "ResourceListPanel",
        "Resources",
        PanelKind::Resources,
        navigation.clone(),
        diagnostics.clone(),
        window,
        cx,
    );
    let details = WorkspacePanel::new(
        "ResourceDetailsPanel",
        "Resource details",
        PanelKind::Details,
        navigation.clone(),
        diagnostics.clone(),
        window,
        cx,
    );
    let agent = WorkspacePanel::new(
        "AgentPanel",
        "Agent",
        PanelKind::Agent,
        navigation.clone(),
        diagnostics.clone(),
        window,
        cx,
    );
    let activity = WorkspacePanel::new(
        "ActivityPanel",
        "Activity",
        PanelKind::Activity,
        navigation.clone(),
        diagnostics.clone(),
        window,
        cx,
    );
    let panel_focus_handles = vec![
        (PanelKind::Sidebar, sidebar.read(cx).focus_handle.clone()),
        (PanelKind::Resources, resources.read(cx).focus_handle.clone()),
        (PanelKind::Details, details.read(cx).focus_handle.clone()),
        (PanelKind::Agent, agent.read(cx).focus_handle.clone()),
        (PanelKind::Activity, activity.read(cx).focus_handle.clone()),
    ];

    let restored_layout = match settings.workspace_layout.clone() {
        None => None,
        Some(saved_layout) => match serde_json::from_value::<DockAreaState>(saved_layout) {
            Ok(state) if workspace_layout_has_all_panels(&state) => Some(state),
            Ok(_) => {
                settings_notice = Some(
                    "Saved layout is incompatible; default layout is active.".to_owned(),
                );
                None
            }
            Err(error) => {
                settings_notice = Some(format!(
                    "Saved layout is invalid; default layout is active: {error}"
                ));
                None
            }
        },
    };

    apply_workspace_layout(
        &dock_area,
        restored_layout.as_ref(),
        &sidebar,
        &resources,
        &details,
        &agent,
        &activity,
        window,
        cx,
    );

    Workbench {
        dock_area,
        navigation,
        search_input,
        palette_input,
        kubeconfig_path_input,
        namespace_override_input,
        panel_focus_handles,
        settings_store,
        settings,
        settings_notice,
        diagnostics,
        diagnostics_notice,
    }
}

fn apply_workspace_layout(
    dock_area: &Entity<DockArea>,
    saved_layout: Option<&DockAreaState>,
    sidebar: &Entity<WorkspacePanel>,
    resources: &Entity<WorkspacePanel>,
    details: &Entity<WorkspacePanel>,
    agent: &Entity<WorkspacePanel>,
    activity: &Entity<WorkspacePanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let center_sizes = saved_layout.and_then(|state| state.center.info.sizes());
    let resources_size = center_sizes.and_then(|sizes| sizes.first().copied());
    let details_size = center_sizes
        .and_then(|sizes| sizes.get(1).copied())
        .or(Some(px(340.0)));
    let left_dock = saved_layout.and_then(|state| state.left_dock.as_ref());
    let right_dock = saved_layout.and_then(|state| state.right_dock.as_ref());
    let bottom_dock = saved_layout.and_then(|state| state.bottom_dock.as_ref());

    dock_area.update(cx, |area, cx| {
        area.set_center(
            DockLayout::h_split()
                .child(
                    DockLayout::tabs().panel_view(panel_handle(resources.clone()), cx),
                    resources_size,
                )
                .child(
                    DockLayout::tabs().panel_view(panel_handle(details.clone()), cx),
                    details_size,
                ),
            window,
            cx,
        );
        area.set_dock(
            DockPlacement::Left,
            DockLayout::tabs().panel_view(panel_handle(sidebar.clone()), cx),
            window,
            cx,
        );
        area.set_dock(
            DockPlacement::Right,
            DockLayout::tabs().panel_view(panel_handle(agent.clone()), cx),
            window,
            cx,
        );
        area.set_dock(
            DockPlacement::Bottom,
            DockLayout::tabs().panel_view(panel_handle(activity.clone()), cx),
            window,
            cx,
        );
        area.set_dock_size(
            DockPlacement::Left,
            left_dock.map_or(px(248.0), |dock| dock.size()),
            window,
            cx,
        );
        area.set_dock_size(
            DockPlacement::Right,
            right_dock.map_or(px(340.0), |dock| dock.size()),
            window,
            cx,
        );
        area.set_dock_size(
            DockPlacement::Bottom,
            bottom_dock.map_or(px(220.0), |dock| dock.size()),
            window,
            cx,
        );
        for (placement, state) in [
            (DockPlacement::Left, left_dock),
            (DockPlacement::Right, right_dock),
            (DockPlacement::Bottom, bottom_dock),
        ] {
            if state.is_some_and(|dock| !dock.open()) {
                area.toggle_dock(placement, window, cx);
            }
        }
    });
}

fn workspace_layout_has_all_panels(state: &DockAreaState) -> bool {
    if state.version != Some(1)
        || !matches!(&state.center.info, PanelInfo::Stack { sizes, axis: 0 } if sizes.len() == 2 && sizes.iter().all(|size| *size > px(0.0)))
    {
        return false;
    }

    let mut panel_names = Vec::new();
    collect_workspace_panel_names(&state.center, &mut panel_names);
    for dock in [
        state.left_dock.as_ref(),
        state.right_dock.as_ref(),
        state.bottom_dock.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        collect_workspace_panel_names(dock.panel(), &mut panel_names);
    }

    let expected = [
        "SidebarPanel",
        "ResourceListPanel",
        "ResourceDetailsPanel",
        "AgentPanel",
        "ActivityPanel",
    ];
    let docks_match = [
        (state.left_dock.as_ref(), DockPlacement::Left, "SidebarPanel"),
        (state.right_dock.as_ref(), DockPlacement::Right, "AgentPanel"),
        (state.bottom_dock.as_ref(), DockPlacement::Bottom, "ActivityPanel"),
    ]
    .into_iter()
    .all(|(dock, placement, panel)| {
        dock.is_some_and(|dock| {
            let mut dock_panels = Vec::new();
            collect_workspace_panel_names(dock.panel(), &mut dock_panels);
            dock.placement() == placement && dock_panels == [panel.to_owned()]
        })
    });
    docks_match
        && state.center.children.len() == 2
        && panel_names.len() == expected.len()
        && expected
            .iter()
            .all(|name| panel_names.iter().filter(|saved| *saved == name).count() == 1)
}

fn collect_workspace_panel_names(state: &PanelState, panel_names: &mut Vec<String>) {
    if matches!(&state.info, PanelInfo::Panel(_)) {
        panel_names.push(state.panel_name.clone());
    }
    for child in &state.children {
        collect_workspace_panel_names(child, panel_names);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaletteKeyAction {
    Open,
    Close,
    MoveNext,
    MovePrevious,
    Activate,
    FocusNextPanel,
}

fn palette_key_action(
    key: &str,
    modifiers: Modifiers,
    palette_open: bool,
) -> Option<PaletteKeyAction> {
    if key.eq_ignore_ascii_case("k")
        && modifiers.secondary()
        && modifiers.number_of_modifiers() == 1
    {
        return Some(PaletteKeyAction::Open);
    }

    if palette_open && !modifiers.modified() {
        return match key.to_ascii_lowercase().as_str() {
            "escape" => Some(PaletteKeyAction::Close),
            "down" | "arrowdown" => Some(PaletteKeyAction::MoveNext),
            "up" | "arrowup" => Some(PaletteKeyAction::MovePrevious),
            "enter" => Some(PaletteKeyAction::Activate),
            _ => None,
        };
    }

    if !palette_open && key.eq_ignore_ascii_case("f6") && !modifiers.modified() {
        return Some(PaletteKeyAction::FocusNextPanel);
    }

    None
}

fn move_to_enabled_result(enabled: &[bool], current: usize, direction: isize) -> Option<usize> {
    if enabled.is_empty() || direction == 0 {
        return None;
    }

    let length = enabled.len() as isize;
    (1..=enabled.len())
        .map(|step| (current as isize + direction * step as isize).rem_euclid(length) as usize)
        .find(|index| enabled[*index])
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui_kit::{TestAppContext, test::TestWindowExt};

    fn relative_luminance(color: u32) -> f64 {
        let [_, red, green, blue] = color.to_be_bytes();
        let linear = |channel: u8| {
            let channel = f64::from(channel) / 255.0;
            if channel <= 0.04045 {
                channel / 12.92
            } else {
                ((channel + 0.055) / 1.055).powf(2.4)
            }
        };
        0.2126 * linear(red) + 0.7152 * linear(green) + 0.0722 * linear(blue)
    }

    fn contrast_ratio(foreground: u32, background: u32) -> f64 {
        let foreground = relative_luminance(foreground);
        let background = relative_luminance(background);
        (foreground.max(background) + 0.05) / (foreground.min(background) + 0.05)
    }

    fn open_test_workbench(
        cx: &mut TestAppContext,
    ) -> (gpui_kit::AnyWindowHandle, Entity<AxiomLensPrototype>) {
        cx.update(|cx| {
            gpui_kit::init(cx);
            gpui_kit::open_window(
                gpui_kit::WindowOptions::default(),
                cx,
                |window, cx| {
                    let workbench = build_workbench_with_settings(
                        crate::simulated_data::initial_snapshot(),
                        SettingsSession::default(),
                        None,
                        None,
                        window,
                        cx,
                    );
                    cx.new(|cx| AxiomLensPrototype::new(workbench, cx))
                },
            )
            .unwrap()
        })
    }

    fn open_test_workbench_with_store(
        cx: &mut TestAppContext,
        store: SettingsStore,
        settings: SettingsDocument,
    ) -> (gpui_kit::AnyWindowHandle, Entity<AxiomLensPrototype>) {
        cx.update(|cx| {
            gpui_kit::init(cx);
            gpui_kit::open_window(
                gpui_kit::WindowOptions::default(),
                cx,
                move |window, cx| {
                    let workbench = build_workbench_with_settings(
                        crate::simulated_data::initial_snapshot(),
                        SettingsSession {
                            store: Some(store),
                            settings,
                            notice: None,
                        },
                        None,
                        None,
                        window,
                        cx,
                    );
                    cx.new(|cx| AxiomLensPrototype::new(workbench, cx))
                },
            )
            .unwrap()
        })
    }

    fn open_test_workbench_with_diagnostics(
        cx: &mut TestAppContext,
        diagnostics: Diagnostics,
    ) -> (gpui_kit::AnyWindowHandle, Entity<AxiomLensPrototype>) {
        cx.update(|cx| {
            gpui_kit::init(cx);
            gpui_kit::open_window(
                gpui_kit::WindowOptions::default(),
                cx,
                move |window, cx| {
                    let workbench = build_workbench_with_settings(
                        crate::simulated_data::initial_snapshot(),
                        SettingsSession::default(),
                        Some(diagnostics),
                        None,
                        window,
                        cx,
                    );
                    cx.new(|cx| AxiomLensPrototype::new(workbench, cx))
                },
            )
            .unwrap()
        })
    }

    fn primary_modifier() -> Modifiers {
        if cfg!(target_os = "macos") {
            Modifiers {
                platform: true,
                ..Modifiers::default()
            }
        } else {
            Modifiers {
                control: true,
                ..Modifiers::default()
            }
        }
    }

    #[test]
    fn light_palette_keeps_text_and_status_contrast() {
        let pairs = [
            (TEXT_1, BG),
            (TEXT_4, PANEL),
            (TEXT_5, PANEL),
            (ACCENT, PANEL),
            (TEXT_1, ACCENT_SOFT),
            (OK, 0x14261c),
            (WARN, ELEVATED),
            (DANGER, 0x251617),
        ];

        for (foreground, background) in pairs {
            let ratio = contrast_ratio(
                light_theme_color(foreground),
                light_theme_color(background),
            );
            assert!(ratio >= 4.5, "contrast ratio {ratio:.2} for {foreground:#08x}");
        }
        assert_ne!(light_theme_color(BG), light_theme_color(PANEL));
        assert_ne!(light_theme_color(PANEL), light_theme_color(RAISED));
    }

    #[test]
    fn command_palette_shortcuts_are_contextual_and_unmodified() {
        assert_eq!(
            palette_key_action("k", primary_modifier(), false),
            Some(PaletteKeyAction::Open)
        );
        assert_eq!(
            palette_key_action("escape", Modifiers::default(), true),
            Some(PaletteKeyAction::Close)
        );
        assert_eq!(
            palette_key_action("arrowdown", Modifiers::default(), true),
            Some(PaletteKeyAction::MoveNext)
        );
        assert_eq!(
            palette_key_action("enter", Modifiers::default(), false),
            None
        );
        assert_eq!(
            palette_key_action(
                "k",
                Modifiers {
                    alt: true,
                    ..primary_modifier()
                },
                false,
            ),
            None
        );
        assert_eq!(
            palette_key_action("f6", Modifiers::default(), false),
            Some(PaletteKeyAction::FocusNextPanel)
        );
    }

    #[test]
    fn result_navigation_wraps_and_skips_disabled_entries() {
        let enabled = [true, false, true, true];
        assert_eq!(move_to_enabled_result(&enabled, 0, 1), Some(2));
        assert_eq!(move_to_enabled_result(&enabled, 2, 1), Some(3));
        assert_eq!(move_to_enabled_result(&enabled, 0, -1), Some(3));
        assert_eq!(move_to_enabled_result(&[false, false], 0, 1), None);
    }

    #[gpui_kit::test]
    fn command_palette_shortcut_and_escape_restore_focus(cx: &mut TestAppContext) {
        let (window_handle, app) = open_test_workbench(cx);
        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            let root_focus = app.read(cx).focus_handle.clone();
            window.focus(&root_focus, cx);
            window.press("secondary-k", cx);
            window.render_frame(cx);
            window.simulate_next_frame(cx);

            assert!(app.read(cx).palette_open);
            let palette_focus = app.read(cx).palette_input.read(cx).focus_handle(cx);
            assert!(palette_focus.is_focused(window));

            window.press("escape", cx);
            window.render_frame(cx);
            assert!(!app.read(cx).palette_open);
            assert!(root_focus.is_focused(window));
        })
        .unwrap();
    }

    #[gpui_kit::test]
    fn simulated_task_logs_correlated_failure_without_output_bodies(cx: &mut TestAppContext) {
        let directory = tempfile::tempdir().unwrap();
        let diagnostics = Diagnostics::in_directory(directory.path()).unwrap();
        let (window_handle, _) =
            open_test_workbench_with_diagnostics(cx, diagnostics.clone());

        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            window.click("simulate-failure", cx);
        })
        .unwrap();
        cx.run_until_parked();
        cx.update_window(window_handle, |_, window, cx| {
            window.click("toggle-theme", cx);
            assert_eq!(Theme::global(cx).mode, ThemeMode::Light);
        })
        .unwrap();
        cx.run_until_parked();
        for _ in 0..12 {
            cx.executor().advance_clock(Duration::from_millis(120));
            cx.run_until_parked();
        }
        diagnostics.flush().unwrap();

        let contents = std::fs::read_to_string(directory.path().join("axiomlens.jsonl")).unwrap();
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let started = records
            .iter()
            .find(|record| record["event"] == "simulated_task_started")
            .unwrap();
        let failed = records
            .iter()
            .find(|record| record["event"] == "simulated_task_failed")
            .unwrap();
        assert_eq!(started["module"], "simulation");
        assert_eq!(failed["module"], "simulation");
        assert_eq!(started["correlation_id"], failed["correlation_id"]);
        assert_eq!(failed["level"], "ERROR");
        assert!(!contents.contains("sample response chunk"));
        assert!(!contents.contains("sample retry"));
    }

    #[gpui_kit::test]
    fn simulated_task_logs_correlated_cancellation(cx: &mut TestAppContext) {
        let directory = tempfile::tempdir().unwrap();
        let diagnostics = Diagnostics::in_directory(directory.path()).unwrap();
        let (window_handle, _) = open_test_workbench_with_diagnostics(cx, diagnostics.clone());

        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            window.click("simulate-success", cx);
        })
        .unwrap();
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(120));
        cx.run_until_parked();
        cx.update_window(window_handle, |_, window, cx| {
            window.click("simulate-cancel", cx);
        })
        .unwrap();
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(120));
        cx.run_until_parked();
        diagnostics.flush().unwrap();

        let contents = std::fs::read_to_string(directory.path().join("axiomlens.jsonl")).unwrap();
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let correlation_for = |event| {
            records
                .iter()
                .find(|record| record["event"] == event)
                .unwrap()["correlation_id"]
                .clone()
        };
        let started = correlation_for("simulated_task_started");
        assert_eq!(started, correlation_for("simulated_task_cancel_requested"));
        assert_eq!(started, correlation_for("simulated_task_cancelled"));
        assert!(!records
            .iter()
            .any(|record| record["event"] == "simulated_task_finished"));
    }

    #[gpui_kit::test]
    fn closing_workbench_drops_delayed_worker_without_orphaned_terminal(cx: &mut TestAppContext) {
        let directory = tempfile::tempdir().unwrap();
        let diagnostics = Diagnostics::in_directory(directory.path()).unwrap();
        let (window_handle, app) =
            open_test_workbench_with_diagnostics(cx, diagnostics.clone());

        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            window.click("simulate-success", cx);
        })
        .unwrap();
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(120));
        cx.run_until_parked();
        cx.update_window(window_handle, |_, window, _| window.remove_window())
            .unwrap();
        drop(app);
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_secs(10));
        cx.run_until_parked();
        diagnostics.flush().unwrap();

        let contents = std::fs::read_to_string(directory.path().join("axiomlens.jsonl")).unwrap();
        assert!(contents.contains("simulated_task_started"));
        assert!(!contents.contains("simulated_task_finished"));
        assert!(!contents.contains("simulated_task_failed"));
        assert!(!contents.contains("simulated_task_cancelled"));
    }

    #[gpui_kit::test]
    fn command_palette_enter_opens_a_local_resource_result(cx: &mut TestAppContext) {
        let (window_handle, app) = open_test_workbench(cx);
        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            let root_focus = app.read(cx).focus_handle.clone();
            window.focus(&root_focus, cx);
            window.press("secondary-k", cx);
            window.render_frame(cx);
            window.simulate_next_frame(cx);
        })
        .unwrap();
        cx.run_until_parked();

        for character in "checkout".chars() {
            cx.update_window(window_handle, |_, window, cx| {
                window.press(&character.to_string(), cx);
            })
            .unwrap();
            cx.run_until_parked();
        }

        cx.update_window(window_handle, |_, _, cx| {
            let input_value = app.read(cx).palette_input.read(cx).value().to_string();
            let query = app.read(cx).palette_query.clone();
            assert_eq!(input_value, "checkout");
            assert_eq!(query, "checkout");
        })
        .unwrap();
        cx.run_until_parked();

        cx.update_window(window_handle, |_, window, cx| {
            window.press("enter", cx);
            window.render_frame(cx);
        })
        .unwrap();
        cx.run_until_parked();

        cx.update_window(window_handle, |_, _, cx| {
            assert!(!app.read(cx).palette_open);
            let selected_name = app
                .read(cx)
                .navigation
                .read(cx)
                .active_resource()
                .map(|resource| resource.name.to_string());
            assert_eq!(selected_name.as_deref(), Some("checkout-svc"));
        })
        .unwrap();
    }

    #[gpui_kit::test]
    fn theme_toggle_preserves_resource_and_layout_state(cx: &mut TestAppContext) {
        let (window_handle, app) = open_test_workbench(cx);
        cx.simulate_window_resize(window_handle, size(px(1280.0), px(800.0)));
        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            let navigation = app.read(cx).navigation.clone();
            let dock_area = app.read(cx).dock_area.clone();
            let initial_tab = navigation.read(cx).active_tab;
            let initial_context = navigation.read(cx).active_context;
            let initial_tabs = navigation.read(cx).tabs.clone();
            let dock_area_id = dock_area.entity_id();
            let viewport_width = window.viewport_size().width;
            let assert_topbar_controls_fit = |window: &Window| {
                for id in [
                    "resource-search",
                    "open-command-palette",
                    "toggle-theme",
                ] {
                    let bounds = window.find(id).bounds();
                    assert!(bounds.origin.x >= px(0.0), "{id} begins outside window");
                    assert!(
                        bounds.origin.x + bounds.size.width <= viewport_width,
                        "{id} is clipped at 1280px reference width"
                    );
                }
            };
            assert_topbar_controls_fit(window);

            window.click("toggle-theme", cx);
            assert_eq!(app.read(cx).theme_mode, ThemeMode::Light);
            assert_eq!(Theme::global(cx).mode, ThemeMode::Light);
            assert!(LIGHT_THEME_ACTIVE.load(Ordering::Acquire));
            assert_eq!(navigation.read(cx).active_tab, initial_tab);
            assert_eq!(navigation.read(cx).active_context, initial_context);
            assert_eq!(navigation.read(cx).tabs, initial_tabs);
            assert_eq!(dock_area.entity_id(), dock_area_id);
            window.render_frame(cx);
            assert_topbar_controls_fit(window);

            window.click("toggle-theme", cx);
            assert_eq!(app.read(cx).theme_mode, ThemeMode::Dark);
            assert_eq!(Theme::global(cx).mode, ThemeMode::Dark);
            assert!(!LIGHT_THEME_ACTIVE.load(Ordering::Acquire));
            assert_eq!(navigation.read(cx).active_tab, initial_tab);
            assert_eq!(navigation.read(cx).active_context, initial_context);
            assert_eq!(navigation.read(cx).tabs, initial_tabs);
        })
        .unwrap();
    }

    #[gpui_kit::test]
    fn saved_theme_and_dock_layout_restore_in_a_new_workbench(cx: &mut TestAppContext) {
        let directory = tempfile::tempdir().unwrap();
        let store = SettingsStore::at_path(directory.path().join("settings.json"));
        let mut initial_settings = SettingsDocument::default();
        initial_settings.preferences.default_namespace = Some("payment-platform".to_owned());
        let (first_window, first_app) =
            open_test_workbench_with_store(cx, store.clone(), initial_settings);

        cx.update_window(first_window, |_, window, cx| {
            let dock_area = first_app.read(cx).dock_area.clone();
            dock_area.update(cx, |area, cx| {
                area.set_dock_size(DockPlacement::Left, px(312.0), window, cx);
                area.toggle_dock(DockPlacement::Bottom, window, cx);
            });
            window.click("toggle-theme", cx);
        })
        .unwrap();
        cx.executor().advance_clock(Duration::from_millis(400));
        cx.run_until_parked();

        let saved = store.load().unwrap();
        assert_eq!(saved.preferences.appearance, AppearancePreference::Light);
        assert_eq!(
            saved.preferences.default_namespace.as_deref(),
            Some("payment-platform")
        );
        let layout: DockAreaState =
            serde_json::from_value(saved.workspace_layout.clone().unwrap()).unwrap();
        assert_eq!(layout.left_dock.as_ref().unwrap().size(), px(312.0));
        assert!(!layout.bottom_dock.as_ref().unwrap().open());

        let (second_window, second_app) =
            open_test_workbench_with_store(cx, store.clone(), saved);
        cx.update_window(second_window, |_, window, cx| {
            window.render_frame(cx);
            let restored_dock = second_app.read(cx).dock_area.clone();
            assert_eq!(second_app.read(cx).theme_mode, ThemeMode::Light);
            assert_eq!(
                second_app
                    .read(cx)
                    .settings
                    .preferences
                    .default_namespace
                    .as_deref(),
                Some("payment-platform")
            );
            assert_eq!(Theme::global(cx).mode, ThemeMode::Light);
            assert_eq!(restored_dock.read(cx).dock_size(DockPlacement::Left), Some(px(312.0)));
            assert!(!restored_dock.read(cx).is_dock_open(DockPlacement::Bottom));
            assert!(second_app.read(cx).settings_notice.is_none());
        })
        .unwrap();
    }

    #[gpui_kit::test]
    fn incompatible_saved_dock_layout_uses_defaults_with_visible_notice(
        cx: &mut TestAppContext,
    ) {
        let state = DockAreaState {
            version: Some(99),
            center: PanelState::new("ResourceListPanel"),
            ..DockAreaState::default()
        };
        let mut settings = SettingsDocument::default();
        settings.workspace_layout = Some(serde_json::to_value(state).unwrap());
        let (window_handle, app) = open_test_workbench_with_store(
            cx,
            SettingsStore::at_path("/tmp/axiomlens-unused-settings.json"),
            settings,
        );

        cx.update_window(window_handle, |_, _, cx| {
            let dock = app.read(cx).dock_area.clone();
            assert_eq!(dock.read(cx).dock_size(DockPlacement::Left), Some(px(248.0)));
            assert!(dock.read(cx).is_dock_open(DockPlacement::Left));
            assert!(app
                .read(cx)
                .settings_notice
                .as_ref()
                .is_some_and(|notice| notice.contains("incompatible")));
        })
        .unwrap();
    }

    #[gpui_kit::test]
    fn settings_write_failure_is_visible_and_never_reported_as_saved(
        cx: &mut TestAppContext,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let blocking_file = directory.path().join("not-a-directory");
        std::fs::write(&blocking_file, b"preserve-me").unwrap();
        let store = SettingsStore::at_path(blocking_file.join("settings.json"));
        let (window_handle, app) =
            open_test_workbench_with_store(cx, store, SettingsDocument::default());

        cx.update_window(window_handle, |_, window, cx| {
            window.click("toggle-theme", cx);
            assert!(app
                .read(cx)
                .settings_notice
                .as_ref()
                .is_some_and(|notice| notice.contains("Saving settings")));
        })
        .unwrap();
        cx.executor().advance_clock(Duration::from_millis(400));
        cx.run_until_parked();

        cx.update_window(window_handle, |_, _, cx| {
            assert!(app
                .read(cx)
                .settings_notice
                .as_ref()
                .is_some_and(|notice| notice.contains("Settings were not saved")));
            assert_eq!(std::fs::read(&blocking_file).unwrap(), b"preserve-me");
        })
        .unwrap();
    }

    #[gpui_kit::test]
    fn f6_cycles_visible_panels_and_skips_collapsed_docks(cx: &mut TestAppContext) {
        let (window_handle, app) = open_test_workbench(cx);
        cx.update_window(window_handle, |_, window, cx| {
            window.render_frame(cx);
            let dock_area = app.read(cx).dock_area.clone();
            dock_area.update(cx, |area, cx| {
                area.toggle_dock(DockPlacement::Bottom, window, cx);
            });
            let root_focus = app.read(cx).focus_handle.clone();
            window.focus(&root_focus, cx);
            window.press("f6", cx);
            assert!(app.read(cx).panel_focus_handle(PanelKind::Sidebar).unwrap().is_focused(window));
            window.press("f6", cx);
            assert!(app.read(cx).panel_focus_handle(PanelKind::Resources).unwrap().is_focused(window));
            window.press("f6", cx);
            assert!(app.read(cx).panel_focus_handle(PanelKind::Details).unwrap().is_focused(window));
            window.press("f6", cx);
            assert!(app.read(cx).panel_focus_handle(PanelKind::Agent).unwrap().is_focused(window));
            window.press("f6", cx);
            assert!(app.read(cx).panel_focus_handle(PanelKind::Sidebar).unwrap().is_focused(window));
        })
        .unwrap();
    }

    #[test]
    fn palette_registry_only_opens_current_context_resources_and_safe_navigation() {
        let navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        let items = build_palette_items(&navigation, "PAYMENT-API", [true; 3]);
        let resources = items
            .iter()
            .filter_map(|item| match item.action {
                PaletteAction::OpenResource(tab) => Some(tab),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].context_index, navigation.active_context);
        let registered_items = build_palette_items(&navigation, "", [true; 3]);
        for item in &registered_items {
            let text = format!("{} {} {}", item.group, item.label, item.detail).to_lowercase();
            for forbidden in ["write", "shell", "terminal", "restart", "execute"] {
                assert!(!text.contains(forbidden));
            }
        }
    }

    #[test]
    fn palette_reports_no_results_and_disables_close_without_open_tabs() {
        let mut navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        assert!(build_palette_items(&navigation, "no-such-command", [true; 3]).is_empty());

        while let Some(tab) = navigation.active_tab {
            navigation.close_tab(tab);
        }
        let close_item = build_palette_items(&navigation, "close active resource tab", [true; 3])
            .into_iter()
            .find(|item| item.action == PaletteAction::CloseActiveTab)
            .unwrap();
        assert!(!close_item.enabled);
    }

    #[test]
    fn palette_lists_inactive_resource_tabs_and_contexts() {
        let mut navigation = ResourceNavigation::new(crate::simulated_data::initial_snapshot());
        navigation.open_resource(1);
        navigation.switch_context();
        let items = build_palette_items(&navigation, "", [true; 3]);

        let tab = items
            .iter()
            .find_map(|item| match item.action {
                PaletteAction::SelectTab(tab)
                    if tab.context_index == 0 && tab.resource_index == 1 =>
                {
                    Some(tab)
                }
                _ => None,
            })
            .expect("palette lists inactive resource tab");
        let context_index = items.iter().find_map(|item| match item.action {
            PaletteAction::SwitchContext(index) if index == 0 => Some(index),
            _ => None,
        });

        navigation.select_tab(tab);
        assert_eq!(navigation.active_context, 0);
        assert_eq!(
            navigation.active_resource().unwrap().name.to_string(),
            "checkout-svc"
        );
        assert_eq!(context_index, Some(0));
    }

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