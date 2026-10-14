use crate::{
    connection::{ApiCapabilitySnapshot, ApiResourceCapability},
    fact_service::{FactErrorKind, FactStreamEvent, FactWatchEvent, ResourceFact},
    target::{ResourceIdentity, TargetBinding},
};
use kube::core::gvk::GroupVersionKind;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    Namespace,
    Deployment,
    StatefulSet,
    DaemonSet,
    ReplicaSet,
    Pod,
    Service,
    Ingress,
    EndpointSlice,
    Job,
    Node,
}

impl ResourceKind {
    pub const ALL: [Self; 11] = [
        Self::Namespace,
        Self::Deployment,
        Self::StatefulSet,
        Self::DaemonSet,
        Self::ReplicaSet,
        Self::Pod,
        Self::Service,
        Self::Ingress,
        Self::EndpointSlice,
        Self::Job,
        Self::Node,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Namespace => "Namespaces",
            Self::Deployment => "Deployments",
            Self::StatefulSet => "StatefulSets",
            Self::DaemonSet => "DaemonSets",
            Self::ReplicaSet => "ReplicaSets",
            Self::Pod => "Pods",
            Self::Service => "Services",
            Self::Ingress => "Ingresses",
            Self::EndpointSlice => "EndpointSlices",
            Self::Job => "Jobs",
            Self::Node => "Nodes",
        }
    }

    pub fn api_kind(self) -> &'static str {
        match self {
            Self::Namespace => "Namespace",
            Self::Deployment => "Deployment",
            Self::StatefulSet => "StatefulSet",
            Self::DaemonSet => "DaemonSet",
            Self::ReplicaSet => "ReplicaSet",
            Self::Pod => "Pod",
            Self::Service => "Service",
            Self::Ingress => "Ingress",
            Self::EndpointSlice => "EndpointSlice",
            Self::Job => "Job",
            Self::Node => "Node",
        }
    }

    pub fn api_group(self) -> &'static str {
        match self {
            Self::Namespace | Self::Pod | Self::Service | Self::Node => "",
            Self::Deployment | Self::StatefulSet | Self::DaemonSet | Self::ReplicaSet => "apps",
            Self::Ingress => "networking.k8s.io",
            Self::EndpointSlice => "discovery.k8s.io",
            Self::Job => "batch",
        }
    }

    pub fn gvk(self) -> GroupVersionKind {
        GroupVersionKind::gvk(self.api_group(), "v1", self.api_kind())
    }

    pub fn is_namespaced(self) -> bool {
        !matches!(self, Self::Namespace | Self::Node)
    }

    pub fn is_optional(self) -> bool {
        matches!(self, Self::ReplicaSet | Self::EndpointSlice)
    }

    pub fn group_label(self) -> &'static str {
        match self {
            Self::Deployment | Self::StatefulSet | Self::DaemonSet | Self::ReplicaSet | Self::Pod => {
                "WORKLOADS"
            }
            Self::Service | Self::Ingress | Self::EndpointSlice => "NETWORK",
            Self::Job => "BATCH",
            Self::Namespace | Self::Node => "CLUSTER",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceCollectionStatus {
    NotRequested,
    Loading,
    Snapshotting,
    Watching,
    ListOnly,
    Reconnecting(u8),
    Unsupported,
    DiscoveryDenied,
    ListUnsupported,
    CapabilityUnavailable,
    Failed(FactErrorKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceTargetStatus {
    Connecting,
    Connected,
    Failed,
}

impl ResourceTargetStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Connecting => "CONNECTING",
            Self::Connected => "CONNECTED",
            Self::Failed => "CONNECTION FAILED",
        }
    }
}

impl ResourceCollectionStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::NotRequested => "NOT LOADED",
            Self::Loading => "LOADING",
            Self::Snapshotting => "SYNCING",
            Self::Watching => "WATCHING",
            Self::ListOnly => "LIST ONLY",
            Self::Reconnecting(_) => "RECONNECTING",
            Self::Unsupported => "UNSUPPORTED",
            Self::DiscoveryDenied => "DISCOVERY DENIED",
            Self::ListUnsupported => "LIST UNSUPPORTED",
            Self::CapabilityUnavailable => "CAPABILITY UNKNOWN",
            Self::Failed(FactErrorKind::PermissionDenied) => "RBAC DENIED",
            Self::Failed(FactErrorKind::CredentialsRejected) => "CREDENTIALS REJECTED",
            Self::Failed(FactErrorKind::RequestTimedOut) => "TIMED OUT",
            Self::Failed(FactErrorKind::ApiUnavailable) => "API UNAVAILABLE",
            Self::Failed(FactErrorKind::ServerUnavailable) => "SERVER UNAVAILABLE",
            Self::Failed(FactErrorKind::WatchUnsupported) => "WATCH UNSUPPORTED",
            Self::Failed(_) => "READ FAILED",
        }
    }
}

pub struct ResourceBrowser {
    target: TargetBinding,
    cluster_name: String,
    context_name: String,
    namespace: Option<String>,
    target_status: ResourceTargetStatus,
    capabilities: Option<ApiCapabilitySnapshot>,
    selected_kind: ResourceKind,
    collections: HashMap<ResourceKind, ResourceCollection>,
    tabs: Vec<ResourceTab>,
    active_tab: Option<usize>,
}

impl ResourceBrowser {
    pub fn new(
        target: TargetBinding,
        cluster_name: String,
        context_name: String,
        namespace: Option<String>,
    ) -> Self {
        Self {
            target,
            cluster_name,
            context_name,
            namespace,
            target_status: ResourceTargetStatus::Connecting,
            capabilities: None,
            selected_kind: ResourceKind::Deployment,
            collections: HashMap::new(),
            tabs: Vec::new(),
            active_tab: None,
        }
    }

    pub fn target(&self) -> &TargetBinding {
        &self.target
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn context_name(&self) -> &str {
        &self.context_name
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn target_status(&self) -> ResourceTargetStatus {
        self.target_status
    }

    pub fn mark_connection_failed(&mut self) {
        self.target_status = ResourceTargetStatus::Failed;
    }

    pub fn set_capabilities(&mut self, capabilities: ApiCapabilitySnapshot) {
        self.capabilities = Some(capabilities);
        self.target_status = ResourceTargetStatus::Connected;
    }

    pub fn capabilities(&self) -> Option<&ApiCapabilitySnapshot> {
        self.capabilities.as_ref()
    }

    pub fn capability(&self, kind: ResourceKind) -> Option<&ApiResourceCapability> {
        self.capabilities
            .as_ref()?
            .resource_gvk(&kind.gvk())
    }

    pub fn selected_kind(&self) -> ResourceKind {
        self.selected_kind
    }

    pub fn select_kind(&mut self, kind: ResourceKind) {
        self.selected_kind = kind;
    }

    pub fn begin_request(&mut self, kind: ResourceKind) {
        let collection = self.collections.entry(kind).or_default();
        collection.status = ResourceCollectionStatus::Loading;
        collection.stale = !collection.facts.is_empty();
        collection.possibly_truncated = false;
        collection.begin_snapshot();
    }

    pub fn set_status(&mut self, kind: ResourceKind, status: ResourceCollectionStatus) {
        let collection = self.collections.entry(kind).or_default();
        collection.status = status;
        collection.stale = !collection.facts.is_empty();
    }

    pub fn fail(&mut self, kind: ResourceKind, error: FactErrorKind) {
        self.set_status(kind, ResourceCollectionStatus::Failed(error));
    }

    pub fn apply_stream_event(
        &mut self,
        target: &TargetBinding,
        kind: ResourceKind,
        event: FactStreamEvent,
    ) -> bool {
        if &self.target != target || self.selected_kind != kind {
            return false;
        }
        let mut actions = Vec::new();

        match event {
            FactStreamEvent::Batch(facts) => {
                let collection = self.collections.entry(kind).or_default();
                for fact in &facts {
                    if collection.snapshot_open {
                        collection
                            .snapshot_seen
                            .insert(fact.resource().identity().clone());
                    }
                    actions.push(CollectionAction::Upsert(fact.clone()));
                }
            }
            FactStreamEvent::WatchBatch(events) => {
                let collection = self.collections.entry(kind).or_default();
                for event in events {
                    match event {
                        FactWatchEvent::SnapshotStarted => {
                            collection.begin_snapshot();
                            collection.stale = !collection.facts.is_empty();
                            collection.status = ResourceCollectionStatus::Snapshotting;
                        }
                        FactWatchEvent::ResyncStarted => {
                            collection.stale = !collection.facts.is_empty();
                            collection.status = ResourceCollectionStatus::Reconnecting(0);
                        }
                        FactWatchEvent::Applied(fact) => {
                            if collection.snapshot_open {
                                collection
                                    .snapshot_seen
                                    .insert(fact.resource().identity().clone());
                            }
                            actions.push(CollectionAction::Upsert(fact));
                        }
                        FactWatchEvent::Deleted(resource) => {
                            actions.push(CollectionAction::Delete(resource.identity().clone()));
                        }
                        FactWatchEvent::SnapshotComplete { .. } => {
                            if collection.snapshot_open {
                                actions.push(CollectionAction::PruneSnapshot);
                            }
                            collection.snapshot_open = false;
                            collection.stale = false;
                            collection.status = ResourceCollectionStatus::Watching;
                        }
                        FactWatchEvent::Reconnecting { attempt, .. } => {
                            collection.stale = !collection.facts.is_empty();
                            collection.status = ResourceCollectionStatus::Reconnecting(attempt);
                        }
                    }
                }
            }
            FactStreamEvent::Complete {
                possibly_truncated: truncated,
                ..
            } => {
                let collection = self.collections.entry(kind).or_default();
                if collection.snapshot_open && !truncated {
                    actions.push(CollectionAction::PruneSnapshot);
                }
                collection.snapshot_open = false;
                collection.stale = false;
                collection.possibly_truncated = truncated;
                collection.status = ResourceCollectionStatus::ListOnly;
            }
        }

        let collection = self.collections.entry(kind).or_default();
        for action in actions {
            match action {
                CollectionAction::Upsert(fact) => {
                    let identity = fact.resource().identity().clone();
                    collection.upsert(fact.clone());
                    for tab in &mut self.tabs {
                        if tab.kind == kind && tab.identity == identity {
                            tab.fact = fact.clone();
                            tab.deleted = false;
                        }
                    }
                }
                CollectionAction::Delete(identity) => {
                    collection.remove(&identity);
                    for tab in &mut self.tabs {
                        if tab.kind == kind && tab.identity == identity {
                            tab.deleted = true;
                        }
                    }
                }
                CollectionAction::PruneSnapshot => {
                    let removed = collection.prune_unseen();
                    for identity in removed {
                        for tab in &mut self.tabs {
                            if tab.kind == kind && tab.identity == identity {
                                tab.deleted = true;
                            }
                        }
                    }
                }
            }
        }
        if matches!(collection.status, ResourceCollectionStatus::ListOnly)
            && collection.facts.is_empty()
        {
            collection.status = ResourceCollectionStatus::ListOnly;
        }
        true
    }

    pub fn status(&self, kind: ResourceKind) -> ResourceCollectionStatus {
        self.collections
            .get(&kind)
            .map_or(ResourceCollectionStatus::NotRequested, |collection| {
                collection.status
            })
    }

    pub fn is_stale(&self, kind: ResourceKind) -> bool {
        self.collections
            .get(&kind)
            .is_some_and(|collection| collection.stale)
    }

    pub fn possibly_truncated(&self, kind: ResourceKind) -> bool {
        self.collections
            .get(&kind)
            .is_some_and(|collection| collection.possibly_truncated)
    }

    pub fn facts(&self, kind: ResourceKind) -> Vec<ResourceFact> {
        self.collections
            .get(&kind)
            .map(|collection| {
                collection
                    .order
                    .iter()
                    .filter_map(|identity| collection.facts.get(identity).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn count(&self, kind: ResourceKind) -> usize {
        self.collections
            .get(&kind)
            .map_or(0, |collection| collection.facts.len())
    }

    pub fn is_selected(&self, kind: ResourceKind, identity: &ResourceIdentity) -> bool {
        self.active_tab().is_some_and(|tab| {
            tab.kind == kind && &tab.identity == identity && !tab.deleted
        })
    }

    pub fn open_resource(&mut self, kind: ResourceKind, fact: ResourceFact) {
        let identity = fact.resource().identity().clone();
        let tab_index = self
            .tabs
            .iter()
            .position(|tab| tab.kind == kind && tab.identity == identity);
        let tab_index = match tab_index {
            Some(index) => index,
            None => {
                self.tabs.push(ResourceTab {
                kind,
                identity,
                    fact: fact.clone(),
                deleted: false,
                });
                self.tabs.len() - 1
            }
        };
        let tab = &mut self.tabs[tab_index];
        tab.fact = fact;
        tab.deleted = false;
        self.active_tab = Some(tab_index);
    }

    pub fn select_tab(&mut self, index: usize) {
        if index < self.tabs.len() {
            self.active_tab = Some(index);
        }
    }

    pub fn close_tab(&mut self, index: usize) {
        if index >= self.tabs.len() {
            return;
        }
        self.tabs.remove(index);
        self.active_tab = match self.active_tab {
            Some(active) if active == index => {
                (!self.tabs.is_empty()).then_some(index.min(self.tabs.len() - 1))
            }
            Some(active) if active > index => Some(active - 1),
            active => active,
        };
    }

    pub fn tabs(&self) -> &[ResourceTab] {
        &self.tabs
    }

    pub fn active_tab_index(&self) -> Option<usize> {
        self.active_tab
    }

    pub fn active_tab(&self) -> Option<&ResourceTab> {
        self.active_tab.and_then(|index| self.tabs.get(index))
    }

    pub fn active_fact(&self) -> Option<&ResourceFact> {
        self.active_tab().map(|tab| &tab.fact)
    }

    pub fn active_is_deleted(&self) -> bool {
        self.active_tab().is_some_and(|tab| tab.deleted)
    }
}

enum CollectionAction {
    Upsert(ResourceFact),
    Delete(ResourceIdentity),
    PruneSnapshot,
}

pub struct ResourceTab {
    kind: ResourceKind,
    identity: ResourceIdentity,
    fact: ResourceFact,
    deleted: bool,
}

impl ResourceTab {
    pub fn kind(&self) -> ResourceKind {
        self.kind
    }

    pub fn identity(&self) -> &ResourceIdentity {
        &self.identity
    }

    pub fn fact(&self) -> &ResourceFact {
        &self.fact
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Default)]
struct ResourceCollection {
    facts: HashMap<ResourceIdentity, ResourceFact>,
    order: Vec<ResourceIdentity>,
    snapshot_seen: HashSet<ResourceIdentity>,
    snapshot_open: bool,
    stale: bool,
    possibly_truncated: bool,
    status: ResourceCollectionStatus,
}

impl Default for ResourceCollectionStatus {
    fn default() -> Self {
        Self::NotRequested
    }
}

impl ResourceCollection {
    fn begin_snapshot(&mut self) {
        self.snapshot_seen.clear();
        self.snapshot_open = true;
    }

    fn upsert(&mut self, fact: ResourceFact) {
        let identity = fact.resource().identity().clone();
        if !self.facts.contains_key(&identity) {
            self.order.push(identity.clone());
        }
        self.facts.insert(identity, fact);
    }

    fn remove(&mut self, identity: &ResourceIdentity) {
        self.facts.remove(identity);
        self.order.retain(|candidate| candidate != identity);
    }

    fn prune_unseen(&mut self) -> Vec<ResourceIdentity> {
        let removed = self
            .facts
            .keys()
            .filter(|identity| !self.snapshot_seen.contains(*identity))
            .cloned()
            .collect::<Vec<_>>();
        for identity in &removed {
            self.remove(identity);
        }
        self.snapshot_seen.clear();
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_and_optional_resources_map_to_expected_api_groups_and_scopes() {
        assert_eq!(ResourceKind::Namespace.api_group(), "");
        assert_eq!(ResourceKind::Deployment.api_group(), "apps");
        assert_eq!(ResourceKind::EndpointSlice.api_group(), "discovery.k8s.io");
        assert_eq!(ResourceKind::Job.api_group(), "batch");
        assert_eq!(ResourceKind::Ingress.api_group(), "networking.k8s.io");
        assert!(!ResourceKind::Node.is_namespaced());
        assert!(!ResourceKind::Namespace.is_namespaced());
        assert!(ResourceKind::EndpointSlice.is_namespaced());
        assert!(ResourceKind::ReplicaSet.is_optional());
        assert!(ResourceKind::EndpointSlice.is_optional());
        assert_eq!(ResourceKind::ALL.len(), 11);
    }

    #[test]
    fn terminal_states_preserve_distinct_permission_and_discovery_errors() {
        assert_eq!(
            ResourceCollectionStatus::DiscoveryDenied.label(),
            "DISCOVERY DENIED"
        );
        assert_eq!(
            ResourceCollectionStatus::Failed(FactErrorKind::PermissionDenied).label(),
            "RBAC DENIED"
        );
        assert_eq!(
            ResourceCollectionStatus::Failed(FactErrorKind::WatchUnsupported).label(),
            "WATCH UNSUPPORTED"
        );
    }
}