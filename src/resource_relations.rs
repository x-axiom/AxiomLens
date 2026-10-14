use crate::{
    cancellation::CancellationToken,
    connection::{ApiCapabilitySnapshot, ApiResourceDiscoveryState},
    fact_service::{
        FactBatchStream, FactErrorKind, FactStreamEvent, FactStreamReceiveError, ResourceFact,
        ReadOnlyFactService, MAX_FACTS_PER_LIST,
    },
    resource_browser::ResourceKind,
    target::ResourceIdentity,
};
use std::collections::HashSet;

const MAX_RELATION_FACTS: usize = MAX_FACTS_PER_LIST * 4;
const MAX_RELATIONS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationKind {
    OwnerReference,
    ServiceSelector,
    ServiceBackend,
    IngressBackend,
    EndpointTarget,
}

impl RelationKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::OwnerReference => "OWNER REFERENCE",
            Self::ServiceSelector => "SERVICE SELECTOR",
            Self::ServiceBackend => "SERVICE BACKEND",
            Self::IngressBackend => "INGRESS BACKEND",
            Self::EndpointTarget => "ENDPOINT TARGET",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelatedCollectionStatus {
    Loaded,
    Truncated,
    Unsupported,
    DiscoveryDenied,
    ListUnsupported,
    CapabilityUnavailable,
    Failed(FactErrorKind),
}

impl RelatedCollectionStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Loaded => "LOADED",
            Self::Truncated => "TRUNCATED",
            Self::Unsupported => "API UNSUPPORTED",
            Self::DiscoveryDenied => "DISCOVERY DENIED",
            Self::ListUnsupported => "LIST UNSUPPORTED",
            Self::CapabilityUnavailable => "CAPABILITY UNKNOWN",
            Self::Failed(FactErrorKind::PermissionDenied) => "RBAC DENIED",
            Self::Failed(FactErrorKind::CredentialsRejected) => "CREDENTIALS REJECTED",
            Self::Failed(FactErrorKind::RequestTimedOut) => "TIMED OUT",
            Self::Failed(FactErrorKind::ApiUnavailable) => "API UNAVAILABLE",
            Self::Failed(FactErrorKind::ServerUnavailable) => "SERVER UNAVAILABLE",
            Self::Failed(_) => "READ FAILED",
        }
    }
}

#[derive(Clone)]
pub struct RelatedFactCollection {
    kind: ResourceKind,
    status: RelatedCollectionStatus,
    facts: Vec<ResourceFact>,
}

impl RelatedFactCollection {
    pub fn kind(&self) -> ResourceKind {
        self.kind
    }

    pub fn status(&self) -> RelatedCollectionStatus {
        self.status
    }

    pub fn facts(&self) -> &[ResourceFact] {
        &self.facts
    }
}

#[derive(Clone)]
pub struct ResourceRelation {
    kind: RelationKind,
    source: ResourceFact,
    target: ResourceFact,
    evidence: String,
}

impl ResourceRelation {
    pub fn kind(&self) -> RelationKind {
        self.kind
    }

    pub fn source(&self) -> &ResourceFact {
        &self.source
    }

    pub fn target(&self) -> &ResourceFact {
        &self.target
    }

    pub fn evidence(&self) -> &str {
        &self.evidence
    }
}

#[derive(Clone)]
pub struct ResourceRelationSnapshot {
    target: crate::target::TargetBinding,
    selected_identity: ResourceIdentity,
    selected_resource_version: Option<String>,
    collections: Vec<RelatedFactCollection>,
    relations: Vec<ResourceRelation>,
}

impl ResourceRelationSnapshot {
    pub fn selected_identity(&self) -> &ResourceIdentity {
        &self.selected_identity
    }

    pub fn matches_selection(&self, selected: &ResourceFact) -> bool {
        self.target == *selected.resource().target()
            && self.selected_identity == *selected.resource().identity()
            && self.selected_resource_version
                == selected.resource().resource_version().map(str::to_owned)
    }

    pub fn collections(&self) -> &[RelatedFactCollection] {
        &self.collections
    }

    pub fn relations(&self) -> &[ResourceRelation] {
        &self.relations
    }
}

pub fn related_kinds(
    selected: &ResourceFact,
    capabilities: &ApiCapabilitySnapshot,
) -> Vec<ResourceKind> {
    let identity = selected.resource().identity();
    if identity.namespace().is_none() {
        return Vec::new();
    }
    let mut kinds = Vec::new();
    if let Some(owner_references) = &selected.object().metadata.owner_references {
        for owner in owner_references {
            if let Some(kind) = resource_kind_for_owner(&owner.api_version, &owner.kind) {
                push_unique(&mut kinds, kind);
            }
        }
    }
    match identity.kind() {
        "Deployment" => {
            push_unique(&mut kinds, ResourceKind::ReplicaSet);
            push_unique(&mut kinds, ResourceKind::Pod);
        }
        "StatefulSet" | "DaemonSet" | "ReplicaSet" | "Job" => {
            push_unique(&mut kinds, ResourceKind::Pod);
            if identity.kind() == "ReplicaSet" {
                push_unique(&mut kinds, ResourceKind::Deployment);
            }
        }
        "Pod" => {
            push_unique(&mut kinds, ResourceKind::Service);
            push_unique(&mut kinds, endpoint_compatibility_kind(capabilities));
        }
        "Service" => {
            push_unique(&mut kinds, ResourceKind::Pod);
            push_unique(&mut kinds, endpoint_compatibility_kind(capabilities));
            push_unique(&mut kinds, ResourceKind::Ingress);
        }
        "Ingress" => push_unique(&mut kinds, ResourceKind::Service),
        "EndpointSlice" | "Endpoints" => {
            push_unique(&mut kinds, ResourceKind::Service);
            push_unique(&mut kinds, ResourceKind::Pod);
        }
        _ => {}
    }
    kinds
}

pub async fn collect_related_facts(
    service: &ReadOnlyFactService,
    capabilities: &ApiCapabilitySnapshot,
    selected: &ResourceFact,
    cancellation: CancellationToken,
) -> ResourceRelationSnapshot {
    let selected_identity = selected.resource().identity().clone();
    let kinds = related_kinds(selected, capabilities);
    let mut collections = Vec::with_capacity(kinds.len());
    let mut collected_count = 0_usize;

    for kind in kinds {
        if cancellation.is_cancelled() || collected_count >= MAX_RELATION_FACTS {
            break;
        }
        let capability = capabilities.resource_gvk(&kind.gvk());
        let capability_status = match capability.map(|capability| capability.state()) {
            None | Some(ApiResourceDiscoveryState::Unavailable | ApiResourceDiscoveryState::TimedOut) => {
                Some(RelatedCollectionStatus::CapabilityUnavailable)
            }
            Some(ApiResourceDiscoveryState::Unsupported) => {
                Some(RelatedCollectionStatus::Unsupported)
            }
            Some(ApiResourceDiscoveryState::DiscoveryDenied) => {
                Some(RelatedCollectionStatus::DiscoveryDenied)
            }
            Some(ApiResourceDiscoveryState::Served {
                list_supported: false,
                ..
            }) => Some(RelatedCollectionStatus::ListUnsupported),
            Some(ApiResourceDiscoveryState::Served {
                list_supported: true,
                ..
            }) => None,
        };
        if let Some(status) = capability_status {
            collections.push(empty_collection(kind, status));
            continue;
        }
        let namespace = kind
            .is_namespaced()
            .then(|| identity_namespace(&selected_identity).map(str::to_owned))
            .flatten();
        let stream = match service.list_stream(
            kind.gvk(),
            namespace,
            MAX_FACTS_PER_LIST.min(MAX_RELATION_FACTS - collected_count),
        ) {
            Ok(stream) => stream,
            Err(error) => {
                collections.push(empty_collection(
                    kind,
                    collection_status_for_error(error.kind()),
                ));
                continue;
            }
        };
        collections.push(collect_list_stream(kind, stream, &cancellation).await);
        collected_count = collections
            .iter()
            .map(|collection| collection.facts.len())
            .sum();
    }

    let relations = resolve_relations(selected, &collections);
    ResourceRelationSnapshot {
        target: selected.resource().target().clone(),
        selected_identity,
        selected_resource_version: selected.resource().resource_version().map(str::to_owned),
        collections,
        relations,
    }
}

pub fn resolve_relations(
    selected: &ResourceFact,
    collections: &[RelatedFactCollection],
) -> Vec<ResourceRelation> {
    let selected_identity = selected.resource().identity();
    let mut facts = vec![selected.clone()];
    for collection in collections {
        if matches!(
            collection.status,
            RelatedCollectionStatus::Loaded | RelatedCollectionStatus::Truncated
        ) {
            facts.extend(collection.facts.iter().cloned());
        }
    }
    facts.retain(|fact| fact.resource().target() == selected.resource().target());
    let mut seen = HashSet::new();
    facts.retain(|fact| seen.insert(fact.resource().identity().clone()));

    let mut relations = Vec::new();
    for child in &facts {
        let Some(owner_references) = &child.object().metadata.owner_references else {
            continue;
        };
        for owner in owner_references {
            let owner_group = owner
                .api_version
                .split_once('/')
                .map_or("", |(group, _version)| group);
            let Some(parent) = facts.iter().find(|candidate| {
                candidate.resource().identity().uid() == owner.uid
                    && candidate.resource().identity().kind() == owner.kind
                    && candidate.resource().identity().api_group() == owner_group
                    && same_namespace(candidate, child)
            }) else {
                continue;
            };
            push_relation(
                &mut relations,
                RelationKind::OwnerReference,
                parent,
                child,
                format!(
                    "ownerReference UID {} / controller {}",
                    owner.uid,
                    owner.controller.unwrap_or(false)
                ),
            );
        }
    }

    for service in facts.iter().filter(|fact| fact.resource().identity().kind() == "Service") {
        for pod in facts.iter().filter(|fact| fact.resource().identity().kind() == "Pod") {
            if let Some(labels) = matching_selector_labels(service, pod)
            {
                push_relation(
                    &mut relations,
                    RelationKind::ServiceSelector,
                    service,
                    pod,
                    format!("selector matched labels: {}", labels.join(", ")),
                );
            }
        }

        for backend in facts.iter().filter(|fact| {
            matches!(fact.resource().identity().kind(), "EndpointSlice" | "Endpoints")
        }) {
            if backend_service_name(backend) == Some(service.resource().identity().name())
                && same_namespace(service, backend)
            {
                push_relation(
                    &mut relations,
                    RelationKind::ServiceBackend,
                    service,
                    backend,
                    "service name / kubernetes.io/service-name".to_owned(),
                );
            }
        }
    }

    for ingress in facts.iter().filter(|fact| fact.resource().identity().kind() == "Ingress") {
        for service in facts.iter().filter(|fact| fact.resource().identity().kind() == "Service") {
            if !same_namespace(ingress, service) {
                continue;
            }
            if let Some(evidence) = ingress_backend_evidence(
                ingress,
                service.resource().identity().name(),
            ) {
                push_relation(
                    &mut relations,
                    RelationKind::IngressBackend,
                    ingress,
                    service,
                    evidence,
                );
            }
        }
    }

    for endpoint in facts.iter().filter(|fact| {
        matches!(fact.resource().identity().kind(), "EndpointSlice" | "Endpoints")
    }) {
        for pod in facts.iter().filter(|fact| fact.resource().identity().kind() == "Pod") {
            if endpoint_target_uids(endpoint)
                    .iter()
                    .any(|uid| uid == pod.resource().identity().uid())
                && same_namespace(endpoint, pod)
            {
                push_relation(
                    &mut relations,
                    RelationKind::EndpointTarget,
                    endpoint,
                    pod,
                    format!("targetRef UID {}", pod.resource().identity().uid()),
                );
            }
        }
    }

    let mut connected = HashSet::from([selected_identity.clone()]);
    loop {
        let previous_count = connected.len();
        for relation in &relations {
            let source = relation.source.resource().identity();
            let target = relation.target.resource().identity();
            if connected.contains(source) {
                connected.insert(target.clone());
            }
            if connected.contains(target) {
                connected.insert(source.clone());
            }
        }
        if connected.len() == previous_count {
            break;
        }
    }
    relations.retain(|relation| {
        connected.contains(relation.source.resource().identity())
            && connected.contains(relation.target.resource().identity())
    });
    relations.truncate(MAX_RELATIONS);
    relations
}

fn collect_list_stream<'a>(
    kind: ResourceKind,
    stream: FactBatchStream,
    cancellation: &'a CancellationToken,
) -> impl std::future::Future<Output = RelatedFactCollection> + 'a {
    async move {
        let request_cancellation = stream.cancellation_token();
        let mut facts = Vec::new();
        loop {
            let received = tokio::select! {
                _ = cancellation.cancelled() => {
                    request_cancellation.cancel();
                    return RelatedFactCollection {
                        kind,
                        status: RelatedCollectionStatus::Failed(FactErrorKind::SessionCancelled),
                        facts,
                    };
                }
                received = stream.recv() => received,
            };
            match received {
                Ok(FactStreamEvent::Batch(batch)) => {
                    facts.extend(batch.into_iter().take(MAX_RELATION_FACTS - facts.len()));
                }
                Ok(FactStreamEvent::Complete {
                    possibly_truncated,
                    ..
                }) => {
                    return RelatedFactCollection {
                        kind,
                        status: if possibly_truncated {
                            RelatedCollectionStatus::Truncated
                        } else {
                            RelatedCollectionStatus::Loaded
                        },
                        facts,
                    };
                }
                Ok(FactStreamEvent::WatchBatch(_)) => {
                    return RelatedFactCollection {
                        kind,
                        status: RelatedCollectionStatus::Failed(FactErrorKind::Internal),
                        facts,
                    };
                }
                Err(FactStreamReceiveError::Service(error)) => {
                    return RelatedFactCollection {
                        kind,
                        status: collection_status_for_error(error.kind()),
                        facts,
                    };
                }
                Err(FactStreamReceiveError::Closed) => {
                    return RelatedFactCollection {
                        kind,
                        status: RelatedCollectionStatus::Failed(FactErrorKind::Internal),
                        facts,
                    };
                }
            }
        }
    }
}

fn empty_collection(kind: ResourceKind, status: RelatedCollectionStatus) -> RelatedFactCollection {
    RelatedFactCollection {
        kind,
        status,
        facts: Vec::new(),
    }
}

fn collection_status_for_error(error: FactErrorKind) -> RelatedCollectionStatus {
    match error {
        FactErrorKind::UnsupportedResource => RelatedCollectionStatus::Unsupported,
        FactErrorKind::DiscoveryDenied => RelatedCollectionStatus::DiscoveryDenied,
        FactErrorKind::ListNotSupported => RelatedCollectionStatus::ListUnsupported,
        FactErrorKind::CapabilityUnavailable => RelatedCollectionStatus::CapabilityUnavailable,
        other => RelatedCollectionStatus::Failed(other),
    }
}

fn endpoint_compatibility_kind(capabilities: &ApiCapabilitySnapshot) -> ResourceKind {
    let endpoint_slice = capabilities.resource_gvk(&ResourceKind::EndpointSlice.gvk());
    match endpoint_slice.map(|capability| capability.state()) {
        Some(ApiResourceDiscoveryState::Unsupported)
        | Some(ApiResourceDiscoveryState::Served {
            list_supported: false,
            ..
        }) => ResourceKind::Endpoints,
        _ => ResourceKind::EndpointSlice,
    }
}

fn resource_kind_for_owner(api_version: &str, kind: &str) -> Option<ResourceKind> {
    let group = api_version.split_once('/').map_or("", |(group, _)| group);
    match (group, kind) {
        ("", "Namespace") => Some(ResourceKind::Namespace),
        ("", "Node") => Some(ResourceKind::Node),
        ("", "Pod") => Some(ResourceKind::Pod),
        ("", "Service") => Some(ResourceKind::Service),
        ("", "Endpoints") => Some(ResourceKind::Endpoints),
        ("apps", "Deployment") => Some(ResourceKind::Deployment),
        ("apps", "StatefulSet") => Some(ResourceKind::StatefulSet),
        ("apps", "DaemonSet") => Some(ResourceKind::DaemonSet),
        ("apps", "ReplicaSet") => Some(ResourceKind::ReplicaSet),
        ("batch", "Job") => Some(ResourceKind::Job),
        ("networking.k8s.io", "Ingress") => Some(ResourceKind::Ingress),
        ("discovery.k8s.io", "EndpointSlice") => Some(ResourceKind::EndpointSlice),
        _ => None,
    }
}

fn push_unique(kinds: &mut Vec<ResourceKind>, kind: ResourceKind) {
    if !kinds.contains(&kind) {
        kinds.push(kind);
    }
}

fn identity_namespace(identity: &ResourceIdentity) -> Option<&str> {
    identity.namespace().map(|namespace| namespace.as_str())
}

fn same_namespace(left: &ResourceFact, right: &ResourceFact) -> bool {
    identity_namespace(left.resource().identity()) == identity_namespace(right.resource().identity())
}

fn push_relation(
    relations: &mut Vec<ResourceRelation>,
    kind: RelationKind,
    source: &ResourceFact,
    target: &ResourceFact,
    evidence: String,
) {
    if relations.iter().any(|relation| {
        relation.kind == kind
            && relation.source.resource().identity() == source.resource().identity()
            && relation.target.resource().identity() == target.resource().identity()
    }) {
        return;
    }
    if relations.len() < MAX_RELATIONS {
        relations.push(ResourceRelation {
            kind,
            source: source.clone(),
            target: target.clone(),
            evidence,
        });
    }
}

fn matching_selector_labels(service: &ResourceFact, pod: &ResourceFact) -> Option<Vec<String>> {
    if service.resource().identity().kind() != "Service"
        || pod.resource().identity().kind() != "Pod"
        || !same_namespace(service, pod)
    {
        return None;
    }
    let selector = service
        .object()
        .data
        .get("spec")?
        .get("selector")?
        .as_object()?;
    let labels = pod.object().metadata.labels.as_ref()?;
    if selector.is_empty()
        || !selector.iter().all(|(key, value)| {
            value
                .as_str()
                .is_some_and(|value| labels.get(key).is_some_and(|label| label == value))
        })
    {
        return None;
    }
    Some(
        selector
            .iter()
            .filter_map(|(key, value)| {
                value.as_str().map(|value| format!("{key}={value}"))
            })
            .collect(),
    )
}

fn backend_service_name(fact: &ResourceFact) -> Option<&str> {
    match fact.resource().identity().kind() {
        "EndpointSlice" => fact
            .object()
            .metadata
            .labels
            .as_ref()?
            .get("kubernetes.io/service-name")
            .map(String::as_str),
        "Endpoints" => Some(fact.resource().identity().name()),
        _ => None,
    }
}

fn endpoint_target_uids(fact: &ResourceFact) -> Vec<String> {
    let data = &fact.object().data;
    let target_refs = match fact.resource().identity().kind() {
        "EndpointSlice" => data
            .get("endpoints")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|endpoint| endpoint.get("targetRef"))
            .collect::<Vec<_>>(),
        "Endpoints" => data
            .get("subsets")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(|subset| {
                ["addresses", "notReadyAddresses"]
                    .into_iter()
                    .filter_map(move |key| subset.get(key).and_then(serde_json::Value::as_array))
                    .flatten()
                    .filter_map(|address| address.get("targetRef"))
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    target_refs
        .into_iter()
        .filter(|target| target.get("kind").and_then(serde_json::Value::as_str) == Some("Pod"))
        .filter_map(|target| target.get("uid").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn ingress_backend_evidence(ingress: &ResourceFact, service_name: &str) -> Option<String> {
    let spec = ingress.object().data.get("spec").unwrap_or(&serde_json::Value::Null);
    if spec
        .get("defaultBackend")
        .and_then(|backend| backend.get("service"))
        .and_then(|service| service.get("name"))
        .and_then(serde_json::Value::as_str)
        == Some(service_name)
    {
        return Some(format!("defaultBackend -> Service {service_name}"));
    }
    for rule in spec
        .get("rules")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let host = rule
            .get("host")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("*");
        for path in rule
            .get("http")
            .and_then(|http| http.get("paths"))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let backend_name = path
                .get("backend")
                .and_then(|backend| backend.get("service"))
                .and_then(|service| service.get("name"))
                .and_then(serde_json::Value::as_str);
            if backend_name == Some(service_name) {
                let route_path = path
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("/");
                let path_type = path
                    .get("pathType")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("UNKNOWN");
                return Some(format!(
                    "host {host} / path {route_path} ({path_type}) -> Service {service_name}"
                ));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn owner_kind_mapping_respects_api_group() {
        assert_eq!(
            resource_kind_for_owner("apps/v1", "ReplicaSet"),
            Some(ResourceKind::ReplicaSet)
        );
        assert_eq!(
            resource_kind_for_owner("batch/v1", "Job"),
            Some(ResourceKind::Job)
        );
        assert_eq!(resource_kind_for_owner("v1", "Unknown"), None);
    }

    #[test]
    fn ingress_backend_extraction_keeps_only_service_names() {
        let names = ingress_backend_names_from_object(&json!({
            "defaultBackend": { "service": { "name": "fallback" } },
            "rules": [{
                "http": { "paths": [
                    { "backend": { "service": { "name": "api" } } },
                    { "backend": { "service": { "name": "api" } } }
                ] }
            }]
        }));
        assert_eq!(names, ["api", "fallback"]);
    }

    #[test]
    fn selector_match_requires_every_label_and_nonempty_selector() {
        let selector = json!({"app":"api","tier":"backend"});
        let matching_labels = json!({"app":"api","tier":"backend","track":"stable"});
        let incomplete_labels = json!({"app":"api"});
        assert_eq!(matching_selector(&selector, &matching_labels), Some(vec![
            "app=api".to_owned(),
            "tier=backend".to_owned(),
        ]));
        assert_eq!(matching_selector(&selector, &incomplete_labels), None);
        assert_eq!(matching_selector(&json!({}), &matching_labels), None);
    }
}

#[cfg(test)]
fn matching_selector(selector: &serde_json::Value, labels: &serde_json::Value) -> Option<Vec<String>> {
    let selector = selector.as_object()?;
    let labels = labels.as_object()?;
    if selector.is_empty()
        || !selector.iter().all(|(key, value)| {
            value.as_str().is_some_and(|value| {
                labels.get(key).and_then(serde_json::Value::as_str) == Some(value)
            })
        })
    {
        return None;
    }
    Some(
        selector
            .iter()
            .filter_map(|(key, value)| value.as_str().map(|value| format!("{key}={value}")))
            .collect(),
    )
}

#[cfg(test)]
fn ingress_backend_names_from_object(spec: &serde_json::Value) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(name) = spec
        .get("defaultBackend")
        .and_then(|backend| backend.get("service"))
        .and_then(|service| service.get("name"))
        .and_then(serde_json::Value::as_str)
    {
        names.push(name.to_owned());
    }
    for name in spec
        .get("rules")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|rule| rule.get("http").and_then(|http| http.get("paths")))
        .filter_map(serde_json::Value::as_array)
        .flatten()
        .filter_map(|path| path.get("backend"))
        .filter_map(|backend| backend.get("service"))
        .filter_map(|service| service.get("name"))
        .filter_map(serde_json::Value::as_str)
    {
        names.push(name.to_owned());
    }
    names.sort();
    names.dedup();
    names
}