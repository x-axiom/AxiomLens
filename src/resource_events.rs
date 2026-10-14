use crate::{
    fact_service::{FactErrorKind, ResourceFact},
    target::{ResourceIdentity, TargetBinding},
};
use jiff::Timestamp;
use kube::api::DynamicObject;
use serde_json::Value;
use std::collections::HashMap;

pub const MAX_CACHED_EVENTS: usize = 10_000;
pub const MAX_EVENT_CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAX_EVENT_TEXT_CHARS: usize = 2048;
const EVENT_CACHE_TTL_SECONDS: i64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventWindow {
    LastHour,
    LastDay,
    LastWeek,
    All,
}

impl EventWindow {
    pub fn label(self) -> &'static str {
        match self {
            Self::LastHour => "1 HOUR",
            Self::LastDay => "24 HOURS",
            Self::LastWeek => "7 DAYS",
            Self::All => "ALL CACHED",
        }
    }

    fn cutoff(self, now_epoch_seconds: i64) -> Option<i64> {
        match self {
            Self::LastHour => Some(now_epoch_seconds.saturating_sub(60 * 60)),
            Self::LastDay => Some(now_epoch_seconds.saturating_sub(24 * 60 * 60)),
            Self::LastWeek => Some(now_epoch_seconds.saturating_sub(7 * 24 * 60 * 60)),
            Self::All => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCollectionStatus {
    NotRequested,
    Loading,
    Loaded,
    Empty,
    Truncated,
    Unsupported,
    DiscoveryDenied,
    ListUnsupported,
    CapabilityUnavailable,
    Failed(FactErrorKind),
}

impl EventCollectionStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::NotRequested => "NOT LOADED",
            Self::Loading => "LOADING",
            Self::Loaded => "LOADED",
            Self::Empty => "NO EVENTS",
            Self::Truncated => "TRUNCATED",
            Self::Unsupported => "EVENT API UNSUPPORTED",
            Self::DiscoveryDenied => "DISCOVERY DENIED",
            Self::ListUnsupported => "LIST UNSUPPORTED",
            Self::CapabilityUnavailable => "CAPABILITY UNKNOWN",
            Self::Failed(FactErrorKind::PermissionDenied) => "RBAC DENIED",
            Self::Failed(FactErrorKind::CredentialsRejected) => "CREDENTIALS REJECTED",
            Self::Failed(FactErrorKind::RequestTimedOut) => "TIMED OUT",
            Self::Failed(FactErrorKind::ApiUnavailable) => "API UNAVAILABLE",
            Self::Failed(_) => "READ FAILED",
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum EventAssociation {
    Uid,
    NameFallback,
}

impl EventAssociation {
    pub fn label(self) -> &'static str {
        match self {
            Self::Uid => "UID MATCH",
            Self::NameFallback => "NAME ONLY / UID MISSING",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOccurrence {
    pub event_name: String,
    pub event_uid: String,
    pub namespace: String,
    pub object_api_group: Option<String>,
    pub object_api_version: Option<String>,
    pub object_kind: String,
    pub object_name: String,
    pub object_uid: Option<String>,
    pub event_type: String,
    pub reason: String,
    pub message: String,
    pub first_timestamp: Option<String>,
    pub latest_timestamp: Option<String>,
    latest_epoch_seconds: Option<i64>,
    pub count: u64,
    estimated_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociatedEvent {
    pub namespace: String,
    pub object_kind: String,
    pub object_name: String,
    pub object_uid: Option<String>,
    pub association: EventAssociation,
    pub event_type: String,
    pub reason: String,
    pub message: String,
    pub first_timestamp: Option<String>,
    pub latest_timestamp: Option<String>,
    latest_epoch_seconds: Option<i64>,
    pub count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventCacheState {
    pub status: EventCollectionStatus,
    pub count: usize,
    pub stale: bool,
    pub truncated: bool,
}

#[derive(Clone)]
pub struct ResourceEventCache {
    target: TargetBinding,
    namespace: String,
    records: BoundedEventRecords,
    pending_records: Option<BoundedEventRecords>,
    status: EventCollectionStatus,
    stale: bool,
    truncated: bool,
    collected_at_epoch_seconds: Option<i64>,
}

impl ResourceEventCache {
    pub fn new(target: TargetBinding, namespace: impl Into<String>) -> Self {
        Self {
            target,
            namespace: namespace.into(),
            records: BoundedEventRecords::default(),
            pending_records: None,
            status: EventCollectionStatus::NotRequested,
            stale: false,
            truncated: false,
            collected_at_epoch_seconds: None,
        }
    }

    pub fn begin_query(&mut self) {
        self.status = EventCollectionStatus::Loading;
        self.stale = !self.records.is_empty();
        self.truncated = false;
        self.pending_records = Some(BoundedEventRecords::default());
    }

    pub fn ingest_fact(&mut self, fact: &ResourceFact) -> bool {
        if fact.resource().target() != &self.target
            || fact.resource().identity().kind() != "Event"
            || fact
                .resource()
                .identity()
                .namespace()
                .is_none_or(|namespace| namespace.as_str() != self.namespace)
        {
            return false;
        }
        let Some(record) = parse_event_object(fact.object(), &self.namespace) else {
            return false;
        };
        self.pending_records
            .get_or_insert_with(BoundedEventRecords::default)
            .insert(record)
    }

    pub fn finish_query(&mut self, truncated: bool) {
        if let Some(pending) = self.pending_records.take() {
            self.records = pending;
        }
        self.truncated |= truncated;
        self.truncated |= self.records.truncated;
        self.stale = false;
        self.collected_at_epoch_seconds = Some(Timestamp::now().as_second());
        self.status = if self.truncated {
            EventCollectionStatus::Truncated
        } else if self.records.is_empty() {
            EventCollectionStatus::Empty
        } else {
            EventCollectionStatus::Loaded
        };
    }

    pub fn mark_failed(&mut self, error: FactErrorKind) {
        self.pending_records = None;
        self.stale = !self.records.is_empty();
        self.status = EventCollectionStatus::Failed(error);
    }

    pub fn mark_status(&mut self, status: EventCollectionStatus) {
        self.pending_records = None;
        self.stale = !self.records.is_empty();
        self.status = status;
    }

    pub fn mark_stale(&mut self) {
        self.stale = !self.records.is_empty();
    }

    pub fn state(&self, now_epoch_seconds: i64) -> EventCacheState {
        EventCacheState {
            status: self.status,
            count: self.records.len(),
            stale: self.stale
                || self.collected_at_epoch_seconds.is_some_and(|collected| {
                    now_epoch_seconds.saturating_sub(collected) > EVENT_CACHE_TTL_SECONDS
                }),
            truncated: self.truncated,
        }
    }

    pub fn associated_events(
        &self,
        selected: &ResourceFact,
        window: EventWindow,
        now_epoch_seconds: i64,
    ) -> Vec<AssociatedEvent> {
        if selected.resource().target() != &self.target
            || selected
                .resource()
                .identity()
                .namespace()
                .is_none_or(|namespace| namespace.as_str() != self.namespace)
        {
            return Vec::new();
        }
        aggregate_events(
            self.records.values(),
            selected.resource().identity(),
            window,
            now_epoch_seconds,
        )
    }
}

fn aggregate_events<'a>(
    records: impl IntoIterator<Item = &'a EventOccurrence>,
    selected_identity: &ResourceIdentity,
    window: EventWindow,
    now_epoch_seconds: i64,
) -> Vec<AssociatedEvent> {
    let cutoff = window.cutoff(now_epoch_seconds);
    let mut grouped = HashMap::<EventGroupKey, EventAccumulator>::new();
    for record in records {
        let Some(association) = record.association_for(selected_identity) else {
            continue;
        };
        if cutoff.is_some_and(|cutoff| {
            record
                .latest_epoch_seconds
                .is_none_or(|timestamp| timestamp < cutoff)
        }) {
            continue;
        }
        let key = EventGroupKey::from_record(record, association);
        grouped
            .entry(key)
            .and_modify(|accumulator| accumulator.merge(record))
            .or_insert_with(|| EventAccumulator::from_record(record));
    }
    let mut events = grouped
        .into_iter()
        .map(|(key, accumulator)| accumulator.into_summary(key))
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        right
            .latest_epoch_seconds
            .cmp(&left.latest_epoch_seconds)
            .then_with(|| left.reason.cmp(&right.reason))
            .then_with(|| left.object_name.cmp(&right.object_name))
    });
    events
}

#[derive(Clone, Default)]
struct BoundedEventRecords {
    records: HashMap<String, EventOccurrence>,
    current_bytes: usize,
    truncated: bool,
}

impl BoundedEventRecords {
    fn len(&self) -> usize {
        self.records.len()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn values(&self) -> impl Iterator<Item = &EventOccurrence> {
        self.records.values()
    }

    fn insert(&mut self, record: EventOccurrence) -> bool {
        let old_bytes = self
            .records
            .get(&record.event_uid)
            .map_or(0, |existing| existing.estimated_bytes);
        let next_bytes = self
            .current_bytes
            .saturating_sub(old_bytes)
            .saturating_add(record.estimated_bytes);
        if (!self.records.contains_key(&record.event_uid)
            && self.records.len() >= MAX_CACHED_EVENTS)
            || next_bytes > MAX_EVENT_CACHE_BYTES
        {
            self.truncated = true;
            return false;
        }
        self.current_bytes = next_bytes;
        self.records.insert(record.event_uid.clone(), record);
        true
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct EventGroupKey {
    namespace: String,
    object_kind: String,
    object_name: String,
    object_uid: Option<String>,
    association: EventAssociation,
    event_type: String,
    reason: String,
    message: String,
}

impl EventGroupKey {
    fn from_record(record: &EventOccurrence, association: EventAssociation) -> Self {
        Self {
            namespace: record.namespace.clone(),
            object_kind: record.object_kind.clone(),
            object_name: record.object_name.clone(),
            object_uid: record.object_uid.clone(),
            association,
            event_type: record.event_type.clone(),
            reason: record.reason.clone(),
            message: record.message.clone(),
        }
    }
}

struct EventAccumulator {
    count: u64,
    first_timestamp: Option<String>,
    first_epoch_seconds: Option<i64>,
    latest_timestamp: Option<String>,
    latest_epoch_seconds: Option<i64>,
}

impl EventAccumulator {
    fn from_record(record: &EventOccurrence) -> Self {
        Self {
            count: record.count,
            first_timestamp: record.first_timestamp.clone(),
            first_epoch_seconds: record.first_timestamp.as_deref().and_then(parse_epoch_seconds),
            latest_timestamp: record.latest_timestamp.clone(),
            latest_epoch_seconds: record.latest_epoch_seconds,
        }
    }

    fn merge(&mut self, record: &EventOccurrence) {
        self.count = self.count.saturating_add(record.count);
        if let Some(timestamp) = record.first_timestamp.as_deref().and_then(parse_epoch_seconds) {
            if self.first_epoch_seconds.is_none_or(|first| timestamp < first) {
                self.first_epoch_seconds = Some(timestamp);
                self.first_timestamp = record.first_timestamp.clone();
            }
        }
        if let Some(timestamp) = record.latest_epoch_seconds {
            if self.latest_epoch_seconds.is_none_or(|latest| timestamp > latest) {
                self.latest_epoch_seconds = Some(timestamp);
                self.latest_timestamp = record.latest_timestamp.clone();
            }
        }
    }

    fn into_summary(self, key: EventGroupKey) -> AssociatedEvent {
        AssociatedEvent {
            namespace: key.namespace,
            object_kind: key.object_kind,
            object_name: key.object_name,
            object_uid: key.object_uid,
            association: key.association,
            event_type: key.event_type,
            reason: key.reason,
            message: key.message,
            first_timestamp: self.first_timestamp,
            latest_timestamp: self.latest_timestamp,
            latest_epoch_seconds: self.latest_epoch_seconds,
            count: self.count,
        }
    }
}

impl EventOccurrence {
    fn association_for(&self, identity: &ResourceIdentity) -> Option<EventAssociation> {
        if self.namespace != identity.namespace()?.as_str()
            || self.object_kind != identity.kind()
            || self.object_name != identity.name()
            || self
                .object_api_group
                .as_deref()
                .is_some_and(|group| group != identity.api_group())
            || self
                .object_api_version
                .as_deref()
                .is_some_and(|version| version != identity.api_version())
        {
            return None;
        }
        match self.object_uid.as_deref() {
            Some(uid) if uid == identity.uid() => Some(EventAssociation::Uid),
            Some(_) => None,
            None => Some(EventAssociation::NameFallback),
        }
    }
}

fn parse_event_object(object: &DynamicObject, namespace: &str) -> Option<EventOccurrence> {
    let data = &object.data;
    let object_ref = data
        .get("involvedObject")
        .or_else(|| data.get("regarding"))?;
    let object_api_version = bounded_text(object_ref.get("apiVersion"));
    let (object_api_group, object_api_version) = object_api_version
        .as_deref()
        .map(|version| {
            version
                .split_once('/')
                .map_or((Some(String::new()), Some(version.to_owned())), |(group, version)| {
                    (Some(group.to_owned()), Some(version.to_owned()))
                })
        })
        .unwrap_or((None, None));
    let object_namespace = bounded_text(object_ref.get("namespace"))
        .unwrap_or_else(|| namespace.to_owned());
    let object_kind = bounded_text(object_ref.get("kind"))?;
    let object_name = bounded_text(object_ref.get("name"))?;
    let event_type = bounded_text(data.get("type")).unwrap_or_else(|| "UNKNOWN".to_owned());
    let reason = bounded_text(data.get("reason")).unwrap_or_else(|| "UNKNOWN".to_owned());
    let message = bounded_text(data.get("message").or_else(|| data.get("note")))
        .unwrap_or_default();
    let series = data.get("series");
    let metadata_timestamp = object
        .metadata
        .creation_timestamp
        .as_ref()
        .and_then(|timestamp| serde_json::to_value(timestamp).ok())
        .and_then(|timestamp| timestamp.as_str().map(str::to_owned));
    let latest_timestamp = first_text([
        series.and_then(|series| series.get("lastObservedTime")),
        data.get("lastTimestamp"),
        data.get("eventTime"),
        data.get("deprecatedLastTimestamp"),
    ])
    .or(metadata_timestamp);
    let first_timestamp = first_text([
        data.get("firstTimestamp"),
        data.get("deprecatedFirstTimestamp"),
        data.get("eventTime"),
    ])
    .or_else(|| latest_timestamp.clone());
    let latest_epoch_seconds = latest_timestamp
        .as_deref()
        .and_then(|timestamp| timestamp.parse::<Timestamp>().ok())
        .map(Timestamp::as_second);
    let count = series
        .and_then(|series| series.get("count"))
        .and_then(Value::as_u64)
        .or_else(|| data.get("count").and_then(Value::as_u64))
        .or_else(|| data.get("deprecatedCount").and_then(Value::as_u64))
        .unwrap_or(1)
        .max(1);
    let event_name = object.metadata.name.clone()?;
    let event_uid = object.metadata.uid.clone().unwrap_or_else(|| event_name.clone());
    let estimated_bytes = event_name.len()
        + event_uid.len()
        + object_namespace.len()
        + object_kind.len()
        + object_name.len()
        + event_type.len()
        + reason.len()
        + message.len()
        + object_api_group.as_ref().map_or(0, String::len)
        + object_api_version.as_ref().map_or(0, String::len)
        + first_timestamp.as_ref().map_or(0, String::len)
        + latest_timestamp.as_ref().map_or(0, String::len);
    Some(EventOccurrence {
        event_name,
        event_uid,
        namespace: object_namespace,
        object_api_group,
        object_api_version,
        object_kind,
        object_name,
        object_uid: bounded_text(object_ref.get("uid")),
        event_type,
        reason,
        message,
        first_timestamp,
        latest_timestamp,
        latest_epoch_seconds,
        count,
        estimated_bytes,
    })
}

fn first_text<'a>(values: impl IntoIterator<Item = Option<&'a Value>>) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .find_map(|value| bounded_text(Some(value)))
}

fn bounded_text(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(|text| {
        text.chars()
            .take(MAX_EVENT_TEXT_CHARS)
            .collect::<String>()
    })
}

fn parse_epoch_seconds(timestamp: &str) -> Option<i64> {
    timestamp.parse::<Timestamp>().ok().map(Timestamp::as_second)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event_object(
        name: &str,
        uid: &str,
        first_timestamp: &str,
        last_timestamp: &str,
        count: u64,
    ) -> DynamicObject {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {
                "name": name,
                "namespace": "payments",
                "uid": uid,
                "creationTimestamp": last_timestamp
            },
            "involvedObject": {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "checkout",
                "namespace": "payments",
                "uid": "uid-deployment"
            },
            "type": "Warning",
            "reason": "BackOff",
            "message": "container restart back-off",
            "count": count,
            "firstTimestamp": first_timestamp,
            "lastTimestamp": last_timestamp
        }))
        .unwrap()
    }

    #[test]
    fn time_windows_exclude_old_and_unknown_events_but_all_window_keeps_them() {
        let now = "2026-10-14T12:00:00Z".parse::<Timestamp>().unwrap().as_second();
        assert!(EventWindow::LastHour.cutoff(now).is_some());
        assert!(EventWindow::LastDay.cutoff(now).unwrap() < now);
        assert_eq!(EventWindow::All.cutoff(now), None);
    }

    #[test]
    fn parser_bounds_untrusted_fields_and_uses_duplicate_count() {
        let event = event_object(
            "event-1",
            "uid-event-1",
            "2026-10-14T11:00:00Z",
            "2026-10-14T11:59:00Z",
            4,
        );
        let record = parse_event_object(&event, "payments").unwrap();
        assert_eq!(record.object_uid.as_deref(), Some("uid-deployment"));
        assert_eq!(record.count, 4);
        assert_eq!(record.reason, "BackOff");
        assert_eq!(record.namespace, "payments");
    }

    #[test]
    fn event_cache_caps_record_count_and_byte_budget() {
        let mut cache = BoundedEventRecords::default();
        for index in 0..MAX_CACHED_EVENTS {
            assert!(cache.insert(test_record(&format!("event-{index}"), 1)));
        }
        assert_eq!(cache.len(), 10_000);
        assert!(!cache.insert(test_record("event-overflow", 1)));
        assert!(cache.truncated);

        let mut byte_limited = BoundedEventRecords::default();
        assert!(!byte_limited.insert(test_record("large-event", MAX_EVENT_CACHE_BYTES + 1)));
        assert!(byte_limited.truncated);
    }

    #[test]
    fn association_uses_uid_and_name_fallback_only_when_event_uid_missing() {
        let event = event_object(
            "event-1",
            "uid-event-1",
            "2026-10-14T11:30:00Z",
            "2026-10-14T11:50:00Z",
            1,
        );
        let mut record = parse_event_object(&event, "payments").unwrap();
        let selected = selected_identity("uid-deployment");
        assert_eq!(record.association_for(&selected), Some(EventAssociation::Uid));
        record.object_uid = Some("uid-recreated-deployment".to_owned());
        assert_eq!(record.association_for(&selected), None);
        record.object_uid = None;
        assert_eq!(
            record.association_for(&selected),
            Some(EventAssociation::NameFallback)
        );
    }

    #[test]
    fn duplicate_events_aggregate_count_time_and_obey_window_filter() {
        let first = parse_event_object(
            &event_object(
                "event-1",
                "uid-event-1",
                "2026-10-14T10:20:00Z",
                "2026-10-14T10:20:00Z",
                3,
            ),
            "payments",
        )
        .unwrap();
        let recent = parse_event_object(
            &event_object(
                "event-2",
                "uid-event-2",
                "2026-10-14T11:50:00Z",
                "2026-10-14T11:50:00Z",
                2,
            ),
            "payments",
        )
        .unwrap();
        let now = "2026-10-14T12:00:00Z".parse::<Timestamp>().unwrap().as_second();
        let selected = selected_identity("uid-deployment");
        let all = aggregate_events([&first, &recent], &selected, EventWindow::All, now);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].count, 5);
        assert_eq!(all[0].first_timestamp.as_deref(), Some("2026-10-14T10:20:00Z"));
        assert_eq!(all[0].latest_timestamp.as_deref(), Some("2026-10-14T11:50:00Z"));

        let last_hour = aggregate_events([&first, &recent], &selected, EventWindow::LastHour, now);
        assert_eq!(last_hour.len(), 1);
        assert_eq!(last_hour[0].count, 2);
        assert_eq!(last_hour[0].first_timestamp.as_deref(), Some("2026-10-14T11:50:00Z"));
    }

    fn selected_identity(uid: &str) -> ResourceIdentity {
        ResourceIdentity::new(
            "apps",
            "v1",
            "Deployment",
            Some(crate::target::NamespaceName::new("payments").unwrap()),
            "checkout",
            uid,
        )
        .unwrap()
    }

    fn test_record(uid: &str, estimated_bytes: usize) -> EventOccurrence {
        EventOccurrence {
            event_name: uid.to_owned(),
            event_uid: uid.to_owned(),
            namespace: "payments".to_owned(),
            object_api_group: Some("apps".to_owned()),
            object_api_version: Some("apps/v1".to_owned()),
            object_kind: "Deployment".to_owned(),
            object_name: "checkout".to_owned(),
            object_uid: Some("uid-deployment".to_owned()),
            event_type: "Warning".to_owned(),
            reason: "BackOff".to_owned(),
            message: "container restart back-off".to_owned(),
            first_timestamp: None,
            latest_timestamp: None,
            latest_epoch_seconds: None,
            count: 1,
            estimated_bytes,
        }
    }
}