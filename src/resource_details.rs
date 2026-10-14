use kube::api::DynamicObject;
use serde_json::Value;

const MAX_DETAIL_ITEMS: usize = 64;
const MAX_DETAIL_TEXT_CHARS: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceCondition {
    pub condition_type: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerDetail {
    pub role: &'static str,
    pub name: String,
    pub image: Option<String>,
    pub image_pull_policy: Option<String>,
    pub ports: Vec<String>,
    pub resource_requests: Option<String>,
    pub resource_limits: Option<String>,
    pub ready: Option<bool>,
    pub restarts: Option<u64>,
    pub state: &'static str,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRuntimeDetail {
    pub phase: Option<String>,
    pub reason: Option<String>,
    pub ready_condition: Option<String>,
    pub restarts: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadRolloutDetail {
    pub desired_replicas: Option<i64>,
    pub current_replicas: Option<i64>,
    pub updated_replicas: Option<i64>,
    pub ready_replicas: Option<i64>,
    pub available_replicas: Option<i64>,
    pub unavailable_replicas: Option<i64>,
    pub generation: Option<i64>,
    pub observed_generation: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobProgressDetail {
    pub active: Option<i64>,
    pub succeeded: Option<i64>,
    pub failed: Option<i64>,
    pub completion_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceDetails {
    pub conditions: Vec<ResourceCondition>,
    pub containers: Vec<ContainerDetail>,
    pub pod: Option<PodRuntimeDetail>,
    pub workload: Option<WorkloadRolloutDetail>,
    pub job: Option<JobProgressDetail>,
}

impl ResourceDetails {
    pub fn from_object(kind: &str, object: &DynamicObject) -> Self {
        let status = object.data.get("status").unwrap_or(&Value::Null);
        let conditions = conditions(status);
        let containers = containers(&object.data);
        let pod = (kind == "Pod").then(|| pod_runtime(status, &containers));
        let workload = matches!(
            kind,
            "Deployment" | "StatefulSet" | "DaemonSet" | "ReplicaSet"
        )
        .then(|| workload_rollout(kind, &object.data, status, object.metadata.generation));
        let job = (kind == "Job").then(|| job_progress(status));

        Self {
            conditions,
            containers,
            pod,
            workload,
            job,
        }
    }
}

pub fn to_readonly_yaml(object: &DynamicObject) -> Result<String, serde_yaml_ng::Error> {
    serde_yaml_ng::to_string(object)
}

fn conditions(status: &Value) -> Vec<ResourceCondition> {
    status
        .get("conditions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(MAX_DETAIL_ITEMS)
        .filter_map(|condition| {
            Some(ResourceCondition {
                condition_type: bounded_string(condition.get("type")?)?,
                status: bounded_string(condition.get("status")?)?,
                reason: optional_string(condition.get("reason")),
                message: optional_string(condition.get("message")),
                last_transition_time: optional_string(condition.get("lastTransitionTime")),
            })
        })
        .collect()
}

fn containers(object: &Value) -> Vec<ContainerDetail> {
    let spec = object.get("spec").unwrap_or(&Value::Null);
    let status = object.get("status").unwrap_or(&Value::Null);
    let container_sets = [
        (
            "Container",
            "containers",
            "containerStatuses",
        ),
        (
            "Init container",
            "initContainers",
            "initContainerStatuses",
        ),
        (
            "Ephemeral container",
            "ephemeralContainers",
            "ephemeralContainerStatuses",
        ),
    ];
    let mut details = Vec::new();

    for (role, spec_key, status_key) in container_sets {
        let specs = spec.get(spec_key).and_then(Value::as_array);
        let statuses = status.get(status_key).and_then(Value::as_array);
        for container in specs.into_iter().flatten().take(MAX_DETAIL_ITEMS - details.len()) {
            let Some(name) = container.get("name").and_then(bounded_string) else {
                continue;
            };
            let container_status = statuses
                .into_iter()
                .flatten()
                .find(|candidate| candidate.get("name").and_then(Value::as_str) == Some(&name));
            let (state, reason) = container_state(container_status);
            details.push(ContainerDetail {
                role,
                name,
                image: optional_string(container.get("image")),
                image_pull_policy: optional_string(container.get("imagePullPolicy")),
                ports: container
                    .get("ports")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .take(MAX_DETAIL_ITEMS)
                    .filter_map(container_port)
                    .collect(),
                resource_requests: object_summary(
                    container.get("resources").and_then(|resources| resources.get("requests")),
                ),
                resource_limits: object_summary(
                    container.get("resources").and_then(|resources| resources.get("limits")),
                ),
                ready: container_status
                    .and_then(|container_status| container_status.get("ready"))
                    .and_then(Value::as_bool),
                restarts: container_status
                    .and_then(|container_status| container_status.get("restartCount"))
                    .and_then(Value::as_u64),
                state,
                reason,
            });
        }
    }
    details
}

fn container_state(status: Option<&Value>) -> (&'static str, Option<String>) {
    let Some(state) = status.and_then(|status| status.get("state")) else {
        return ("UNKNOWN", None);
    };
    if state.get("running").is_some() {
        ("RUNNING", None)
    } else if let Some(waiting) = state.get("waiting") {
        ("WAITING", optional_string(waiting.get("reason")))
    } else if let Some(terminated) = state.get("terminated") {
        ("TERMINATED", optional_string(terminated.get("reason")))
    } else {
        ("UNKNOWN", None)
    }
}

fn pod_runtime(status: &Value, containers: &[ContainerDetail]) -> PodRuntimeDetail {
    let container_statuses = [
        "containerStatuses",
        "initContainerStatuses",
        "ephemeralContainerStatuses",
    ]
    .into_iter()
    .filter_map(|key| status.get(key).and_then(Value::as_array))
    .flatten()
    .collect::<Vec<_>>();
    let restarts = (!container_statuses.is_empty()).then(|| {
        container_statuses
            .iter()
            .filter_map(|status| status.get("restartCount").and_then(Value::as_u64))
            .fold(0_u64, u64::saturating_add)
    });
    let ready_condition = status
        .get("conditions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|condition| condition.get("type").and_then(Value::as_str) == Some("Ready"))
        .and_then(|condition| condition.get("status"))
        .and_then(bounded_string);
    let reason = optional_string(status.get("reason")).or_else(|| {
        containers
            .iter()
            .find_map(|container| container.reason.clone())
    });

    PodRuntimeDetail {
        phase: optional_string(status.get("phase")),
        reason,
        ready_condition,
        restarts,
    }
}

fn workload_rollout(
    kind: &str,
    object: &Value,
    status: &Value,
    generation: Option<i64>,
) -> WorkloadRolloutDetail {
    let spec = object.get("spec").unwrap_or(&Value::Null);
    let daemon_set = kind == "DaemonSet";
    WorkloadRolloutDetail {
        desired_replicas: if daemon_set {
            number(status.get("desiredNumberScheduled"))
        } else {
            number(spec.get("replicas"))
        },
        current_replicas: number(status.get("replicas"))
            .or_else(|| number(status.get("currentNumberScheduled"))),
        updated_replicas: number(status.get("updatedReplicas"))
            .or_else(|| number(status.get("updatedNumberScheduled"))),
        ready_replicas: number(status.get("readyReplicas"))
            .or_else(|| number(status.get("numberReady"))),
        available_replicas: number(status.get("availableReplicas"))
            .or_else(|| number(status.get("numberAvailable"))),
        unavailable_replicas: number(status.get("unavailableReplicas"))
            .or_else(|| number(status.get("numberUnavailable"))),
        generation,
        observed_generation: number(status.get("observedGeneration")),
    }
}

fn container_port(port: &Value) -> Option<String> {
    let number = port.get("containerPort").and_then(Value::as_i64)?;
    let protocol = optional_string(port.get("protocol")).unwrap_or_else(|| "TCP".to_owned());
    let name = optional_string(port.get("name"));
    Some(match name {
        Some(name) => format!("{name} / {number}/{protocol}"),
        None => format!("{number}/{protocol}"),
    })
}

fn object_summary(value: Option<&Value>) -> Option<String> {
    let value = value?;
    if value.is_null() || value.as_object().is_some_and(serde_json::Map::is_empty) {
        return None;
    }
    serde_json::to_string(value).ok().map(|summary| {
        summary
            .chars()
            .take(MAX_DETAIL_TEXT_CHARS)
            .collect::<String>()
    })
}

fn job_progress(status: &Value) -> JobProgressDetail {
    JobProgressDetail {
        active: number(status.get("active")),
        succeeded: number(status.get("succeeded")),
        failed: number(status.get("failed")),
        completion_time: optional_string(status.get("completionTime")),
    }
}

fn number(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64)
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    value.and_then(bounded_string)
}

fn bounded_string(value: &Value) -> Option<String> {
    value.as_str().map(|value| {
        value
            .chars()
            .take(MAX_DETAIL_TEXT_CHARS)
            .collect::<String>()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dynamic_object(value: Value) -> DynamicObject {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn yaml_is_readable_and_round_trips_original_object() {
        let value = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "checkout-0",
                "namespace": "payments",
                "uid": "uid-1",
                "resourceVersion": "rv-4"
            },
            "spec": {
                "containers": [{ "name": "api", "image": "example/api:v1" }]
            },
            "status": { "phase": "Running" }
        });
        let object = dynamic_object(value.clone());

        let yaml = to_readonly_yaml(&object).unwrap();
        let round_trip: Value = serde_yaml_ng::from_str(&yaml).unwrap();

        assert!(yaml.contains("apiVersion: v1"));
        assert!(yaml.contains("kind: Pod"));
        assert_eq!(round_trip, value);
    }

    #[test]
    fn pod_details_report_conditions_container_state_and_restarts() {
        let object = dynamic_object(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "checkout-0", "generation": 1 },
            "spec": {
                "containers": [{
                    "name": "api",
                    "image": "example/api:v1",
                    "imagePullPolicy": "IfNotPresent",
                    "ports": [{ "name": "http", "containerPort": 8080, "protocol": "TCP" }],
                    "resources": {
                        "requests": { "cpu": "100m", "memory": "128Mi" },
                        "limits": { "memory": "256Mi" }
                    },
                    "env": [{ "name": "TOKEN", "value": "local-secret-value" }]
                }]
            },
            "status": {
                "phase": "Running",
                "reason": "ContainersNotReady",
                "conditions": [{
                    "type": "Ready",
                    "status": "False",
                    "reason": "ContainersNotReady",
                    "message": "container is waiting"
                }],
                "containerStatuses": [{
                    "name": "api",
                    "ready": false,
                    "restartCount": 4,
                    "state": { "waiting": { "reason": "CrashLoopBackOff" } }
                }]
            }
        }));

        let details = ResourceDetails::from_object("Pod", &object);
        assert_eq!(details.pod.as_ref().unwrap().phase.as_deref(), Some("Running"));
        assert_eq!(
            details.pod.as_ref().unwrap().ready_condition.as_deref(),
            Some("False")
        );
        assert_eq!(details.pod.as_ref().unwrap().restarts, Some(4));
        assert_eq!(details.conditions[0].condition_type, "Ready");
        assert_eq!(details.containers[0].image.as_deref(), Some("example/api:v1"));
        assert_eq!(
            details.containers[0].image_pull_policy.as_deref(),
            Some("IfNotPresent")
        );
        assert_eq!(details.containers[0].ports, ["http / 8080/TCP"]);
        assert!(details.containers[0]
            .resource_requests
            .as_deref()
            .unwrap()
            .contains("100m"));
        assert_eq!(details.containers[0].state, "WAITING");
        assert_eq!(details.containers[0].reason.as_deref(), Some("CrashLoopBackOff"));
        assert!(!format!("{:?}", details).contains("local-secret-value"));
    }

    #[test]
    fn workload_detail_reports_rollout_counts_without_health_claims() {
        let object = dynamic_object(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "checkout", "generation": 8 },
            "spec": { "replicas": 5 },
            "status": {
                "replicas": 4,
                "updatedReplicas": 3,
                "readyReplicas": 2,
                "availableReplicas": 2,
                "unavailableReplicas": 3,
                "observedGeneration": 7
            }
        }));

        let details = ResourceDetails::from_object("Deployment", &object);
        let rollout = details.workload.unwrap();
        assert_eq!(rollout.desired_replicas, Some(5));
        assert_eq!(rollout.ready_replicas, Some(2));
        assert_eq!(rollout.generation, Some(8));
        assert_eq!(rollout.observed_generation, Some(7));
    }

    #[test]
    fn daemonset_rollout_uses_scheduled_counts() {
        let object = dynamic_object(json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": { "name": "node-agent", "generation": 3 },
            "spec": {},
            "status": {
                "currentNumberScheduled": 6,
                "desiredNumberScheduled": 8,
                "numberReady": 5,
                "updatedNumberScheduled": 7,
                "numberAvailable": 5,
                "numberUnavailable": 3,
                "observedGeneration": 2
            }
        }));

        let rollout = ResourceDetails::from_object("DaemonSet", &object)
            .workload
            .unwrap();
        assert_eq!(rollout.desired_replicas, Some(8));
        assert_eq!(rollout.current_replicas, Some(6));
        assert_eq!(rollout.ready_replicas, Some(5));
        assert_eq!(rollout.updated_replicas, Some(7));
    }
}