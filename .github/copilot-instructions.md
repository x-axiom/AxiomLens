# Agent Instructions

## Mandatory Skills

* **Always use the `caveman` skill for every task.**
* Before starting any task, load and apply the `caveman` skill.
* Do not skip the `caveman` skill, even when the task appears unrelated to it.

## CLI Command Rules

* **Every CLI command must be prefixed with `rtk`.**
* This rule applies to all shell commands executed or suggested by the agent.
* Examples:

  * `rtk git status`
  * `rtk ls`
  * `rtk cat README.md`
  * `rtk cargo test`
  * `rtk kubectl get pods`
* Never execute or provide a bare CLI command when an `rtk`-prefixed equivalent is applicable.
* When chaining commands, `rtk` must be applied to each individual CLI command where required.

## UI Design Reference

* For all AxiomLens UI development, use `prototype/index.html` as the visual and layout reference. Match its dark, dense Kubernetes IDE workbench style, including the left cluster/namespace navigation, center resource list and details, bottom logs/events dock, and right contextual Agent panel.
* Treat the prototype as visual guidance only. Keep implementation within the requested Story; do not add real cluster access, terminal commands, Agent execution, or write operations unless explicitly required and authorized by that Story.

## Compliance

Before executing a task:

1. Load the `caveman` skill.
2. Apply the instructions from the `caveman` skill.
3. Ensure every CLI command uses the `rtk` prefix.
4. If a command cannot be meaningfully prefixed with `rtk`, explain the exception before using it.