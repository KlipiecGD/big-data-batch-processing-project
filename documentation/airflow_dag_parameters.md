## Airflow DAG Parameters Reference

### Core Identification & Metadata

These parameters define how the DAG is identified and displayed within the Airflow UI.

* **`dag_id` (str):** The unique identifier for the DAG. It must consist exclusively of alphanumeric characters, dashes, dots, and underscores.
* **`dag_display_name` (str):** The name of the DAG as it appears on the UI. It defaults to the `dag_id` if not specified.
* **`description` (str):** A text description of the DAG shown in the webserver.
* **`tags` (List[str]):** A list of tags used to filter DAGs in the UI. Each tag is restricted to a maximum length of 100 characters.
* **`owner_links` (dict):** A dictionary mapping owners to clickable links (HTTP or mailto) in the UI.

---

### Scheduling & Execution Timing

These parameters control when and how the DAG triggers its runs.

| Parameter | Type | Description |
| --- | --- | --- |
| **`schedule`** | `ScheduleArg` | Defines scheduling rules. Supports cron strings, `timedelta`, `Timetable` objects, or a list of `Asset` objects for data-driven triggers. Defaults to `None` in Airflow 3.0. |
| **`start_date`** | `datetime` | The timestamp from which the scheduler attempts backfills. If not provided, backfilling requires manual intervention. |
| **`end_date`** | `datetime` | The date beyond which the DAG will no longer run. Defaults to `None` for open-ended scheduling. |
| **`catchup`** | `bool` | Whether the scheduler should perform a "catchup" of past runs or only run the latest. Defaults to the global `catchup_by_default` config. |

---

In Airflow, **catchup** is a boolean parameter that determines how the scheduler handles "missed" DAG runs between the `start_date` and the current time.

#### How Catchup Works

When you create a new DAG or unpause an existing one, Airflow looks at the `start_date` and the `schedule` interval.

* **If `catchup=True`:** The scheduler will trigger a DAG run for every scheduled interval that has passed since the `start_date`. For example, if your `start_date` was a week ago and your schedule is `@daily`, Airflow will immediately kick off 7 DAG runs to "catch up" to today.
* **If `catchup=False`:** The scheduler only runs the **most recent** interval that has passed. It effectively ignores all other historical intervals.


### Concurrency & Failure Control

Manage resources and handle pipeline failures at the DAG level.

* **`max_active_tasks` (int):** The total number of task instances allowed to run concurrently across all runs of this DAG.
* **`max_active_runs` (int):** The maximum number of active DAG runs allowed. The scheduler will not create new runs beyond this limit.
* **`max_consecutive_failed_dag_runs` (int):** An experimental feature that disables the DAG after a specific number of consecutive failures.
* **`dagrun_timeout` (timedelta):** The maximum duration a `DagRun` is allowed to run before timing out. Running tasks are marked as "skipped" upon timeout.
* **`fail_fast` (bool):** If `True`, all currently running tasks in the DAG are failed immediately if any single task fails. This requires all tasks to use the `all_success` trigger rule.

---

### Templating & Environment

Airflow uses the Jinja2 engine for dynamic parameterization.

* **`template_searchpath` (str | Iterable[str]):** A list of non-relative folders where Jinja looks for template files.
* **`user_defined_macros` (dict):** Macros exposed in Jinja templates (e.g., passing `dict(foo='bar')` allows using `{{ foo }}`).
* **`user_defined_filters` (dict):** Custom filters for templates (e.g., `{{ 'world' | my_custom_filter }}`).
* **`render_template_as_native_obj` (bool):** If `True`, Jinja renders templates as native Python types (like lists or dicts) rather than strings.
* **`jinja_environment_kwargs` (dict):** Additional options passed directly to the Jinja `Environment`.

---

### Callbacks & Alerts

These parameters trigger functions based on the state of a DAG run.

* **`on_success_callback`:** A function or list of functions executed when the DAG succeeds.
* **`on_failure_callback`:** A function or list of functions executed when the DAG fails.
* **`deadline` (`DeadlineAlert`):** Replaces the deprecated SLA feature. Allows for alerts if a DAG does not complete within a specific timeframe.
* **`sla_miss_callback`:** This feature is **removed** in Airflow 3.0 and should be replaced with `deadline` alerts in versions 3.1 and above.

---

### Advanced Configuration

* **`default_args` (dict):** A dictionary of parameters used as defaults for all operators within the DAG.
* **`params` (dict):** DAG-level parameters accessible in templates under the `params` namespace. These can be overridden at the task level.
* **`access_control` (dict):** Optional DAG-level permissions defining which roles can read, edit, or delete the DAG.
* **`is_paused_upon_creation` (bool):** Determines if the DAG starts in a paused state when first loaded.
* **`auto_register` (bool):** Automatically registers the DAG when it is used within a `with` block.

## Trigger Rules

By default, Airflow will wait for all upstream (direct parents) tasks for a task to be successful before it runs that task. However, you can control this behavior using the `trigger_rule` argument on a Task.

| Trigger Rule | Description |
| --- | --- |
| **`all_success`** (default) | All upstream tasks have succeeded |
| **`all_failed`** | All upstream tasks are in a failed or upstream_failed state |
| **`all_done`** | All upstream tasks are done with their execution |
| **`all_done_min_one_success`** | All non-skipped upstream tasks are done with their execution and at least one upstream task has succeeded |
| **`all_skipped`** | All upstream tasks are in a skipped state |
| **`one_failed`** | At least one upstream task has failed (does not wait for all upstream tasks to be done) |
| **`one_success`** | At least one upstream task has succeeded (does not wait for all upstream tasks to be done) |
| **`one_done`** | At least one upstream task succeeded or failed |
| **`none_failed`** | All upstream tasks have not failed or upstream_failed (i.e., all have succeeded or been skipped) |
| **`none_failed_min_one_success`** | All upstream tasks have not failed or upstream_failed, and at least one upstream task has succeeded |
| **`none_skipped`** | No upstream task is in a skipped state (i.e., all upstream tasks are in a success, failed, or upstream_failed state) |
| **`always`** | No dependencies at all; run this task at any time |

## Task Groups

Task groups are a way to organize tasks in a DAG. They allow you to group related tasks together, making it easier to manage and visualize complex workflows. You can define task groups using the `@task_group` decorator.

```python
 from airflow.sdk import task_group


 @task_group()
 def group1():
     task1 = EmptyOperator(task_id="task1")
     task2 = EmptyOperator(task_id="task2")


 task3 = EmptyOperator(task_id="task3")

 group1() >> task3

```

