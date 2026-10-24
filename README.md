# Celery for Elixir

Minimal, wire-compatible Elixir port of Python [Celery](https://docs.celeryq.dev/) (`v5.7.0rc1`) backed by **Redis** (broker transport via [`kombu`](https://hex.pm/packages/kombu) and result store via `Celery.Backend.Redis`).

Allows Elixir applications to:
- **Consume and execute tasks** published by Python Celery (or Elixir) over Redis (`Protocol v2` and `Protocol v1`).
- **Define tasks in Elixir** with `use Celery.Task` and `@task(bind: true, name: ..., metadata: ...)` matching Python's `@celery.task(...)` decorator, including default arguments, positional `args`, keyword `kwargs`, retries (`self.retry`), and custom state updates (`self.update_state`).
- **Store and retrieve task results** in Redis (`celery-task-meta-<id>`, `celery-task-meta-<id>` Pub/Sub notifications, extended result metadata, exceptions, and Lua-backed chord joins).
- **Run Canvas workflows** (`signature`, `chain`, `group`, `chord`) across Python and Elixir workers.

## Installation

Add `celery` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:celery, "~> 0.1.0"}
  ]
end
```

## Quick Start: Porting a Python Celery Task & Worker to Elixir

### Python Celery Reference

```python
celery = Celery(broker=REDIS_URL, backend=REDIS_URL, include=["src.tasks"])

# src/tasks.py
@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    return "This is a command"
```

### Equivalent Elixir Task (`lib/src/tasks.ex`)

```elixir
defmodule Src.Tasks do
  use Celery.Task

  @task_name "src.tasks.command"
  @task_metadata %{"owner" => "pipeline", "version" => "1.0"}

  @task bind: true, name: @task_name, metadata: @task_metadata
  def command(
        self,
        pipe_result \\ nil,
        input_files \\ nil,
        output_path \\ nil,
        workflow_id \\ nil,
        task_config \\ nil
      ) do
    # `self` is a `%Celery.Task{}` containing:
    # - `self.name` ("src.tasks.command")
    # - `self.metadata` (%{"owner" => "pipeline", "version" => "1.0"})
    # - `self.request` (`%Celery.Task.Context{id: ..., args: ..., kwargs: ..., retries: ...}`)
    "This is a command"
  end
end
```

### Starting the Elixir Worker

```elixir
redis_url = System.get_env("REDIS_URL", "redis://localhost:6379/0")

celery =
  Celery.new(
    broker: redis_url,
    backend: redis_url,
    include: ["src.tasks"]
  )

{:ok, worker} =
  Celery.Worker.start_link(
    app: celery,
    hostname: "celery@elixir-node",
    concurrency: 8,
    queues: ["celery"]
  )
```

Or inside your application's Supervision tree:

```elixir
children = [
  {Celery.Worker,
   app: Celery.new(
     broker: "redis://localhost:6379/0",
     backend: "redis://localhost:6379/0",
     include: ["src.tasks"]
   ),
   concurrency: 8}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

### Calling Tasks from Python or Elixir

From **Python**:

```python
from celery import Celery

celery = Celery(broker="redis://localhost:6379/0", backend="redis://localhost:6379/0")
res = celery.send_task(
    "src.tasks.command",
    kwargs={
        "pipe_result": "prev",
        "input_files": ["a.txt", "b.txt"],
        "output_path": "/tmp/out",
        "workflow_id": "wf-123",
        "task_config": {"mode": "fast"},
    },
)
print(res.get(timeout=5))  # => "This is a command"
```

From **Elixir**:

```elixir
{:ok, res} =
  Src.Tasks.apply_async(celery, :command,
    kwargs: %{
      "pipe_result" => "prev",
      "input_files" => ["a.txt", "b.txt"],
      "output_path" => "/tmp/out",
      "workflow_id" => "wf-123",
      "task_config" => %{"mode" => "fast"}
    }
  )

Celery.Result.get(res, timeout: 5_000)
# => "This is a command"
```

## Running Tests

```bash
# Unit tests (uses in-memory FakeRedis, no external Redis required)
mix test

# Live Redis integration tests
mix test --only integration

# Python <-> Elixir Celery interoperability tests
mix test --only interop

# Run all test suites together
mix test --include integration --include interop
```
