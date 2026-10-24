defmodule Src.Tasks do
  @moduledoc """
  Sample Celery tasks module matching the user's Python Celery `src.tasks` specification:

  ```python
  celery = Celery(broker=REDIS_URL, backend=REDIS_URL, include=["src.tasks"])

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
  """
  use Celery.Task

  @task_name "src.tasks.command"
  @task_metadata %{"owner" => "pipeline", "version" => "1.0"}

  def task_name, do: @task_name
  def task_metadata, do: @task_metadata

  @task bind: true, name: @task_name, metadata: @task_metadata
  def command(
        self,
        pipe_result \\ nil,
        input_files \\ nil,
        output_path \\ nil,
        workflow_id \\ nil,
        task_config \\ nil
      ) do
    # If task_config asks for detailed echo (for testing argument binding), return a map;
    # otherwise return `"This is a command"` exactly as in the Python example.
    if is_map(task_config) and Map.get(task_config, "echo") == true do
      %{
        "message" => "This is a command",
        "task_id" => self.request.id,
        "task_name" => self.name,
        "metadata" => self.metadata,
        "pipe_result" => pipe_result,
        "input_files" => input_files,
        "output_path" => output_path,
        "workflow_id" => workflow_id,
        "task_config" => task_config
      }
    else
      _ = {self, pipe_result, input_files, output_path, workflow_id, task_config}
      "This is a command"
    end
  end

  @task name: "src.tasks.add"
  def add(x, y) do
    x + y
  end

  @task name: "src.tasks.sum_list"
  def sum_list(numbers) when is_list(numbers) do
    Enum.sum(numbers)
  end

  @task bind: true, name: "src.tasks.fail_task"
  def fail_task(_self, reason \\ "boom") do
    raise RuntimeError, reason
  end

  @task bind: true, name: "src.tasks.flaky_task", max_retries: 3, default_retry_delay: 0.05
  def flaky_task(self, succeed_on_retry \\ 1) do
    if self.request.retries < succeed_on_retry do
      Celery.Task.retry(self, countdown: 0.02, exc: RuntimeError.exception("try again"))
    else
      "recovered_after_#{self.request.retries}_retries"
    end
  end

  @task bind: true, name: "src.tasks.progress_task", track_started: true
  def progress_task(self, steps \\ 2) do
    Celery.Task.update_state(self, state: "PROGRESS", meta: %{"current" => 1, "total" => steps})
    "completed_#{steps}_steps"
  end

  @task name: "src.tasks.slow_task", time_limit: 0.1
  def slow_task(sleep_ms \\ 500) do
    Process.sleep(sleep_ms)
    "done"
  end
end
