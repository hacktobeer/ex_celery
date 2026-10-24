defmodule Celery.Result do
  @moduledoc """
  Task result handle (`AsyncResult` in Python Celery, ported from `celery.result.AsyncResult`).

  Provides querying of task state, result value, traceback, and blocking `get/2`
  backed by `Celery.Backend.Redis`, as well as local eager execution results.
  """

  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Config
  alias Celery.States

  @type t :: %__MODULE__{
          id: String.t(),
          app: Celery.t() | Config.t() | nil,
          parent: t() | nil,
          is_eager: boolean(),
          eager_state: String.t() | nil,
          eager_result: term(),
          eager_traceback: String.t() | nil
        }

  defstruct id: nil,
            app: nil,
            parent: nil,
            is_eager: false,
            eager_state: nil,
            eager_result: nil,
            eager_traceback: nil

  @doc """
  Creates a new `%Celery.Result{}` (`AsyncResult`) for `task_id`.
  """
  @spec new(String.t(), keyword()) :: t()
  def new(task_id, opts \\ []) when is_binary(task_id) do
    %__MODULE__{
      id: task_id,
      app: Keyword.get(opts, :app),
      parent: Keyword.get(opts, :parent),
      is_eager: Keyword.get(opts, :is_eager, false),
      eager_state: Keyword.get(opts, :eager_state),
      eager_result: Keyword.get(opts, :eager_result),
      eager_traceback: Keyword.get(opts, :eager_traceback)
    }
  end

  @doc """
  Creates an eager `%Celery.Result{}` (`EagerResult` in Python Celery).
  """
  @spec eager(String.t(), term(), String.t(), keyword()) :: t()
  def eager(task_id, result, state \\ States.success(), opts \\ []) do
    %__MODULE__{
      id: task_id,
      app: Keyword.get(opts, :app),
      parent: Keyword.get(opts, :parent),
      is_eager: true,
      eager_state: state,
      eager_result: result,
      eager_traceback: Keyword.get(opts, :traceback)
    }
  end

  @doc "Returns the task's current state (`\"PENDING\"`, `\"STARTED\"`, `\"SUCCESS\"`, `\"FAILURE\"`, `\"REVOKED\"`, `\"RETRY\"`)."
  @spec state(t()) :: String.t()
  def state(%__MODULE__{is_eager: true, eager_state: st}), do: st || States.success()

  def state(%__MODULE__{id: id, app: app}) do
    RedisBackend.get_state(resolve_app(app), id)
  end

  @doc "Alias for `state/1` (matches `AsyncResult.status` in Python Celery)."
  @spec status(t()) :: String.t()
  def status(%__MODULE__{} = res), do: state(res)

  @doc "Returns the task's stored result (or exception struct if failed, or `nil` if pending)."
  @spec result(t()) :: term()
  def result(%__MODULE__{is_eager: true, eager_result: res}), do: res

  def result(%__MODULE__{id: id, app: app}) do
    RedisBackend.get_result(resolve_app(app), id)
  end

  @doc "Alias for `result/1` (matches `AsyncResult.info` in Python Celery)."
  @spec info(t()) :: term()
  def info(%__MODULE__{} = res), do: result(res)

  @doc "Returns the traceback of a failed task (or `nil`)."
  @spec traceback(t()) :: String.t() | nil
  def traceback(%__MODULE__{is_eager: true, eager_traceback: tb}), do: tb

  def traceback(%__MODULE__{id: id, app: app}) do
    RedisBackend.get_traceback(resolve_app(app), id)
  end

  @doc "Returns the full task metadata map from Redis."
  @spec backend_meta(t()) :: map()
  def backend_meta(%__MODULE__{is_eager: true} = res) do
    %{
      "task_id" => res.id,
      "status" => res.eager_state,
      "result" => res.eager_result,
      "traceback" => res.eager_traceback
    }
  end

  def backend_meta(%__MODULE__{id: id, app: app}) do
    RedisBackend.get_task_meta(resolve_app(app), id)
  end

  @doc "Returns `true` if the task has been executed (`SUCCESS`, `FAILURE`, or `REVOKED`)."
  @spec ready?(t()) :: boolean()
  def ready?(%__MODULE__{} = res), do: States.ready?(state(res))

  @doc "Returns `true` if the task succeeded (`state == \"SUCCESS\"`)."
  @spec successful?(t()) :: boolean()
  def successful?(%__MODULE__{} = res), do: state(res) == States.success()

  @doc "Returns `true` if the task failed (`state == \"FAILURE\"`)."
  @spec failed?(t()) :: boolean()
  def failed?(%__MODULE__{} = res), do: state(res) == States.failure()

  @doc """
  Waits until the task is ready and returns its result.

  Options:
  - `:timeout` - timeout in milliseconds, float seconds, or `:infinity` (default `:infinity`)
  - `:interval` - poll interval in milliseconds (default `50`)
  - `:propagate` - re-raise exception if the task failed or was revoked (default `true`)
  """
  @spec get(t(), keyword()) :: term()
  def get(res, opts \\ [])

  def get(%__MODULE__{is_eager: true} = res, opts) do
    propagate = Keyword.get(opts, :propagate, true)

    if propagate and States.propagate?(res.eager_state) do
      case res.eager_result do
        %{__exception__: true} = exc -> raise exc
        other -> raise Celery.Error, message: "Task #{res.id} failed: #{inspect(other)}"
      end
    else
      res.eager_result
    end
  end

  def get(%__MODULE__{id: id, app: app}, opts) do
    RedisBackend.wait_for(resolve_app(app), id, opts)
  end

  @doc "Forgets (deletes) the result from the Redis result backend."
  @spec forget(t()) :: :ok
  def forget(%__MODULE__{is_eager: true}), do: :ok

  def forget(%__MODULE__{id: id, app: app}) do
    RedisBackend.forget(resolve_app(app), id)
  end

  @doc """
  Revokes the task by marking it revoked in the backend and optionally broadcasting a
  revoke control command over `Kombu.Pidbox`.
  """
  @spec revoke(t(), keyword()) :: :ok
  def revoke(%__MODULE__{id: id, app: app}, opts \\ []) do
    resolved = resolve_app(app)
    RedisBackend.mark_as_revoked(resolved, id, "revoked")

    if Keyword.get(opts, :broadcast, false) do
      Celery.Worker.Control.revoke(resolved, id, opts)
    end

    :ok
  end

  defp resolve_app(nil), do: Config.new()
  defp resolve_app(app), do: app
end

defmodule Celery.Result.GroupResult do
  @moduledoc """
  Result handle for a group of tasks (`GroupResult` in Python Celery).
  """

  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Config
  alias Celery.Result

  @type t :: %__MODULE__{
          id: String.t(),
          results: [Result.t()],
          app: Celery.t() | Config.t() | nil,
          parent: Result.t() | nil
        }

  defstruct id: nil,
            results: [],
            app: nil,
            parent: nil

  @doc "Creates a new `%Celery.Result.GroupResult{}`."
  @spec new(String.t(), [Result.t()], keyword()) :: t()
  def new(id, results \\ [], opts \\ []) do
    %__MODULE__{
      id: id,
      results: results,
      app: Keyword.get(opts, :app),
      parent: Keyword.get(opts, :parent)
    }
  end

  @doc "Returns `true` when all child results in the group are ready."
  @spec ready?(t()) :: boolean()
  def ready?(%__MODULE__{results: results}) do
    Enum.all?(results, &Result.ready?/1)
  end

  @doc "Returns `true` when all child results in the group succeeded."
  @spec successful?(t()) :: boolean()
  def successful?(%__MODULE__{results: results}) do
    Enum.all?(results, &Result.successful?/1)
  end

  @doc "Returns `true` if any child result in the group failed."
  @spec failed?(t()) :: boolean()
  def failed?(%__MODULE__{results: results}) do
    Enum.any?(results, &Result.failed?/1)
  end

  @doc "Returns the number of completed tasks in the group."
  @spec completed_count(t()) :: non_neg_integer()
  def completed_count(%__MODULE__{results: results}) do
    Enum.count(results, &Result.ready?/1)
  end

  @doc """
  Waits for all tasks in the group to complete and returns their results in order.
  """
  @spec get(t(), keyword()) :: [term()]
  def get(%__MODULE__{results: results}, opts \\ []) do
    Enum.map(results, &Result.get(&1, opts))
  end

  @doc "Alias for `get/2` (matches `GroupResult.join` in Python Celery)."
  @spec join(t(), keyword()) :: [term()]
  def join(%__MODULE__{} = group_res, opts \\ []), do: get(group_res, opts)

  @doc "Saves the group result IDs to the Redis backend under `celery-taskset-meta-<id>`."
  @spec save(t()) :: :ok | {:error, term()}
  def save(%__MODULE__{id: id, results: results, app: app}) do
    meta = %{"id" => id, "results" => Enum.map(results, & &1.id)}
    RedisBackend.save_group(app || Config.new(), id, meta)
  end

  @doc "Restores a `%Celery.Result.GroupResult{}` from the Redis backend."
  @spec restore(String.t(), keyword()) :: t() | nil
  def restore(group_id, opts \\ []) when is_binary(group_id) do
    app = Keyword.get(opts, :app) || Config.new()

    case RedisBackend.restore_group(app, group_id) do
      %{"results" => ids} when is_list(ids) ->
        results =
          Enum.map(ids, fn
            tid when is_binary(tid) -> Result.new(tid, app: app)
            [tid | _] when is_binary(tid) -> Result.new(tid, app: app)
          end)

        %__MODULE__{id: group_id, results: results, app: app}

      _ ->
        nil
    end
  end

  @doc "Deletes the saved group metadata from Redis."
  @spec delete(t()) :: :ok
  def delete(%__MODULE__{id: id, app: app}) do
    RedisBackend.delete_group(app || Config.new(), id)
  end
end
