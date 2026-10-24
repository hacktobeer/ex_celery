defmodule Celery do
  @moduledoc """
  Elixir port of Python's Celery distributed task queue (`celery.Celery`),
  wire-compatible with Python Celery (Protocol v1 & v2) and backed by Kombu and Redis
  for transport and result storage.

  ## Quick Start

  Define a task module using `use Celery.Task` and `@task`:

  ```elixir
  defmodule Src.Tasks do
    use Celery.Task

    @task_name "src.tasks.command"
    @task_metadata %{"service" => "pipeline"}

    @task bind: true, name: @task_name, metadata: @task_metadata
    def command(
      self,
      pipe_result \\\\ nil,
      input_files \\\\ nil,
      output_path \\\\ nil,
      workflow_id \\\\ nil,
      task_config \\\\ nil
    ) do
      "This is a command"
    end
  end
  ```

  Create a `Celery` application and start a worker:

  ```elixir
  celery = Celery.new(
    broker: "redis://localhost:6379/0",
    backend: "redis://localhost:6379/0",
    include: ["src.tasks"]
  )

  {:ok, worker} = Celery.start_worker(celery)
  ```
  """

  alias Celery.AMQP
  alias Celery.Canvas
  alias Celery.Canvas.Signature
  alias Celery.Config
  alias Celery.Protocol
  alias Celery.Registry
  alias Celery.Result
  alias Celery.Result.GroupResult
  alias Celery.Task
  alias Celery.Task.Definition
  alias Celery.Worker

  @type t :: %__MODULE__{
          main: String.t(),
          config: Config.t(),
          tasks: %{String.t() => Definition.t()}
        }

  defstruct main: "celery",
            config: %Config{},
            tasks: %{}

  @doc """
  Creates a new `%Celery{}` application instance.

  Accepts Python Celery-style options:
  - `:broker` or `:broker_url` - Redis broker URL (e.g. `"redis://localhost:6379/0"`)
  - `:backend` or `:result_backend` - Redis result backend URL (e.g. `"redis://localhost:6379/0"`)
  - `:include` - list of task modules (`[Src.Tasks]` or Python-style `["src.tasks"]`)
  - any other `%Celery.Config{}` option (`:task_serializer`, `:result_serializer`, `:task_acks_late`, etc.)
  """
  @spec new(keyword() | map() | String.t(), keyword() | map()) :: t()
  def new(main_or_opts \\ [], opts \\ [])

  def new(main, opts) when is_binary(main) do
    opts_map = Enum.into(opts, %{}) |> Map.put(:main, main)
    build_app(opts_map)
  end

  def new(opts, _extra) when is_list(opts) or is_map(opts) do
    build_app(opts)
  end

  @doc """
  Updates the configuration of a `%Celery{}` application (`app.conf.update(...)`).
  """
  @spec update_config(t(), keyword() | map()) :: t()
  def update_config(%__MODULE__{config: config} = app, changes) do
    new_config = Config.update(config, changes)
    %{app | main: new_config.main, config: new_config}
  end

  @doc """
  Registers an inline task function on the `%Celery{}` application (matching `@celery.task(...)`).

  ## Example

      celery =
        Celery.task(
          celery,
          [bind: true, name: "src.tasks.command", metadata: %{"v" => 1}],
          fn self, pipe_result, input_files, output_path, workflow_id, task_config ->
            "This is a command"
          end
        )
  """
  @spec task(t(), keyword(), function()) :: {t(), Definition.t()}
  def task(%__MODULE__{} = app, opts \\ [], fun) when is_function(fun) do
    {:arity, arity} = Function.info(fun, :arity)

    name =
      Keyword.get(opts, :name) || "celery.anonymous_task_#{:erlang.unique_integer([:positive])}"

    param_specs = Keyword.get(opts, :param_specs, [])

    defn =
      Task.build_definition(
        nil,
        nil,
        arity,
        Keyword.put(opts, :fun, fun) |> Keyword.put(:name, name),
        param_specs
      )

    updated_app = %{app | tasks: Map.put(app.tasks, defn.name, defn)}
    {updated_app, defn}
  end

  @doc """
  Registers a `%Celery.Task.Definition{}` or a module using `Celery.Task` on the `%Celery{}` app.
  """
  @spec register_task(t(), Definition.t() | module() | String.t()) :: t()
  def register_task(%__MODULE__{} = app, %Definition{} = defn) do
    Registry.register(defn)
    %{app | tasks: Map.put(app.tasks, defn.name, defn)}
  end

  def register_task(%__MODULE__{} = app, mod_or_str)
      when is_atom(mod_or_str) or is_binary(mod_or_str) do
    loaded = Registry.load_include(mod_or_str)

    Enum.reduce(loaded, app, fn defn, acc ->
      %{acc | tasks: Map.put(acc.tasks, defn.name, defn)}
    end)
  end

  @doc """
  Sends a task by name (`celery.send_task(name, args=..., kwargs=..., ...)`).

  If `app.config.task_always_eager` is `true`, executes the task locally via `Celery.Task.apply_local/5`.
  Otherwise encodes a Celery Protocol v2 (or v1) task message and publishes it over Kombu to Redis.

  Returns `{:ok, %Celery.Result{id: task_id}}`.
  """
  @spec send_task(t(), String.t(), keyword()) :: {:ok, Result.t()} | {:error, term()}
  def send_task(%__MODULE__{config: config} = app, name, opts \\ []) when is_binary(name) do
    args = Keyword.get(opts, :args, [])
    kwargs = Keyword.get(opts, :kwargs, %{})

    if config.task_always_eager do
      Task.apply_local(app, name, args, kwargs, opts)
    else
      task_id = Keyword.get_lazy(opts, :task_id, &Protocol.uuid/0)
      opts = Keyword.put(opts, :task_id, task_id)
      msg = AMQP.create_task_message(config, name, args, kwargs, opts)

      case AMQP.send_task_message(config, msg, opts) do
        :ok ->
          {:ok, Result.new(task_id, app: app)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Convenience function to dispatch a task with positional `args` and keyword `kwargs` (`delay`).
  """
  @spec delay(t(), Definition.t() | String.t(), list(), map() | keyword()) ::
          {:ok, Result.t()} | {:error, term()}
  def delay(%__MODULE__{} = app, task_or_name, args \\ [], kwargs \\ %{}) do
    Task.delay(app, task_or_name, args, kwargs)
  end

  @doc """
  Convenience function to dispatch a task with full options (`apply_async`).
  """
  @spec apply_async(t(), Definition.t() | String.t(), keyword()) ::
          {:ok, Result.t()} | {:error, term()}
  def apply_async(%__MODULE__{} = app, task_or_name, opts \\ []) do
    Task.apply_async(app, task_or_name, opts)
  end

  @doc """
  Creates a `%Celery.Result{}` (`AsyncResult`) bound to `app`.
  """
  @spec async_result(t(), String.t()) :: Result.t()
  def async_result(%__MODULE__{} = app, task_id) when is_binary(task_id) do
    Result.new(task_id, app: app)
  end

  @doc """
  Starts a `Celery.Worker` consuming tasks for `app`.
  """
  @spec start_worker(t() | keyword(), keyword()) :: GenServer.on_start()
  def start_worker(app_or_opts \\ [], opts \\ [])

  def start_worker(%__MODULE__{} = app, opts) do
    Worker.start_link(Keyword.put(opts, :app, app))
  end

  def start_worker(opts, _extra) when is_list(opts) do
    Worker.start_link(opts)
  end

  @doc "Creates a `%Celery.Canvas.Signature{}` bound to `app`."
  @spec signature(t(), String.t(), keyword()) :: Signature.t()
  def signature(%__MODULE__{} = app, task_name, opts \\ []) do
    Canvas.signature(task_name, Keyword.put(opts, :app, app))
  end

  @doc "Creates a `%Celery.Canvas.Signature{}` (`s`) with `args` and `kwargs`."
  @spec s(String.t() | Definition.t(), list(), map() | keyword()) :: Signature.t()
  def s(task_or_name, args \\ [], kwargs \\ %{}), do: Task.s(task_or_name, args, kwargs)

  @doc "Creates an immutable `%Celery.Canvas.Signature{}` (`si`) with `args` and `kwargs`."
  @spec si(String.t() | Definition.t(), list(), map() | keyword()) :: Signature.t()
  def si(task_or_name, args \\ [], kwargs \\ %{}), do: Task.si(task_or_name, args, kwargs)

  @doc "Executes a chain of `%Celery.Canvas.Signature{}` tasks."
  @spec chain([Signature.t()], keyword()) :: {:ok, Result.t()} | {:error, term()}
  def chain(sigs, opts \\ []), do: Canvas.chain(sigs, opts)

  @doc "Executes a parallel group of `%Celery.Canvas.Signature{}` tasks."
  @spec group([Signature.t()], keyword()) :: {:ok, GroupResult.t()} | {:error, term()}
  def group(sigs, opts \\ []), do: Canvas.group(sigs, opts)

  @doc "Executes a chord (parallel group header + callback body signature)."
  @spec chord([Signature.t()], Signature.t(), keyword()) :: {:ok, Result.t()} | {:error, term()}
  def chord(header_sigs, body_sig, opts \\ []), do: Canvas.chord(header_sigs, body_sig, opts)

  defp build_app(opts) do
    config = Config.new(opts)

    tasks =
      Enum.reduce(config.include, %{}, fn mod_or_str, acc ->
        loaded = Registry.load_include(mod_or_str)

        Enum.reduce(loaded, acc, fn defn, map_acc ->
          Map.put(map_acc, defn.name, defn)
        end)
      end)

    %__MODULE__{
      main: config.main,
      config: config,
      tasks: tasks
    }
  end
end
