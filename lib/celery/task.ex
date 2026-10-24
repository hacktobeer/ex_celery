defmodule Celery.Task.Definition do
  @moduledoc """
  Metadata and binding specification for a registered Celery task.
  """

  @type param_spec :: %{
          name: String.t(),
          raw_name: String.t(),
          has_default: boolean(),
          default: term()
        }

  @type t :: %__MODULE__{
          name: String.t(),
          module: module() | nil,
          function: atom() | nil,
          arity: non_neg_integer(),
          fun: function() | nil,
          bind: boolean(),
          metadata: term(),
          param_specs: [param_spec()],
          arg_names: [atom()],
          defaults: map(),
          acks_late: boolean() | nil,
          ack_on_failure_or_timeout: boolean() | nil,
          reject_on_worker_lost: boolean() | nil,
          track_started: boolean() | nil,
          ignore_result: boolean() | nil,
          store_errors_even_if_ignored: boolean() | nil,
          max_retries: non_neg_integer() | :infinity | nil,
          default_retry_delay: number(),
          autoretry_for: [module()],
          retry_backoff: boolean() | number(),
          retry_backoff_max: number(),
          retry_jitter: boolean(),
          serializer: String.t() | nil,
          compression: String.t() | nil,
          queue: String.t() | nil,
          exchange: String.t() | nil,
          routing_key: String.t() | nil,
          time_limit: number() | nil,
          soft_time_limit: number() | nil,
          rate_limit: String.t() | nil,
          typing: boolean(),
          options: keyword()
        }

  defstruct name: nil,
            module: nil,
            function: nil,
            arity: 0,
            fun: nil,
            bind: false,
            metadata: nil,
            param_specs: [],
            arg_names: [],
            defaults: %{},
            acks_late: nil,
            ack_on_failure_or_timeout: nil,
            reject_on_worker_lost: nil,
            track_started: nil,
            ignore_result: nil,
            store_errors_even_if_ignored: nil,
            max_retries: 3,
            default_retry_delay: 180,
            autoretry_for: [],
            retry_backoff: false,
            retry_backoff_max: 600,
            retry_jitter: true,
            serializer: nil,
            compression: nil,
            queue: nil,
            exchange: nil,
            routing_key: nil,
            time_limit: nil,
            soft_time_limit: nil,
            rate_limit: nil,
            typing: true,
            options: []
end

defmodule Celery.Task do
  @moduledoc """
  Celery Task behaviour, `@task` attribute decorator, `deftask` macro, and runtime `self` struct
  (ported from `celery.app.task.Task`).

  ## Defining Tasks in Elixir

  You can define tasks using either the `@task` module attribute (mirroring Python's
  `@celery.task(...)` decorator) or the `deftask` macro:

  ```elixir
  defmodule Src.Tasks do
    use Celery.Task

    @task_name "src.tasks.command"
    @task_metadata %{"owner" => "pipeline", "version" => 1}

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

  When `bind: true` is specified, the first argument (`self`) is a `%Celery.Task{}` struct
  containing:
  - `self.name` - the registered task name
  - `self.metadata` - the task metadata configured via `metadata: ...`
  - `self.request` - the `%Celery.Task.Context{}` for the current execution
  - `self.app` - the `%Celery{}` application instance
  """

  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Canvas.Signature
  alias Celery.Protocol
  alias Celery.States
  alias Celery.Task.Context
  alias Celery.Task.Definition

  @type t :: %__MODULE__{
          name: String.t(),
          metadata: term(),
          request: Context.t(),
          app: Celery.t() | nil,
          definition: Definition.t() | nil,
          max_retries: non_neg_integer() | :infinity | nil,
          default_retry_delay: number(),
          acks_late: boolean(),
          ignore_result: boolean(),
          track_started: boolean(),
          serializer: String.t()
        }

  defstruct name: nil,
            metadata: nil,
            request: %Context{},
            app: nil,
            definition: nil,
            max_retries: 3,
            default_retry_delay: 180,
            acks_late: false,
            ignore_result: false,
            track_started: false,
            serializer: "json"

  defmacro __using__(_opts) do
    quote do
      import Celery.Task, only: [deftask: 2, deftask: 3]

      Module.register_attribute(__MODULE__, :celery_raw_tasks, accumulate: true)
      Module.register_attribute(__MODULE__, :task, accumulate: false)

      @on_definition {Celery.Task, :__on_definition__}
      @before_compile {Celery.Task, :__before_compile__}
    end
  end

  @doc """
  Defines a Celery task function with options (`bind`, `name`, `metadata`, `max_retries`, etc.).
  """
  defmacro deftask(call, opts \\ [], do_block)

  defmacro deftask(call, [do: body], []) do
    quote do
      @task []
      def unquote(call) do
        unquote(body)
      end
    end
  end

  defmacro deftask(call, opts, do: body) do
    quote do
      @task unquote(opts)
      def unquote(call) do
        unquote(body)
      end
    end
  end

  @doc false
  def __on_definition__(env, :def, fun_name, args, _guards, _body) do
    case Module.get_attribute(env.module, :task) do
      nil ->
        :ok

      task_opts when is_list(task_opts) or is_map(task_opts) ->
        Module.delete_attribute(env.module, :task)
        opts = Enum.into(task_opts, [])
        raw_specs = extract_raw_param_specs(args)

        Module.put_attribute(env.module, :celery_raw_tasks, %{
          function: fun_name,
          arity: length(args),
          opts: opts,
          raw_specs: raw_specs
        })
    end
  end

  def __on_definition__(_env, _kind, _fun_name, _args, _guards, _body), do: :ok

  @doc false
  defmacro __before_compile__(env) do
    raw_tasks =
      env.module
      |> Module.get_attribute(:celery_raw_tasks)
      |> Enum.reverse()

    task_def_Exprs =
      Enum.map(raw_tasks, fn %{function: fun_name, arity: arity, opts: opts, raw_specs: raw_specs} ->
        spec_exprs =
          Enum.map(raw_specs, fn {canonical, raw, has_default, default_ast} ->
            quote do
              %{
                name: unquote(canonical),
                raw_name: unquote(raw),
                has_default: unquote(has_default),
                default: unquote(default_ast)
              }
            end
          end)

        quote do
          Celery.Task.build_definition(
            __MODULE__,
            unquote(fun_name),
            unquote(arity),
            unquote(Macro.escape(opts)),
            [unquote_splicing(spec_exprs)]
          )
        end
      end)

    quote do
      @doc false
      def __celery_tasks__ do
        [unquote_splicing(task_def_Exprs)]
      end

      @doc false
      def __celery_task__(fun_name) when is_atom(fun_name) do
        Enum.find(__celery_tasks__(), &(&1.function == fun_name))
      end

      @doc false
      def delay(app, fun_name, args \\ [], kwargs \\ %{}) when is_atom(fun_name) do
        defn =
          __celery_task__(fun_name) || raise ArgumentError, "Unknown task #{inspect(fun_name)}"

        Celery.Task.delay(app, defn, args, kwargs)
      end

      @doc false
      def apply_async(app, fun_name, opts \\ []) when is_atom(fun_name) do
        defn =
          __celery_task__(fun_name) || raise ArgumentError, "Unknown task #{inspect(fun_name)}"

        Celery.Task.apply_async(app, defn, opts)
      end

      @doc false
      def __after_compile__(_env, _bytecode) do
        Enum.each(__celery_tasks__(), &Celery.Registry.register/1)
      end

      @after_compile __MODULE__
    end
  end

  @doc false
  @spec build_definition(module(), atom(), non_neg_integer(), keyword(), [Definition.param_spec()]) ::
          Definition.t()
  def build_definition(module, function, arity, opts, param_specs) do
    default_name = "#{Celery.Registry.python_module_name(module)}.#{function}"
    name = Keyword.get(opts, :name) || default_name
    bind? = Keyword.get(opts, :bind, false) == true

    user_specs =
      case {bind?, param_specs} do
        {true, [_self_spec | rest]} -> rest
        {true, []} -> []
        {false, specs} -> specs
      end

    arg_names = Enum.map(user_specs, fn spec -> String.to_atom(spec.name) end)

    defaults =
      user_specs
      |> Enum.filter(& &1.has_default)
      |> Map.new(fn spec -> {String.to_atom(spec.name), spec.default} end)

    task_def = %Definition{
      name: to_string(name),
      module: module,
      function: function,
      arity: arity,
      fun: Keyword.get(opts, :fun),
      bind: bind?,
      metadata: Keyword.get(opts, :metadata),
      param_specs: param_specs,
      arg_names: arg_names,
      defaults: defaults,
      acks_late: Keyword.get(opts, :acks_late),
      ack_on_failure_or_timeout: Keyword.get(opts, :ack_on_failure_or_timeout),
      reject_on_worker_lost: Keyword.get(opts, :reject_on_worker_lost),
      track_started: Keyword.get(opts, :track_started),
      ignore_result: Keyword.get(opts, :ignore_result),
      store_errors_even_if_ignored: Keyword.get(opts, :store_errors_even_if_ignored),
      max_retries: Keyword.get(opts, :max_retries, 3),
      default_retry_delay: Keyword.get(opts, :default_retry_delay, 180),
      autoretry_for: Keyword.get(opts, :autoretry_for, []),
      retry_backoff: Keyword.get(opts, :retry_backoff, false),
      retry_backoff_max: Keyword.get(opts, :retry_backoff_max, 600),
      retry_jitter: Keyword.get(opts, :retry_jitter, true),
      serializer: Keyword.get(opts, :serializer),
      compression: Keyword.get(opts, :compression),
      queue: Keyword.get(opts, :queue),
      exchange: Keyword.get(opts, :exchange),
      routing_key: Keyword.get(opts, :routing_key),
      time_limit: Keyword.get(opts, :time_limit),
      soft_time_limit: Keyword.get(opts, :soft_time_limit),
      rate_limit: Keyword.get(opts, :rate_limit),
      typing: Keyword.get(opts, :typing, true),
      options: opts
    }

    Celery.Registry.register(task_def)
    task_def
  end

  @doc """
  Builds a `%Celery.Task{}` (`self`) instance for a `%Celery.Task.Definition{}` and
  a `%Celery.Task.Context{}` request.
  """
  @spec new_self(Definition.t(), Context.t(), Celery.t() | nil) :: t()
  def new_self(%Definition{} = defn, %Context{} = request, app \\ nil) do
    config = if app, do: app.config, else: %Celery.Config{}

    %__MODULE__{
      name: defn.name,
      metadata: defn.metadata,
      request: request,
      app: app,
      definition: defn,
      max_retries: defn.max_retries,
      default_retry_delay: defn.default_retry_delay,
      acks_late: if(is_nil(defn.acks_late), do: config.task_acks_late, else: defn.acks_late),
      ignore_result:
        if(is_nil(defn.ignore_result), do: config.task_ignore_result, else: defn.ignore_result),
      track_started:
        if(is_nil(defn.track_started), do: config.task_track_started, else: defn.track_started),
      serializer: defn.serializer || config.task_serializer
    }
  end

  @doc """
  Invokes a `%Celery.Task.Definition{}` with positional `args` (list), keyword `kwargs` (map),
  and `%Celery.Task.Context{}` (`request`), binding `self` as the first argument when `bind: true`
  and resolving positional, keyword, and default arguments just like Python Celery.
  """
  @spec invoke(Definition.t(), list(), map(), keyword()) :: term()
  def invoke(%Definition{} = defn, args \\ [], kwargs \\ %{}, opts \\ []) do
    request = Keyword.get_lazy(opts, :request, fn -> %Context{args: args, kwargs: kwargs} end)
    app = Keyword.get(opts, :app)
    self_task = new_self(defn, request, app)

    call_args = bind_call_arguments(defn, self_task, args, kwargs)

    cond do
      is_function(defn.fun) ->
        apply(defn.fun, call_args)

      not is_nil(defn.module) and not is_nil(defn.function) ->
        apply(defn.module, defn.function, call_args)

      true ->
        raise Celery.InvalidTaskError, message: "Task #{defn.name} has no callable function"
    end
  end

  @doc """
  Binds positional `args` and named `kwargs` to the task function's parameter list,
  prepending `self_task` if `defn.bind == true`.
  """
  @spec bind_call_arguments(Definition.t(), t(), list(), map()) :: list()
  def bind_call_arguments(%Definition{} = defn, %__MODULE__{} = self_task, args, kwargs) do
    norm_args = if is_list(args), do: args, else: List.wrap(args)
    norm_kwargs = normalize_kwargs_map(kwargs)

    user_specs =
      case {defn.bind, defn.param_specs} do
        {true, [_self_spec | rest]} -> rest
        {true, []} -> []
        {false, specs} -> specs
      end

    expected_user_arity =
      if defn.bind do
        max(defn.arity - 1, 0)
      else
        defn.arity
      end

    bound_user_args =
      cond do
        user_specs == [] and expected_user_arity == 0 ->
          []

        user_specs == [] ->
          # Anonymous function without named parameter specs:
          cond do
            length(norm_args) == expected_user_arity ->
              norm_args

            norm_args == [] and expected_user_arity == 1 and map_size(norm_kwargs) > 0 ->
              [norm_kwargs]

            length(norm_args) < expected_user_arity ->
              norm_args ++ List.duplicate(nil, expected_user_arity - length(norm_args))

            true ->
              Enum.take(norm_args, expected_user_arity)
          end

        length(norm_args) > length(user_specs) and defn.typing ->
          raise Celery.InvalidTaskError,
            message:
              "Task #{defn.name} takes at most #{length(user_specs)} arguments (#{length(norm_args)} given)"

        true ->
          user_specs
          |> Enum.with_index()
          |> Enum.map(fn {spec, idx} ->
            cond do
              idx < length(norm_args) ->
                Enum.at(norm_args, idx)

              Map.has_key?(norm_kwargs, spec.name) ->
                Map.get(norm_kwargs, spec.name)

              Map.has_key?(norm_kwargs, spec.raw_name) ->
                Map.get(norm_kwargs, spec.raw_name)

              spec.has_default ->
                spec.default

              length(user_specs) == 1 and norm_args == [] and map_size(norm_kwargs) > 0 ->
                norm_kwargs

              true ->
                raise Celery.InvalidTaskError,
                  message: "Task #{defn.name} missing required argument '#{spec.name}'"
            end
          end)
      end

    if defn.bind do
      [self_task | bound_user_args]
    else
      bound_user_args
    end
  end

  @doc """
  Retries the currently executing task (ported from `Task.retry` in `celery/app/task.py`).

  When called inside a bound task (`Celery.Task.retry(self, countdown: 5, exc: err)`),
  checks `max_retries`:
  - If `self.request.retries >= max_retries`, raises `%Celery.MaxRetriesExceededError{}`.
  - Otherwise, raises `%Celery.Retry{}` (when `throw: true`, the default) which the
    `Celery.Worker` / `Celery.Tracer` catches to re-publish the task with `retries + 1`
    and store the `RETRY` state in Redis.
  """
  @spec retry(t(), keyword()) :: no_return() | {:ok, Celery.Result.t()}
  def retry(%__MODULE__{} = self_task, opts \\ []) do
    request = self_task.request
    exc = Keyword.get(opts, :exc)
    throw? = Keyword.get(opts, :throw, true)
    max_retries = Keyword.get(opts, :max_retries, self_task.max_retries)
    current_retries = request.retries || 0

    if is_integer(max_retries) and current_retries >= max_retries do
      max_err =
        Celery.MaxRetriesExceededError.exception(
          task_name: self_task.name,
          task_id: request.id,
          args: Keyword.get(opts, :args, request.args),
          kwargs: Keyword.get(opts, :kwargs, request.kwargs)
        )

      raise max_err
    end

    countdown =
      Keyword.get_lazy(opts, :countdown, fn ->
        if Keyword.has_key?(opts, :eta), do: nil, else: self_task.default_retry_delay
      end)

    eta = Keyword.get(opts, :eta)
    when_val = eta || countdown

    retry_opts =
      opts
      |> Keyword.put(:task_id, request.id)
      |> Keyword.put(:retries, current_retries + 1)
      |> Keyword.put(:eta, Protocol.format_time_option(eta, countdown))
      |> Keyword.put_new(:root_id, request.root_id)
      |> Keyword.put_new(:parent_id, request.parent_id)
      |> Keyword.put_new(:group_id, request.group)
      |> Keyword.put_new(:group_index, request.group_index)
      |> Keyword.put_new(:chord, request.chord)
      |> Keyword.put_new(:chain, request.chain)
      |> Keyword.put_new(:callbacks, request.callbacks)
      |> Keyword.put_new(:errbacks, request.errbacks)

    retry_exc =
      Celery.Retry.exception(
        exc: exc,
        when: when_val,
        is_eager: request.is_eager,
        args: Keyword.get(opts, :args, request.args),
        kwargs: Keyword.get(opts, :kwargs, request.kwargs),
        options: retry_opts
      )

    if throw? do
      raise retry_exc
    else
      {:error, retry_exc}
    end
  end

  @doc """
  Updates the task's state and metadata in the Redis result backend directly from within
  a running task (ported from `Task.update_state` in `celery/app/task.py`).
  """
  @spec update_state(t(), keyword() | String.t(), map() | nil) ::
          {:ok, term()} | {:error, term()}
  def update_state(self_task, state_or_opts, meta \\ nil)

  def update_state(%__MODULE__{} = self_task, opts, nil) when is_list(opts) do
    task_id = Keyword.get(opts, :task_id, self_task.request.id)
    state = Keyword.fetch!(opts, :state)
    meta = Keyword.get(opts, :meta, %{})

    RedisBackend.store_result(self_task.app || %Celery.Config{}, task_id, meta, state,
      request: self_task.request
    )
  end

  def update_state(%__MODULE__{} = self_task, state, meta) when is_binary(state) do
    task_id = self_task.request.id

    RedisBackend.store_result(self_task.app || %Celery.Config{}, task_id, meta || %{}, state,
      request: self_task.request
    )
  end

  @doc """
  Creates a `%Celery.Canvas.Signature{}` for a task definition or task name (`task.s(...)`).
  """
  @spec s(Definition.t() | String.t(), list(), map() | keyword()) :: Signature.t()
  def s(task_or_name, args \\ [], kwargs \\ %{})

  def s(%Definition{name: name}, args, kwargs) do
    Signature.new(name, args: args, kwargs: kwargs)
  end

  def s(name, args, kwargs) when is_binary(name) do
    Signature.new(name, args: args, kwargs: kwargs)
  end

  @doc """
  Creates an immutable `%Celery.Canvas.Signature{}` (`task.si(...)`).
  """
  @spec si(Definition.t() | String.t(), list(), map() | keyword()) :: Signature.t()
  def si(task_or_name, args \\ [], kwargs \\ %{})

  def si(%Definition{name: name}, args, kwargs) do
    Signature.new(name, args: args, kwargs: kwargs, immutable: true)
  end

  def si(name, args, kwargs) when is_binary(name) do
    Signature.new(name, args: args, kwargs: kwargs, immutable: true)
  end

  @doc """
  Dispatches a task using positional `args` and keyword `kwargs` (`task.delay(...)`).
  """
  @spec delay(Celery.t(), Definition.t() | String.t(), list(), map() | keyword()) ::
          {:ok, Celery.Result.t()} | {:error, term()}
  def delay(app, task_or_name, args \\ [], kwargs \\ %{}) do
    apply_async(app, task_or_name, args: args, kwargs: kwargs)
  end

  @doc """
  Dispatches a task asynchronously (`task.apply_async(...)`).
  """
  @spec apply_async(Celery.t(), Definition.t() | String.t(), keyword()) ::
          {:ok, Celery.Result.t()} | {:error, term()}
  def apply_async(app, %Definition{} = defn, opts) do
    merged_opts =
      opts
      |> maybe_put_opt(:serializer, defn.serializer)
      |> maybe_put_opt(:compression, defn.compression)
      |> maybe_put_opt(:queue, defn.queue)
      |> maybe_put_opt(:exchange, defn.exchange)
      |> maybe_put_opt(:routing_key, defn.routing_key)
      |> maybe_put_opt(:time_limit, defn.time_limit)
      |> maybe_put_opt(:soft_time_limit, defn.soft_time_limit)
      |> maybe_put_opt(:ignore_result, defn.ignore_result)

    Celery.send_task(app, defn.name, merged_opts)
  end

  def apply_async(app, name, opts) when is_binary(name) do
    case Celery.Registry.lookup(name, app) do
      {:ok, defn} -> apply_async(app, defn, opts)
      {:error, _} -> Celery.send_task(app, name, opts)
    end
  end

  @doc """
  Executes a task locally in the calling process (`task.apply(...)`), returning
  `{:ok, %Celery.Result{is_eager: true}}`.
  """
  @spec apply_local(Celery.t(), Definition.t() | String.t(), list(), map() | keyword(), keyword()) ::
          {:ok, Celery.Result.t()}
  def apply_local(app, task_or_name, args \\ [], kwargs \\ %{}, opts \\ []) do
    defn =
      case task_or_name do
        %Definition{} = d -> d
        name when is_binary(name) -> Celery.Registry.fetch!(name, app)
      end

    norm_kwargs = normalize_kwargs_map(kwargs)
    task_id = Keyword.get_lazy(opts, :task_id, &Protocol.uuid/0)
    throw? = Keyword.get(opts, :throw, app.config.task_eager_propagates)

    ctx = %Context{
      id: task_id,
      task: defn.name,
      args: args,
      kwargs: norm_kwargs,
      retries: Keyword.get(opts, :retries, 0),
      is_eager: true,
      root_id: Keyword.get(opts, :root_id, task_id),
      parent_id: Keyword.get(opts, :parent_id),
      group: Keyword.get(opts, :group_id),
      group_index: Keyword.get(opts, :group_index),
      headers: Keyword.get(opts, :headers, %{}),
      called_directly: false
    }

    try do
      value = invoke(defn, args, norm_kwargs, request: ctx, app: app)

      if app.config.task_store_eager_result and
           not (defn.ignore_result || app.config.task_ignore_result) do
        RedisBackend.mark_as_done(app, task_id, value, request: ctx)
      end

      {:ok, Celery.Result.eager(task_id, value, States.success(), app: app)}
    rescue
      exc ->
        tb = Exception.format(:error, exc, __STACKTRACE__)

        if app.config.task_store_eager_result do
          RedisBackend.mark_as_failure(app, task_id, exc, traceback: tb, request: ctx)
        end

        if throw? do
          reraise exc, __STACKTRACE__
        else
          {:ok, Celery.Result.eager(task_id, exc, States.failure(), app: app, traceback: tb)}
        end
    end
  end

  # --- AST Parameter Extraction Helpers ---

  defp extract_raw_param_specs(args) when is_list(args) do
    Enum.map(args, fn
      {:\\, _meta, [param_ast, default_ast]} ->
        {canonical, raw} = extract_param_names(param_ast)
        {canonical, raw, true, default_ast}

      param_ast ->
        {canonical, raw} = extract_param_names(param_ast)
        {canonical, raw, false, nil}
    end)
  end

  defp extract_param_names({:=, _meta, [left, right]}) do
    case extract_var_name(right) do
      nil ->
        case extract_var_name(left) do
          nil -> {"arg", "arg"}
          name -> normalize_var_name(name)
        end

      name ->
        normalize_var_name(name)
    end
  end

  defp extract_param_names(ast) do
    case extract_var_name(ast) do
      nil -> {"arg", "arg"}
      name -> normalize_var_name(name)
    end
  end

  defp extract_var_name({name, _meta, ctx}) when is_atom(name) and is_atom(ctx) do
    Atom.to_string(name)
  end

  defp extract_var_name(_), do: nil

  defp normalize_var_name(raw_name) do
    canonical =
      case String.trim_leading(raw_name, "_") do
        "" -> raw_name
        trimmed -> trimmed
      end

    {canonical, raw_name}
  end

  defp normalize_kwargs_map(nil), do: %{}

  defp normalize_kwargs_map(kwargs) when is_list(kwargs) do
    Map.new(kwargs, fn {k, v} -> {to_string(k), v} end)
  end

  defp normalize_kwargs_map(kwargs) when is_map(kwargs) do
    Map.new(kwargs, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {to_string(k), v}
    end)
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, val), do: Keyword.put_new(opts, key, val)
end
