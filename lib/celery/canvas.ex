defmodule Celery.Canvas.Signature do
  @moduledoc """
  Serializable task invocation signature (`celery.canvas.Signature` / `subtask`).

  Matches Python Celery's wire dictionary representation so signatures can be passed in
  `callbacks`, `errbacks`, `chain`, and `chord` fields between Python and Elixir workers:
  ```elixir
  %{
    "task" => "src.tasks.command",
    "args" => [...],
    "kwargs" => %{...},
    "options" => %{...},
    "subtask_type" => nil,
    "immutable" => false
  }
  ```
  """

  @type t :: %__MODULE__{
          task: String.t(),
          args: list(),
          kwargs: map(),
          options: map(),
          subtask_type: String.t() | nil,
          immutable: boolean(),
          app: Celery.t() | nil
        }

  defstruct task: nil,
            args: [],
            kwargs: %{},
            options: %{},
            subtask_type: nil,
            immutable: false,
            app: nil

  @doc """
  Creates a new `%Celery.Canvas.Signature{}`.
  """
  @spec new(String.t(), keyword()) :: t()
  def new(task_name, opts \\ []) when is_binary(task_name) do
    args = Keyword.get(opts, :args, [])
    kwargs = normalize_map(Keyword.get(opts, :kwargs, %{}))
    options = normalize_map(Keyword.get(opts, :options, %{}))
    immutable = Keyword.get(opts, :immutable, false)
    subtask_type = Keyword.get(opts, :subtask_type)
    app = Keyword.get(opts, :app)

    %__MODULE__{
      task: task_name,
      args: args,
      kwargs: kwargs,
      options: options,
      subtask_type: subtask_type,
      immutable: immutable,
      app: app
    }
  end

  @doc """
  Serializes a `%Celery.Canvas.Signature{}` to a Python Celery wire-compatible map.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = sig) do
    %{
      "task" => sig.task,
      "args" => sig.args,
      "kwargs" => normalize_map(sig.kwargs),
      "options" => normalize_map(sig.options),
      "subtask_type" => sig.subtask_type,
      "immutable" => sig.immutable,
      "chord_size" => Map.get(sig.options, "chord_size")
    }
  end

  @doc """
  Reconstructs a `%Celery.Canvas.Signature{}` from a wire map.
  """
  @spec from_map(map() | t()) :: t()
  def from_map(%__MODULE__{} = sig), do: sig

  def from_map(%{} = map) do
    str_map = normalize_map(map)
    options = normalize_map(Map.get(str_map, "options") || %{})

    options =
      case Map.get(str_map, "chord_size") do
        nil -> options
        size -> Map.put_new(options, "chord_size", size)
      end

    %__MODULE__{
      task: Map.get(str_map, "task"),
      args: Map.get(str_map, "args") || [],
      kwargs: normalize_map(Map.get(str_map, "kwargs") || %{}),
      options: options,
      subtask_type: Map.get(str_map, "subtask_type"),
      immutable: Map.get(str_map, "immutable", false) == true
    }
  end

  @doc """
  Freezes a signature by assigning a `task_id` in its `options` if not already set,
  returning `{updated_signature, %Celery.Result{}}`.
  """
  @spec freeze(t(), keyword()) :: {t(), Celery.Result.t()}
  def freeze(%__MODULE__{} = sig, opts \\ []) do
    tid =
      Keyword.get(opts, :task_id) ||
        Map.get(sig.options, "task_id") ||
        Celery.Protocol.uuid()

    group_id = Keyword.get(opts, :group_id) || Map.get(sig.options, "group_id")
    group_index = Keyword.get(opts, :group_index) || Map.get(sig.options, "group_index")

    new_options =
      sig.options
      |> Map.put("task_id", tid)
      |> maybe_put("group_id", group_id)
      |> maybe_put("group_index", group_index)

    frozen_sig = %{sig | options: new_options}
    result = Celery.Result.new(tid, app: Keyword.get(opts, :app, sig.app))
    {frozen_sig, result}
  end

  @doc """
  Dispatches a `%Celery.Canvas.Signature{}` asynchronously via `Celery.send_task/3`.
  """
  @spec apply_async(t(), keyword()) :: {:ok, Celery.Result.t()} | {:error, term()}
  def apply_async(%__MODULE__{} = sig, extra_opts \\ []) do
    app = Keyword.get(extra_opts, :app) || sig.app || Celery.new()

    sig_opts =
      sig.options
      |> Enum.map(fn {k, v} -> {String.to_atom(to_string(k)), v} end)
      |> Keyword.merge(extra_opts)
      |> Keyword.put(:args, Keyword.get(extra_opts, :args, sig.args))
      |> Keyword.put(
        :kwargs,
        Map.merge(
          sig.kwargs,
          Map.new(Keyword.get(extra_opts, :kwargs, %{}), fn {k, v} -> {to_string(k), v} end)
        )
      )

    Celery.send_task(app, sig.task, sig_opts)
  end

  @doc """
  Dispatches a `%Celery.Canvas.Signature{}` with positional/keyword arguments (`delay`).
  """
  @spec delay(t(), list(), map() | keyword()) :: {:ok, Celery.Result.t()} | {:error, term()}
  def delay(%__MODULE__{} = sig, partial_args \\ [], partial_kwargs \\ %{}) do
    args =
      if sig.immutable do
        sig.args
      else
        partial_args ++ sig.args
      end

    kwargs =
      if sig.immutable do
        sig.kwargs
      else
        Map.merge(sig.kwargs, normalize_map(partial_kwargs))
      end

    apply_async(sig, args: args, kwargs: kwargs)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, val), do: Map.put(map, key, val)

  defp normalize_map(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {to_string(k), v}
    end)
  end

  defp normalize_map(list) when is_list(list) do
    Map.new(list, fn {k, v} -> {to_string(k), v} end)
  end
end

defmodule Celery.Canvas do
  @moduledoc """
  Celery Canvas workflow primitives (`signature`, `chain`, `group`, `chord`)
  compatible with Python Celery's wire protocol.
  """

  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Canvas.Signature
  alias Celery.Protocol
  alias Celery.Result
  alias Celery.Result.GroupResult

  @doc """
  Creates a `%Celery.Canvas.Signature{}` (`s/2`).
  """
  @spec s(String.t() | map() | Signature.t(), keyword()) :: Signature.t()
  def s(task_or_sig, opts \\ []), do: signature(task_or_sig, opts)

  @spec signature(String.t() | map() | Signature.t(), keyword()) :: Signature.t()
  def signature(task_or_sig, opts \\ [])
  def signature(%Signature{} = sig, _opts), do: sig
  def signature(%{} = map, _opts), do: Signature.from_map(map)

  def signature(task_name, opts) when is_binary(task_name) do
    Signature.new(task_name, opts)
  end

  @doc """
  Creates an immutable `%Celery.Canvas.Signature{}` (`si/2`).
  """
  @spec si(String.t(), keyword()) :: Signature.t()
  def si(task_name, opts \\ []) when is_binary(task_name) do
    Signature.new(task_name, Keyword.put(opts, :immutable, true))
  end

  @doc """
  Executes a sequential chain of `%Celery.Canvas.Signature{}` tasks.

  In Celery Protocol v2, the first task in the chain is published with the remaining tasks
  stored in its `chain` embed list (in reverse order, as Python Celery pops from the end
  of `request.chain`!). Returns `{:ok, last_task_result}` with `parent` links pointing up
  the chain.
  """
  @spec chain([Signature.t()], keyword()) :: {:ok, Result.t()} | {:error, term()}
  def chain([single], opts) do
    Signature.apply_async(single, opts)
  end

  def chain([_ | _] = sigs, opts) do
    app = Keyword.get(opts, :app) || hd(sigs).app || Celery.new()

    {frozen_sigs, results} =
      Enum.reduce(sigs, {[], []}, fn sig, {acc_sigs, acc_results} ->
        parent_res = List.first(acc_results)
        {f_sig, res} = Signature.freeze(sig, app: app)
        res = %{res | parent: parent_res}
        {[f_sig | acc_sigs], [res | acc_results]}
      end)

    frozen_sigs = Enum.reverse(frozen_sigs)
    results = Enum.reverse(results)

    [first_sig | rest_sigs] = frozen_sigs
    first_tid = Map.fetch!(first_sig.options, "task_id")

    # Python Celery stores `chain` in reverse order because workers `pop()` the next task
    # from the end of `request.chain`.
    chain_embed =
      rest_sigs
      |> Enum.reverse()
      |> Enum.map(&Signature.to_map/1)

    send_opts =
      first_sig.options
      |> Enum.map(fn {k, v} -> {String.to_atom(to_string(k)), v} end)
      |> Keyword.merge(opts)
      |> Keyword.put(:task_id, first_tid)
      |> Keyword.put(:root_id, first_tid)
      |> Keyword.put(:args, first_sig.args)
      |> Keyword.put(:kwargs, first_sig.kwargs)
      |> Keyword.put(:chain, chain_embed)

    case Celery.send_task(app, first_sig.task, send_opts) do
      {:ok, _first_res} -> {:ok, List.last(results)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Dispatches a parallel group of `%Celery.Canvas.Signature{}` tasks and returns
  `{:ok, %Celery.Result.GroupResult{}}`.
  """
  @spec group([Signature.t()], keyword()) :: {:ok, GroupResult.t()} | {:error, term()}
  def group(sigs, opts \\ []) when is_list(sigs) do
    app = Keyword.get(opts, :app) || (sigs != [] && hd(sigs).app) || Celery.new()
    group_id = Keyword.get_lazy(opts, :group_id, &Protocol.uuid/0)

    results =
      sigs
      |> Enum.with_index()
      |> Enum.map(fn {sig, idx} ->
        {f_sig, _res} = Signature.freeze(sig, app: app, group_id: group_id, group_index: idx)

        {:ok, res} =
          Signature.apply_async(f_sig,
            app: app,
            task_id: Map.fetch!(f_sig.options, "task_id"),
            group_id: group_id,
            group_index: idx
          )

        res
      end)

    {:ok, GroupResult.new(group_id, results, app: app)}
  end

  @doc """
  Dispatches a chord (a parallel header of `%Celery.Canvas.Signature{}` tasks followed by a
  body callback `%Celery.Canvas.Signature{}` that receives the list of header results).
  Returns `{:ok, callback_result}`.
  """
  @spec chord([Signature.t()], Signature.t(), keyword()) ::
          {:ok, Result.t()} | {:error, term()}
  def chord(header_sigs, %Signature{} = body_sig, opts \\ []) when is_list(header_sigs) do
    app = Keyword.get(opts, :app) || body_sig.app || Celery.new()
    group_id = Keyword.get_lazy(opts, :group_id, &Protocol.uuid/0)
    chord_size = length(header_sigs)

    {frozen_body, body_res} = Signature.freeze(body_sig, app: app)

    frozen_body = %{
      frozen_body
      | options:
          frozen_body.options
          |> Map.put("group_id", group_id)
          |> Map.put("chord_size", chord_size)
    }

    chord_map = Signature.to_map(frozen_body)
    RedisBackend.set_chord_size(app, group_id, chord_size)

    header_results =
      header_sigs
      |> Enum.with_index()
      |> Enum.map(fn {sig, idx} ->
        {f_sig, _res} = Signature.freeze(sig, app: app, group_id: group_id, group_index: idx)

        {:ok, res} =
          Signature.apply_async(f_sig,
            app: app,
            task_id: Map.fetch!(f_sig.options, "task_id"),
            group_id: group_id,
            group_index: idx,
            chord: chord_map
          )

        res
      end)

    group_result = GroupResult.new(group_id, header_results, app: app)
    {:ok, %{body_res | parent: group_result}}
  end
end
