defmodule Celery.Registry do
  @moduledoc """
  Task registry for Celery applications (ported from `celery.app.registry.TaskRegistry`).

  Maintains a global ETS table mapping task names to `%Celery.Task.Definition{}` structs,
  while also supporting per-app task maps on `%Celery{}`.
  """
  use GenServer

  @table :celery_task_registry

  @doc "Starts the global Celery task registry."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers a `%Celery.Task.Definition{}` in the global registry.
  """
  @spec register(map()) :: :ok
  def register(%{name: name} = task_def) when is_binary(name) do
    ensure_table!()
    :ets.insert(@table, {name, task_def})
    :ok
  end

  @doc """
  Unregisters a task by name from the global registry.
  """
  @spec unregister(String.t()) :: :ok
  def unregister(name) when is_binary(name) do
    ensure_table!()
    :ets.delete(@table, name)
    :ok
  end

  @doc """
  Looks up a task definition by name in an optional app's local registry or the global registry.
  """
  @spec lookup(String.t(), map() | nil) :: {:ok, map()} | {:error, Celery.NotRegistered.t()}
  def lookup(name, app \\ nil) when is_binary(name) do
    case lookup_in_app(name, app) do
      {:ok, task_def} ->
        {:ok, task_def}

      :error ->
        ensure_table!()

        case :ets.lookup(@table, name) do
          [{^name, task_def}] -> {:ok, task_def}
          [] -> {:error, Celery.NotRegistered.exception(name)}
        end
    end
  end

  @doc """
  Fetches a task definition by name or raises `Celery.NotRegistered`.
  """
  @spec fetch!(String.t(), map() | nil) :: map()
  def fetch!(name, app \\ nil) when is_binary(name) do
    case lookup(name, app) do
      {:ok, task_def} -> task_def
      {:error, err} -> raise err
    end
  end

  @doc """
  Returns all registered tasks as a map of `%{name => task_def}`, merging global and app-specific tasks.
  """
  @spec all(map() | nil) :: %{String.t() => map()}
  def all(app \\ nil) do
    ensure_table!()

    global =
      @table
      |> :ets.tab2list()
      |> Map.new()

    app_tasks =
      case app do
        %{tasks: tasks} when is_map(tasks) -> tasks
        _ -> %{}
      end

    Map.merge(global, app_tasks)
  end

  @doc """
  Returns a sorted list of all registered task names.
  """
  @spec keys(map() | nil) :: [String.t()]
  def keys(app \\ nil) do
    app
    |> all()
    |> Map.keys()
    |> Enum.sort()
  end

  @doc """
  Resolves a module from either an Elixir module atom (`Src.Tasks`) or a Python-style
  dotted module string (`"src.tasks"` -> `Src.Tasks`), loads its `__celery_tasks__/0`
  definitions if present, and registers them.
  """
  @spec load_include(module() | String.t()) :: [map()]
  def load_include(mod) when is_atom(mod) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :__celery_tasks__, 0) do
      tasks = mod.__celery_tasks__()
      Enum.each(tasks, &register/1)
      tasks
    else
      []
    end
  end

  def load_include(mod_str) when is_binary(mod_str) do
    candidates = module_candidates(mod_str)

    Enum.find_value(candidates, [], fn candidate ->
      if Code.ensure_loaded?(candidate) and function_exported?(candidate, :__celery_tasks__, 0) do
        tasks = candidate.__celery_tasks__()
        Enum.each(tasks, &register/1)
        tasks
      else
        nil
      end
    end)
  end

  @doc """
  Converts a Python-style dotted module name (e.g. `"src.tasks"`) or Elixir module string
  (e.g. `"Src.Tasks"`, `"Elixir.Src.Tasks"`) into candidate Elixir module atoms.
  """
  @spec module_candidates(String.t()) :: [module()]
  def module_candidates(mod_str) when is_binary(mod_str) do
    camelized =
      mod_str
      |> String.trim_leading("Elixir.")
      |> String.split(".")
      |> Enum.map(&Macro.camelize/1)
      |> Module.concat()

    raw =
      mod_str
      |> String.trim_leading("Elixir.")
      |> String.split(".")
      |> Module.concat()

    Enum.uniq([camelized, raw])
  end

  @doc """
  Converts an Elixir module atom into a Python-style dotted module string
  (e.g. `Src.Tasks` -> `"src.tasks"`).
  """
  @spec python_module_name(module()) :: String.t()
  def python_module_name(mod) when is_atom(mod) do
    mod
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
    |> String.split(".")
    |> Enum.map_join(".", &Macro.underscore/1)
  end

  defp lookup_in_app(name, %{tasks: tasks}) when is_map(tasks) do
    Map.fetch(tasks, name)
  end

  defp lookup_in_app(_name, _app), do: :error

  defp ensure_table! do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
        rescue
          ArgumentError -> @table
        end

      _tid ->
        @table
    end
  end

  @impl true
  def init(_opts) do
    ensure_table!()
    {:ok, %{}}
  end
end
