defmodule Celery.Config do
  @moduledoc """
  Celery configuration structure and defaults (ported from `celery.app.defaults`).

  Supports only Redis for broker transport and result backend (plus `memory://`
  via Kombu and `FakeRedis` for local unit testing).
  """

  @type t :: %__MODULE__{
          main: String.t(),
          broker_url: String.t(),
          broker_transport_options: keyword() | map(),
          result_backend: String.t() | nil,
          result_backend_transport_options: keyword() | map(),
          result_serializer: String.t(),
          result_compression: String.t() | nil,
          result_expires: non_neg_integer() | nil,
          result_extended: boolean(),
          result_chord_ordered: boolean(),
          result_chord_join_timeout: number(),
          include: [module() | String.t()],
          accept_content: [String.t()],
          task_serializer: String.t(),
          task_compression: String.t() | nil,
          task_protocol: 1 | 2,
          task_default_queue: String.t(),
          task_default_exchange: String.t(),
          task_default_exchange_type: String.t(),
          task_default_routing_key: String.t(),
          task_default_delivery_mode: 1 | 2,
          task_queues: [Kombu.Queue.t()] | nil,
          task_routes: map() | keyword(),
          task_acks_late: boolean(),
          task_ack_on_failure_or_timeout: boolean(),
          task_reject_on_worker_lost: boolean(),
          task_track_started: boolean(),
          task_ignore_result: boolean(),
          task_store_errors_even_if_ignored: boolean(),
          task_always_eager: boolean(),
          task_eager_propagates: boolean(),
          task_store_eager_result: boolean(),
          task_time_limit: number() | nil,
          task_soft_time_limit: number() | nil,
          task_publish_retry: boolean(),
          worker_concurrency: pos_integer(),
          worker_prefetch_multiplier: pos_integer(),
          worker_enable_remote_control: boolean(),
          worker_send_task_events: boolean(),
          worker_heartbeat_interval: non_neg_integer() | nil,
          timezone: String.t(),
          enable_utc: boolean(),
          redis_conn: GenServer.server() | nil
        }

  defstruct main: "celery",
            broker_url: "redis://localhost:6379/0",
            broker_transport_options: [],
            result_backend: nil,
            result_backend_transport_options: [],
            result_serializer: "json",
            result_compression: nil,
            result_expires: 86_400,
            result_extended: false,
            result_chord_ordered: true,
            result_chord_join_timeout: 3.0,
            include: [],
            accept_content: ["json"],
            task_serializer: "json",
            task_compression: nil,
            task_protocol: 2,
            task_default_queue: "celery",
            task_default_exchange: "celery",
            task_default_exchange_type: "direct",
            task_default_routing_key: "celery",
            task_default_delivery_mode: 2,
            task_queues: nil,
            task_routes: %{},
            task_acks_late: false,
            task_ack_on_failure_or_timeout: true,
            task_reject_on_worker_lost: false,
            task_track_started: false,
            task_ignore_result: false,
            task_store_errors_even_if_ignored: false,
            task_always_eager: false,
            task_eager_propagates: false,
            task_store_eager_result: false,
            task_time_limit: nil,
            task_soft_time_limit: nil,
            task_publish_retry: true,
            worker_concurrency: System.schedulers_online(),
            worker_prefetch_multiplier: 4,
            worker_enable_remote_control: true,
            worker_send_task_events: true,
            worker_heartbeat_interval: 5_000,
            timezone: "UTC",
            enable_utc: true,
            redis_conn: nil

  @valid_broker_schemes ["redis", "rediss", "memory"]
  @valid_backend_schemes ["redis", "rediss"]

  @doc """
  Builds a normalized `%Celery.Config{}` from keyword options or a map.

  Accepts both Python-style shorthand keys (`:broker`, `:backend`, `:changes`)
  and canonical Celery configuration keys (`:broker_url`, `:result_backend`).
  """
  @spec new(keyword() | map()) :: t()
  def new(opts \\ []) do
    opts_map =
      opts
      |> Enum.into(%{})
      |> normalize_aliases()

    base = struct(__MODULE__, Map.to_list(opts_map))

    validate_broker_url!(base.broker_url)
    validate_result_backend!(base.result_backend)

    base
  end

  @doc """
  Updates an existing `%Celery.Config{}` with new settings.
  """
  @spec update(t(), keyword() | map()) :: t()
  def update(%__MODULE__{} = config, changes) do
    changes_map =
      changes
      |> Enum.into(%{})
      |> normalize_aliases()

    updated = Map.merge(config, changes_map)
    validate_broker_url!(updated.broker_url)
    validate_result_backend!(updated.result_backend)
    updated
  end

  @doc """
  Validates that the broker URL uses Redis (or `memory://` for testing).
  Raises `ArgumentError` for unsupported transports.
  """
  @spec validate_broker_url!(String.t() | nil) :: :ok
  def validate_broker_url!(nil), do: :ok

  def validate_broker_url!(url) when is_binary(url) do
    uri = URI.parse(url)
    scheme = uri.scheme || "redis"

    if scheme in @valid_broker_schemes do
      :ok
    else
      raise ArgumentError,
            "Unsupported broker transport #{inspect(scheme)} in #{inspect(url)}. " <>
              "Celery Elixir only supports Redis (`redis://`, `rediss://`) and `memory://`."
    end
  end

  @doc """
  Validates that the result backend URL uses Redis (or `nil` when disabled).
  Raises `ArgumentError` for unsupported backends.
  """
  @spec validate_result_backend!(String.t() | nil) :: :ok
  def validate_result_backend!(nil), do: :ok

  def validate_result_backend!(url) when is_binary(url) do
    uri = URI.parse(url)
    scheme = uri.scheme || "redis"

    if scheme in @valid_backend_schemes do
      :ok
    else
      raise ArgumentError,
            "Unsupported result backend #{inspect(scheme)} in #{inspect(url)}. " <>
              "Celery Elixir only supports Redis (`redis://`, `rediss://`)."
    end
  end

  defp normalize_aliases(map) do
    map
    |> maybe_alias(:broker, :broker_url)
    |> maybe_alias(:backend, :result_backend)
    |> maybe_merge_changes()
  end

  @doc """
  Converts a `%Celery.Config{}` into a JSON-serializable map suitable for `inspect().conf()`.
  """
  @spec to_json_map(t()) :: map()
  def to_json_map(%__MODULE__{} = config) do
    config
    |> Map.from_struct()
    |> Map.delete(:redis_conn)
    |> Enum.into(%{}, fn {k, v} -> {Atom.to_string(k), sanitize_json_val(v)} end)
  end

  defp sanitize_json_val(val) when is_pid(val) or is_reference(val) or is_function(val),
    do: inspect(val)

  defp sanitize_json_val(%Kombu.Queue{} = q), do: Kombu.Queue.as_dict(q, recurse: true)
  defp sanitize_json_val(%Kombu.Exchange{} = ex), do: Kombu.Exchange.as_dict(ex, recurse: true)
  defp sanitize_json_val(%_{} = struct), do: struct |> Map.from_struct() |> sanitize_json_val()

  defp sanitize_json_val(list) when is_list(list) do
    if Keyword.keyword?(list) and list != [] do
      Map.new(list, fn {k, v} -> {to_string(k), sanitize_json_val(v)} end)
    else
      Enum.map(list, &sanitize_json_val/1)
    end
  end

  defp sanitize_json_val(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {to_string(k), sanitize_json_val(v)} end)
  end

  defp sanitize_json_val(tuple) when is_tuple(tuple) do
    tuple |> Tuple.to_list() |> Enum.map(&sanitize_json_val/1)
  end

  defp sanitize_json_val(other), do: other

  defp maybe_alias(map, from_key, to_key) do
    cond do
      Map.has_key?(map, from_key) and not Map.has_key?(map, to_key) ->
        val = Map.get(map, from_key)
        map |> Map.delete(from_key) |> Map.put(to_key, val)

      Map.has_key?(map, from_key) ->
        Map.delete(map, from_key)

      true ->
        map
    end
  end

  defp maybe_merge_changes(%{changes: changes} = map) when is_list(changes) or is_map(changes) do
    changes_map = Enum.into(changes, %{}) |> normalize_aliases()
    map |> Map.delete(:changes) |> Map.merge(changes_map)
  end

  defp maybe_merge_changes(map), do: map
end
