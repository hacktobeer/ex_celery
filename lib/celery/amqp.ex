defmodule Celery.AMQP do
  @moduledoc """
  AMQP / Kombu task routing, queue configuration, and task message publishing
  (ported from `celery.app.amqp`).
  """

  alias Celery.Config
  alias Celery.Protocol

  @doc """
  Returns the default `%Kombu.Queue{}` configured for the given `%Celery{}` or `%Celery.Config{}`.
  """
  @spec default_queue(Celery.t() | Config.t()) :: Kombu.Queue.t()
  def default_queue(%{config: %Config{} = config}), do: default_queue(config)

  def default_queue(%Config{} = config) do
    exchange =
      Kombu.Exchange.new(config.task_default_exchange,
        type: config.task_default_exchange_type,
        delivery_mode: config.task_default_delivery_mode
      )

    Kombu.Queue.new(config.task_default_queue,
      exchange: exchange,
      routing_key: config.task_default_routing_key
    )
  end

  @doc """
  Returns the list of `%Kombu.Queue{}` structs for a worker to consume from.
  """
  @spec queues_for_worker(Celery.t() | Config.t(), [String.t() | Kombu.Queue.t()] | nil) :: [
          Kombu.Queue.t()
        ]
  def queues_for_worker(%{config: %Config{} = config}, explicit_queues) do
    queues_for_worker(config, explicit_queues)
  end

  def queues_for_worker(%Config{} = config, explicit_queues) do
    cond do
      is_list(explicit_queues) and explicit_queues != [] ->
        Enum.map(explicit_queues, &coerce_queue(&1, config))

      is_list(config.task_queues) and config.task_queues != [] ->
        Enum.map(config.task_queues, &coerce_queue(&1, config))

      true ->
        [default_queue(config)]
    end
  end

  @doc """
  Resolves routing parameters (`exchange`, `routing_key`, `queue`, `serializer`, `compression`, `delivery_mode`, `priority`)
  for a task invocation.
  """
  @spec resolve_route(Celery.t() | Config.t(), String.t(), keyword()) :: map()
  def resolve_route(%{config: %Config{} = config}, task_name, opts) do
    resolve_route(config, task_name, opts)
  end

  def resolve_route(%Config{} = config, task_name, opts) do
    route_cfg = lookup_task_route(config.task_routes, task_name)

    queue_opt = Keyword.get(opts, :queue) || Map.get(route_cfg, :queue)
    exchange_opt = Keyword.get(opts, :exchange) || Map.get(route_cfg, :exchange)
    routing_key_opt = Keyword.get(opts, :routing_key) || Map.get(route_cfg, :routing_key)

    {exchange, routing_key, queue} =
      cond do
        match?(%Kombu.Queue{}, queue_opt) ->
          q = queue_opt

          ex =
            if q.exchange,
              do: q.exchange.name,
              else: exchange_opt || config.task_default_exchange

          rk =
            if q.routing_key != "",
              do: q.routing_key,
              else: routing_key_opt || q.name

          {ex, rk, q}

        is_binary(queue_opt) and queue_opt != "" ->
          q = coerce_queue(queue_opt, config)
          ex = exchange_opt || (q.exchange && q.exchange.name) || queue_opt
          rk = routing_key_opt || (q.routing_key != "" && q.routing_key) || queue_opt
          {ex, rk, q}

        true ->
          def_q = default_queue(config)
          ex = exchange_opt || config.task_default_exchange
          rk = routing_key_opt || config.task_default_routing_key
          {ex, rk, def_q}
      end

    %{
      exchange: exchange,
      routing_key: routing_key,
      queue: queue,
      serializer: Keyword.get(opts, :serializer, config.task_serializer),
      compression: Keyword.get(opts, :compression, config.task_compression),
      delivery_mode: Keyword.get(opts, :delivery_mode, config.task_default_delivery_mode),
      priority: Keyword.get(opts, :priority)
    }
  end

  @doc """
  Creates a task message (`as_task_v2` or `as_task_v1`) according to the app's `task_protocol`
  setting (or `:protocol` option).
  """
  @spec create_task_message(Celery.t() | Config.t(), String.t(), list(), map(), keyword()) ::
          Protocol.task_message()
  def create_task_message(%{config: %Config{} = config}, name, args, kwargs, opts) do
    create_task_message(config, name, args, kwargs, opts)
  end

  def create_task_message(%Config{} = config, name, args, kwargs, opts) do
    protocol = Keyword.get(opts, :protocol, config.task_protocol)

    merged_opts =
      opts
      |> Keyword.put_new(:time_limit, config.task_time_limit)
      |> Keyword.put_new(:soft_time_limit, config.task_soft_time_limit)
      |> Keyword.put_new(:ignore_result, config.task_ignore_result)

    case protocol do
      1 -> Protocol.as_task_v1(name, args, kwargs, merged_opts)
      _ -> Protocol.as_task_v2(name, args, kwargs, merged_opts)
    end
  end

  @doc """
  Publishes a prepared task message (`%{headers: ..., properties: ..., body: ...}`)
  over Kombu using the configured broker connection.
  """
  @spec send_task_message(Celery.t() | Config.t(), Protocol.task_message(), keyword()) ::
          :ok | {:error, term()}
  def send_task_message(%{config: %Config{} = config}, task_message, opts) do
    send_task_message(config, task_message, opts)
  end

  def send_task_message(%Config{} = config, task_message, opts) do
    task_name =
      Map.get(task_message.headers, "task") ||
        (is_map(task_message.body) && Map.get(task_message.body, "task")) || ""

    route = resolve_route(config, task_name, opts)
    conn = build_connection(config)

    publish_opts =
      [
        exchange: route.exchange,
        routing_key: route.routing_key,
        serializer: route.serializer,
        compression: route.compression,
        headers: task_message.headers,
        correlation_id: Map.get(task_message.properties, "correlation_id"),
        reply_to: Map.get(task_message.properties, "reply_to"),
        delivery_mode: route.delivery_mode,
        priority: route.priority,
        declare: [route.queue],
        retry: Keyword.get(opts, :retry, config.task_publish_retry)
      ]
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)

    try do
      producer = Kombu.Producer.new(conn)
      Kombu.Producer.publish(producer, task_message.body, publish_opts)
    after
      Kombu.Connection.release(conn)
    end

    :ok
  end

  @doc """
  Builds a `%Kombu.Connection{}` for the given `%Celery{}` or `%Celery.Config{}`.
  """
  @spec build_connection(Celery.t() | Config.t()) :: Kombu.Connection.t()
  def build_connection(%{config: %Config{} = config}), do: build_connection(config)

  def build_connection(%Config{} = config) do
    normalized_conn = normalize_injected_conn(config.redis_conn)

    transport_opts =
      config.broker_transport_options
      |> Enum.into([])
      |> maybe_put_redis_conn(normalized_conn)

    conn_opts =
      [transport_options: transport_opts]
      |> maybe_put_conn_opt(normalized_conn)

    Kombu.Connection.new(config.broker_url, conn_opts)
  end

  defp normalize_injected_conn(nil), do: nil

  defp normalize_injected_conn(conn) when is_pid(conn) do
    case Process.info(conn, :dictionary) do
      {:dictionary, dict} ->
        case Keyword.get(dict, :"$initial_call") do
          {mod, :init, 1} when mod != Redix.Connection -> {mod, conn}
          _ -> conn
        end

      _ ->
        conn
    end
  end

  defp normalize_injected_conn(other), do: other

  defp maybe_put_redis_conn(opts, nil), do: opts
  defp maybe_put_redis_conn(opts, conn), do: Keyword.put_new(opts, :redis_conn, conn)

  defp maybe_put_conn_opt(opts, nil), do: opts
  defp maybe_put_conn_opt(opts, conn), do: Keyword.put(opts, :conn, conn)

  defp coerce_queue(%Kombu.Queue{} = q, _config), do: q

  defp coerce_queue(name, %Config{} = config) when is_binary(name) do
    if name == config.task_default_queue do
      default_queue(config)
    else
      ex =
        Kombu.Exchange.new(name,
          type: "direct",
          delivery_mode: config.task_default_delivery_mode
        )

      Kombu.Queue.new(name, exchange: ex, routing_key: name)
    end
  end

  defp lookup_task_route(nil, _task_name), do: %{}

  defp lookup_task_route(routes, task_name) when is_map(routes) or is_list(routes) do
    routes_map = Enum.into(routes, %{})

    case Map.get(routes_map, task_name) do
      nil ->
        %{}

      queue_name when is_binary(queue_name) ->
        %{queue: queue_name}

      route_opts when is_list(route_opts) or is_map(route_opts) ->
        Map.new(route_opts, fn
          {k, v} when is_binary(k) -> {String.to_atom(k), v}
          {k, v} -> {k, v}
        end)
    end
  end
end
