defmodule Celery.Events do
  @moduledoc """
  Celery event creation, formatting, and publishing to the `celeryev` topic exchange
  (ported from `celery.events` and `celery.events.dispatcher`).

  Emits worker (`worker-online`, `worker-heartbeat`, `worker-offline`) and task lifecycle
  events (`task-received`, `task-started`, `task-succeeded`, `task-failed`, `task-retried`,
  `task-revoked`) compatible with Flower, `celery.events.EventReceiver`, and OpenRelik Mediator.
  """

  alias Kombu.{Exchange, Producer}

  @event_exchange_name "celeryev"

  @doc """
  Returns the `celeryev` exchange (`durable: true`).
  Matches Python Celery's `celery.events.event.get_exchange/2`, which switches
  the exchange type from `"topic"` to `"fanout"` when Redis is used as the transport.
  """
  @spec exchange(term()) :: Exchange.t()
  def exchange(channel_or_conn \\ nil) do
    type = if redis_transport?(channel_or_conn), do: "fanout", else: "topic"
    Exchange.new(@event_exchange_name, type: type, durable: true)
  end

  defp redis_transport?(%Kombu.Connection{transport: t})
       when t in ["redis", "rediss", :redis, :rediss, Kombu.Transport.Redis],
       do: true

  defp redis_transport?(pid) when is_pid(pid) do
    case Process.info(pid, :dictionary) do
      {:dictionary, dict} ->
        Keyword.get(dict, :"$initial_call") == {Kombu.Transport.Redis.Channel, :init, 1}

      _ ->
        false
    end
  end

  defp redis_transport?(_), do: false

  @doc """
  Builds a Celery event dictionary matching `celery.events.Event`.

  Converts `"worker-online"` into routing key `"worker.online"`, attaches standard
  metadata (`hostname`, `timestamp`, `utcoffset`, `pid`, `clock`, `local_received`),
  and merges `fields`.
  """
  @spec build_event(String.t(), String.t(), map(), keyword()) :: {String.t(), map()}
  def build_event(type, hostname, fields \\ %{}, opts \\ []) when is_binary(type) do
    now = System.os_time(:millisecond) / 1000.0
    clock = Keyword.get(opts, :clock, 1)
    pid = Keyword.get_lazy(opts, :pid, &os_pid/0)

    base = %{
      "type" => type,
      "hostname" => hostname,
      "timestamp" => now,
      "local_received" => now,
      "utcoffset" => 0,
      "pid" => pid,
      "clock" => clock
    }

    routing_key = String.replace(type, "-", ".")
    {routing_key, Map.merge(fields, base)}
  end

  @doc """
  Publishes a Celery event to the `celeryev` exchange on `channel_or_conn`.
  """
  @spec publish(term(), String.t(), String.t(), map(), keyword()) :: :ok
  def publish(channel_or_conn, type, hostname, fields \\ %{}, opts \\ []) do
    {routing_key, event} = build_event(type, hostname, fields, opts)
    ex = exchange(channel_or_conn)
    serializer = Keyword.get(opts, :serializer, "json")

    try do
      producer =
        Producer.new(channel_or_conn, exchange: ex, serializer: serializer, auto_declare: true)

      Producer.publish(producer, event,
        exchange: ex,
        routing_key: routing_key,
        declare: [ex],
        serializer: serializer,
        headers: %{"hostname" => hostname}
      )
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end

  @doc """
  Default software identity metadata included in `worker-online`, `worker-heartbeat`,
  and `worker-offline` events.
  """
  @spec sw_info() :: map()
  def sw_info do
    %{
      "sw_ident" => "celery-elixir",
      "sw_ver" => "5.4.0",
      "sw_sys" => "Elixir/#{System.version()}"
    }
  end

  @doc """
  Formats an Elixir value as a Python literal string (`repr()`), suitable for
  Python's `ast.literal_eval/1` (e.g. `inspect().registered("metadata")` in OpenRelik
  and `args`/`kwargs`/`result` in Flower).
  """
  @spec to_python_repr(term()) :: String.t()
  def to_python_repr(nil), do: "None"
  def to_python_repr(true), do: "True"
  def to_python_repr(false), do: "False"
  def to_python_repr(n) when is_integer(n) or is_float(n), do: to_string(n)

  def to_python_repr(a) when is_atom(a) do
    a |> Atom.to_string() |> to_python_repr()
  end

  def to_python_repr(s) when is_binary(s) do
    escaped =
      s
      |> String.replace("\\", "\\\\")
      |> String.replace("'", "\\'")
      |> String.replace("\n", "\\n")
      |> String.replace("\r", "\\r")
      |> String.replace("\t", "\\t")

    "'#{escaped}'"
  end

  def to_python_repr(list) when is_list(list) do
    inner = list |> Enum.map(&to_python_repr/1) |> Enum.join(", ")
    "[#{inner}]"
  end

  def to_python_repr(tuple) when is_tuple(tuple) do
    elems = Tuple.to_list(tuple)

    case elems do
      [single] -> "(#{to_python_repr(single)},)"
      _ -> "(#{elems |> Enum.map(&to_python_repr/1) |> Enum.join(", ")})"
    end
  end

  def to_python_repr(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> to_python_repr()
  end

  def to_python_repr(map) when is_map(map) do
    inner =
      map
      |> Enum.sort_by(fn {k, _v} -> to_string(k) end)
      |> Enum.map(fn {k, v} -> "#{to_python_repr(to_string(k))}: #{to_python_repr(v)}" end)
      |> Enum.join(", ")

    "{#{inner}}"
  end

  def to_python_repr(other), do: to_python_repr(inspect(other))

  @doc """
  Returns the OS PID as an integer.
  """
  @spec os_pid() :: pos_integer()
  def os_pid do
    case Integer.parse(System.pid()) do
      {pid, _} -> pid
      :error -> 1
    end
  end
end
