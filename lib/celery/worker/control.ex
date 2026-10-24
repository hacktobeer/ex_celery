defmodule Celery.Worker.Control do
  @moduledoc """
  Remote control and inspection commands for Celery workers over `Kombu.Pidbox`
  (ported from `celery.worker.control` and `celery.app.control`).

  Supports standard Celery control/inspect methods:
  - `ping` -> `%{"ok" => "pong"}`
  - `registered` -> list of registered task names (with optional `[metadata=...]` when `taskinfoitems` is requested)
  - `active` -> list of currently executing task request info maps
  - `reserved` -> list of prefetched/reserved task request info maps
  - `scheduled` -> list of ETA-scheduled task maps
  - `revoked` -> list of revoked task IDs
  - `active_queues` -> list of queue dictionaries consumed by the worker
  - `conf` -> worker configuration dictionary
  - `stats` -> worker statistics dictionary
  - `report` -> worker software/environment report
  - `enable_events` / `disable_events` -> toggles event emission
  - `heartbeat` -> triggers an immediate worker heartbeat
  - `revoke` -> revokes one or more `task_id`s
  - `shutdown` -> stops the worker gracefully
  """

  alias Celery.AMQP
  alias Celery.Config
  alias Celery.Events
  alias Celery.Task.Definition

  @mailbox_name "celery"

  @doc """
  Creates a `%Kombu.Pidbox.Mailbox{}` (`type: "fanout"`) for the given `%Celery{}` or `%Celery.Config{}`.
  """
  @spec mailbox(Celery.t() | Config.t()) :: Kombu.Pidbox.Mailbox.t()
  def mailbox(app_or_config) do
    conn = AMQP.build_connection(app_or_config)
    Kombu.Pidbox.Mailbox.new(@mailbox_name, type: "fanout", connection: conn)
  end

  @doc """
  Broadcasts a control/inspect command to workers via `Kombu.Pidbox`.
  """
  @spec broadcast(Celery.t() | Config.t(), String.t(), map(), keyword()) ::
          :ok | list() | {:ok, list()} | {:error, term()}
  def broadcast(app_or_config, command, arguments \\ %{}, opts \\ []) do
    mb = mailbox(app_or_config)
    Kombu.Pidbox.Mailbox.cast(mb, Keyword.get(opts, :destination), command, arguments, opts)
  end

  @doc """
  Broadcasts an inspect command and collects replies from workers via `Kombu.Pidbox`.
  """
  @spec inspect_call(Celery.t() | Config.t(), String.t(), map(), keyword()) :: [map()]
  def inspect_call(app_or_config, command, arguments \\ %{}, opts \\ []) do
    mb = mailbox(app_or_config)
    raw_timeout = Keyword.get(opts, :timeout, 1.0)
    timeout = normalize_timeout_seconds(raw_timeout)
    destination = Keyword.get(opts, :destination)

    Kombu.Pidbox.Mailbox.call(
      mb,
      destination,
      command,
      arguments,
      Keyword.put(opts, :timeout, timeout)
    )
  end

  @doc """
  Pings active workers via `Kombu.Pidbox` and collects replies.
  """
  @spec ping(Celery.t() | Config.t(), keyword()) :: [map()]
  def ping(app_or_config, opts \\ []) do
    inspect_call(app_or_config, "ping", %{}, opts)
  end

  @doc """
  Broadcasts a `revoke` command for `task_id` (or a list of task IDs) to workers.
  """
  @spec revoke(Celery.t() | Config.t(), String.t() | [String.t()], keyword()) ::
          :ok | list() | {:ok, list()} | {:error, term()}
  def revoke(app_or_config, task_ids, opts \\ []) do
    args = %{
      "task_id" => task_ids,
      "terminate" => Keyword.get(opts, :terminate, false),
      "signal" => Keyword.get(opts, :signal, "SIGTERM")
    }

    broadcast(app_or_config, "revoke", args, opts)
  end

  @doc """
  Formats registered tasks for `inspect.registered`, supporting `taskinfoitems`
  (such as `["metadata"]` used by OpenRelik's `inspect().registered("metadata")`).
  """
  @spec format_registered_tasks(Celery.t(), map()) :: [String.t()]
  def format_registered_tasks(%Celery{} = app, args \\ %{}) do
    taskinfoitems =
      case Map.get(args, "taskinfoitems") || Map.get(args, :taskinfoitems) do
        items when is_list(items) -> Enum.map(items, &to_string/1)
        _ -> []
      end

    app
    |> Celery.Registry.keys()
    |> Enum.reject(&String.starts_with?(&1, "celery."))
    |> Enum.map(fn task_name ->
      if taskinfoitems == [] do
        task_name
      else
        case Celery.Registry.lookup(task_name, app) do
          {:ok, %Definition{} = defn} ->
            extras =
              taskinfoitems
              |> Enum.flat_map(fn field_str ->
                case lookup_definition_field(defn, field_str) do
                  {:ok, val} when not is_nil(val) ->
                    ["#{field_str}=#{Events.to_python_repr(val)}"]

                  _ ->
                    []
                end
              end)

            if extras == [] do
              task_name
            else
              "#{task_name} [#{Enum.join(extras, " ")}]"
            end

          _ ->
            task_name
        end
      end
    end)
  end

  defp lookup_definition_field(%Definition{} = defn, "metadata"), do: {:ok, defn.metadata}
  defp lookup_definition_field(%Definition{} = defn, "time_limit"), do: {:ok, defn.time_limit}

  defp lookup_definition_field(%Definition{} = defn, "soft_time_limit"),
    do: {:ok, defn.soft_time_limit}

  defp lookup_definition_field(%Definition{} = defn, "rate_limit"), do: {:ok, defn.rate_limit}
  defp lookup_definition_field(%Definition{} = defn, "max_retries"), do: {:ok, defn.max_retries}
  defp lookup_definition_field(%Definition{} = defn, "acks_late"), do: {:ok, defn.acks_late}

  defp lookup_definition_field(%Definition{} = defn, "ignore_result"),
    do: {:ok, defn.ignore_result}

  defp lookup_definition_field(_defn, _other), do: :error

  @doc """
  Formats a worker's `%Kombu.Queue{}` list into the dictionary list expected by
  Flower and `inspect().active_queues()`.
  """
  @spec format_active_queues([Kombu.Queue.t()]) :: [map()]
  def format_active_queues(queues) when is_list(queues) do
    Enum.map(queues, fn %Kombu.Queue{} = q ->
      ex = q.exchange || Kombu.Exchange.new(q.name, type: "direct", durable: true)
      ex_name = if ex.name == "", do: q.name, else: ex.name

      %{
        "name" => q.name,
        "exchange" => %{
          "name" => ex_name,
          "type" => ex.type || "direct",
          "arguments" => ex.arguments,
          "durable" => ex.durable,
          "passive" => ex.passive,
          "auto_delete" => ex.auto_delete,
          "delivery_mode" => ex.delivery_mode || 2,
          "no_declare" => ex.no_declare
        },
        "routing_key" => if(q.routing_key == "", do: q.name, else: q.routing_key),
        "queue_arguments" => q.queue_arguments,
        "binding_arguments" => q.binding_arguments,
        "consumer_arguments" => q.consumer_arguments,
        "durable" => q.durable,
        "exclusive" => q.exclusive,
        "auto_delete" => q.auto_delete,
        "no_ack" => q.no_ack,
        "alias" => q.alias,
        "bindings" => [],
        "no_declare" => q.no_declare,
        "expires" => q.expires,
        "message_ttl" => q.message_ttl,
        "max_length" => q.max_length,
        "max_length_bytes" => q.max_length_bytes,
        "max_priority" => q.max_priority
      }
    end)
  end

  @doc """
  Builds the handler map used by a `Celery.Worker`'s `Kombu.Pidbox.Node` to respond to
  remote control and inspect requests from Python or Elixir Celery clients.
  """
  @spec build_handlers(pid(), Celery.t()) :: %{String.t() => (term(), map() -> term())}
  def build_handlers(worker_pid, %Celery{} = app) do
    %{
      "ping" => fn _state, _args ->
        %{"ok" => "pong"}
      end,
      "registered" => fn _state, args ->
        format_registered_tasks(app, args)
      end,
      "active" => fn _state, _args ->
        GenServer.call(worker_pid, :inspect_active)
      end,
      "reserved" => fn _state, _args ->
        GenServer.call(worker_pid, :inspect_reserved)
      end,
      "scheduled" => fn _state, _args ->
        GenServer.call(worker_pid, :inspect_scheduled)
      end,
      "revoked" => fn _state, _args ->
        GenServer.call(worker_pid, :inspect_revoked)
      end,
      "active_queues" => fn _state, _args ->
        GenServer.call(worker_pid, :inspect_active_queues)
      end,
      "conf" => fn _state, _args ->
        Config.to_json_map(app.config)
      end,
      "stats" => fn _state, _args ->
        GenServer.call(worker_pid, :inspect_stats)
      end,
      "report" => fn _state, _args ->
        %{
          "ok" =>
            "software -> celery-elixir:0.1.1 (kombu:0.1.1) Elixir/#{System.version()} OTP/#{System.otp_release()}"
        }
      end,
      "enable_events" => fn _state, _args ->
        send(worker_pid, {:set_send_events, true})
        %{"ok" => "events enabled"}
      end,
      "disable_events" => fn _state, _args ->
        send(worker_pid, {:set_send_events, false})
        %{"ok" => "events disabled"}
      end,
      "heartbeat" => fn _state, _args ->
        send(worker_pid, :heartbeat)
        %{"ok" => "heartbeat sent"}
      end,
      "revoke" => fn _state, args ->
        ids =
          case Map.get(args, "task_id") || Map.get(args, :task_id) do
            list when is_list(list) -> list
            single when is_binary(single) -> [single]
            _ -> []
          end

        terminate? = Map.get(args, "terminate", false) == true
        GenServer.call(worker_pid, {:revoke, ids, terminate?})
        %{"ok" => "tasks #{inspect(ids)} flagged as revoked"}
      end,
      "shutdown" => fn _state, _args ->
        send(worker_pid, :remote_shutdown)
        %{"ok" => "shutting down"}
      end
    }
  end

  defp normalize_timeout_seconds(t) when is_integer(t) and t > 60, do: t / 1000.0
  defp normalize_timeout_seconds(t) when is_number(t) and t > 0, do: t * 1.0
  defp normalize_timeout_seconds(_), do: 1.0
end
