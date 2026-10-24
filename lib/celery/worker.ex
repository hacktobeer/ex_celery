defmodule Celery.Worker do
  @moduledoc """
  Celery Worker / Consumer OTP `GenServer` (ported from `celery.worker`).

  Consumes Celery task messages from Redis (or `memory://` in tests) via `Kombu`,
  decodes Protocol v1 and v2 task messages, schedules ETA/countdown tasks, enforces
  concurrency limits, executes tasks through `Celery.Tracer`, stores results in
  `Celery.Backend.Redis`, emits `celeryev` worker/task events (`Celery.Events`),
  and listens for `Kombu.Pidbox` remote control and inspect commands.

  ## Starting a Worker

  ```elixir
  celery = Celery.new(
    broker: "redis://localhost:6379/0",
    backend: "redis://localhost:6379/0",
    include: ["src.tasks"]
  )

  {:ok, worker} = Celery.Worker.start_link(app: celery, concurrency: 4)
  ```
  """

  use GenServer, shutdown: 30_000
  require Logger

  alias Celery.AMQP
  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Config
  alias Celery.Events
  alias Celery.Protocol
  alias Celery.Task.Definition
  alias Celery.Tracer
  alias Celery.Worker.Control
  alias Celery.Worker.Request

  @type t :: pid()

  defstruct [
    :app,
    :hostname,
    :connection,
    :channel,
    :consumer,
    :queues,
    :concurrency,
    :poll_interval_ms,
    :poll_timer,
    :heartbeat_interval_ms,
    :heartbeat_timer,
    :pidbox_mailbox,
    :pidbox_node,
    :pidbox_consumer,
    send_events: true,
    clock: 0,
    active: %{},
    reserved: :queue.new(),
    scheduled: %{},
    revoked: MapSet.new(),
    processed_count: 0,
    total_by_task: %{},
    started_at: nil,
    notify_pid: nil
  ]

  @doc """
  Starts a `Celery.Worker` linked to the calling process.

  ## Options
  - `:app` - `%Celery{}` instance (required, or built from remaining options)
  - `:queues` - list of queue names or `%Kombu.Queue{}` structs to consume from
  - `:hostname` - worker node hostname (default `"celery@<host>"`)
  - `:concurrency` - maximum number of concurrent task processes (default `app.config.worker_concurrency`)
  - `:poll_interval` - poll interval in milliseconds (default `25`)
  - `:enable_remote_control` - whether to start a `Kombu.Pidbox.Node` listener (default `app.config.worker_enable_remote_control`, which is `true`)
  - `:send_events` - whether to emit `celeryev` worker/task events (default `app.config.worker_send_task_events`, which is `true`)
  - `:heartbeat_interval` - worker heartbeat interval in milliseconds (default `app.config.worker_heartbeat_interval`, `5_000`)
  - `:notify` - optional PID to receive `{:celery_task_done, task_id, outcome}` events
  - `:name` - optional GenServer registration name
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name_opt, init_opts} = Keyword.pop(opts, :name)

    if name_opt do
      GenServer.start_link(__MODULE__, init_opts, name: name_opt)
    else
      GenServer.start_link(__MODULE__, init_opts)
    end
  end

  @doc "Stops a running `Celery.Worker` gracefully."
  @spec stop(GenServer.server(), timeout()) :: :ok
  def stop(worker, timeout \\ 5_000) do
    try do
      GenServer.stop(worker, :normal, timeout)
    catch
      :exit, _ -> :ok
    end
  end

  @doc "Returns the list of currently executing task requests (`inspect.active`)."
  @spec active(GenServer.server()) :: [map()]
  def active(worker), do: GenServer.call(worker, :inspect_active)

  @doc "Returns the list of prefetched/reserved task requests (`inspect.reserved`)."
  @spec reserved(GenServer.server()) :: [map()]
  def reserved(worker), do: GenServer.call(worker, :inspect_reserved)

  @doc "Returns the list of ETA-scheduled task requests (`inspect.scheduled`)."
  @spec scheduled(GenServer.server()) :: [map()]
  def scheduled(worker), do: GenServer.call(worker, :inspect_scheduled)

  @doc "Returns the list of revoked task IDs on this worker (`inspect.revoked`)."
  @spec revoked(GenServer.server()) :: [String.t()]
  def revoked(worker), do: GenServer.call(worker, :inspect_revoked)

  @doc "Returns the list of active queue dictionaries on this worker (`inspect.active_queues`)."
  @spec active_queues(GenServer.server()) :: [map()]
  def active_queues(worker), do: GenServer.call(worker, :inspect_active_queues)

  @doc "Returns worker statistics (`inspect.stats`)."
  @spec stats(GenServer.server()) :: map()
  def stats(worker), do: GenServer.call(worker, :inspect_stats)

  @doc "Revokes one or more task IDs on this worker."
  @spec revoke(GenServer.server(), String.t() | [String.t()], keyword()) :: :ok
  def revoke(worker, task_ids, opts \\ []) do
    ids = List.wrap(task_ids)
    terminate? = Keyword.get(opts, :terminate, false)
    GenServer.call(worker, {:revoke, ids, terminate?})
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    app =
      case Keyword.get(opts, :app) do
        %Celery{} = a -> a
        nil -> Celery.new(opts)
      end

    Enum.each(app.config.include, &Celery.Registry.load_include/1)

    hostname = Keyword.get_lazy(opts, :hostname, &default_hostname/0)
    concurrency = Keyword.get(opts, :concurrency, app.config.worker_concurrency)
    poll_interval_ms = Keyword.get(opts, :poll_interval, 25)
    notify_pid = Keyword.get(opts, :notify)
    queues = AMQP.queues_for_worker(app, Keyword.get(opts, :queues))

    conn = AMQP.build_connection(app)
    channel = Kombu.Connection.default_channel(conn)

    worker_pid = self()

    consumer =
      Kombu.Consumer.new(channel, queues,
        accept: app.config.accept_content,
        callbacks: [
          fn _body, message ->
            send(worker_pid, {:incoming_message, message})
          end
        ]
      )
      |> Kombu.Consumer.consume()

    enable_rc? =
      Keyword.get(opts, :enable_remote_control, app.config.worker_enable_remote_control)

    {pidbox_mailbox, pidbox_node, pidbox_consumer} =
      if enable_rc? do
        mb = Kombu.Pidbox.Mailbox.new("celery", type: "fanout", connection: conn)
        node = Kombu.Pidbox.Mailbox.node(mb, hostname, channel: channel)

        pb_consumer =
          Kombu.Pidbox.Node.listen(node,
            channel: channel,
            callbacks: [
              fn body, message ->
                send(worker_pid, {:pidbox_message, body, message})
              end
            ]
          )

        {mb, node, pb_consumer}
      else
        {nil, nil, nil}
      end

    send_events? = Keyword.get(opts, :send_events, app.config.worker_send_task_events)

    heartbeat_interval_ms =
      Keyword.get(opts, :heartbeat_interval, app.config.worker_heartbeat_interval)

    state = %__MODULE__{
      app: app,
      hostname: hostname,
      connection: conn,
      channel: channel,
      consumer: consumer,
      queues: queues,
      concurrency: concurrency,
      poll_interval_ms: poll_interval_ms,
      heartbeat_interval_ms: heartbeat_interval_ms,
      pidbox_mailbox: pidbox_mailbox,
      pidbox_node: pidbox_node,
      pidbox_consumer: pidbox_consumer,
      send_events: send_events?,
      clock: 0,
      started_at: System.monotonic_time(:millisecond),
      notify_pid: notify_pid
    }

    state =
      state
      |> maybe_emit_worker_event("worker-online")
      |> maybe_emit_worker_event("worker-heartbeat")
      |> maybe_schedule_heartbeat()
      |> schedule_poll(0)

    {:ok, state}
  end

  @impl true
  def handle_call(:inspect_active, _from, state) do
    {:reply, build_active_info(state), state}
  end

  def handle_call(:inspect_reserved, _from, state) do
    {:reply, build_reserved_info(state), state}
  end

  def handle_call(:inspect_scheduled, _from, state) do
    {:reply, build_scheduled_info(state), state}
  end

  def handle_call(:inspect_revoked, _from, state) do
    {:reply, MapSet.to_list(state.revoked), state}
  end

  def handle_call(:inspect_active_queues, _from, state) do
    {:reply, Control.format_active_queues(state.queues), state}
  end

  def handle_call(:inspect_stats, _from, state) do
    {:reply, build_stats_info(state), state}
  end

  def handle_call({:revoke, ids, terminate?}, _from, state) do
    state = do_revoke(state, ids, terminate?)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    try do
      Kombu.Connection.drain_events(state.connection, timeout: 0)
    rescue
      Kombu.TimeoutError -> :ok
      _ -> :ok
    end

    state =
      state
      |> dispatch_ready_tasks()
      |> schedule_poll(state.poll_interval_ms)

    {:noreply, state}
  end

  def handle_info(:heartbeat, state) do
    state =
      state
      |> maybe_emit_worker_event("worker-heartbeat")
      |> maybe_schedule_heartbeat()

    {:noreply, state}
  end

  def handle_info({:set_send_events, enabled?}, state) do
    state = %{state | send_events: enabled?}

    state =
      if enabled? do
        maybe_emit_worker_event(state, "worker-heartbeat")
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({:pidbox_message, body, message}, state) when is_map(body) do
    state = handle_pidbox_message(state, body, message)
    {:noreply, state}
  end

  def handle_info({:incoming_message, %Kombu.Message{} = message}, state) do
    state =
      case Request.from_message(message, state.hostname) do
        {:ok, %Request{} = req} ->
          handle_request_arrival(state, req)

        {:error, reason} ->
          Logger.warning("Failed to decode Celery task message: #{inspect(reason)}")
          Kombu.Message.reject(message, requeue: false)
          state
      end

    {:noreply, dispatch_ready_tasks(state)}
  end

  def handle_info({:eta_ready, task_id}, state) do
    case Map.pop(state.scheduled, task_id) do
      {nil, _} ->
        {:noreply, state}

      {%{request: req}, new_scheduled} ->
        new_reserved = :queue.in(req, state.reserved)
        state = %{state | scheduled: new_scheduled, reserved: new_reserved}
        {:noreply, dispatch_ready_tasks(state)}
    end
  end

  def handle_info({:task_finished, task_pid, outcome}, state) do
    case Map.pop(state.active, task_pid) do
      {nil, _} ->
        {:noreply, state}

      {%{ref: ref, request: req}, new_active} ->
        Process.demonitor(ref, [:flush])
        state = finalize_request(%{state | active: new_active}, req, outcome)
        {:noreply, dispatch_ready_tasks(state)}
    end
  end

  def handle_info({:DOWN, _ref, :process, task_pid, reason}, state) do
    case Map.pop(state.active, task_pid) do
      {nil, _} ->
        {:noreply, state}

      {%{request: req}, new_active} ->
        exc = Celery.Error.exception(message: "Worker task crashed: #{inspect(reason)}")
        RedisBackend.mark_as_failure(state.app, req.id, exc, request: req.context)

        if state.app.config.task_reject_on_worker_lost do
          Request.reject(req, requeue: true)
        else
          Request.acknowledge(req)
        end

        notify_done(state, req.id, {:error, exc, ""})

        state =
          %{state | active: new_active}
          |> increment_processed(req.context.task)
          |> maybe_emit_task_event("task-failed", req, %{
            "exception" => inspect(exc),
            "traceback" => ""
          })

        {:noreply, dispatch_ready_tasks(state)}
    end
  end

  def handle_info(:remote_shutdown, state) do
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.poll_timer, do: Process.cancel_timer(state.poll_timer)
    if state.heartbeat_timer, do: Process.cancel_timer(state.heartbeat_timer)

    if broker_alive?(state) do
      _ = maybe_emit_worker_event(state, "worker-offline")

      if state.pidbox_consumer do
        try do
          Kombu.Consumer.cancel(state.pidbox_consumer)
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
      end

      if state.consumer do
        try do
          Kombu.Consumer.cancel(state.consumer)
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
      end

      if state.connection do
        try do
          Kombu.Connection.close(state.connection)
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
      end
    end

    :ok
  end

  defp broker_alive?(%__MODULE__{channel: channel, app: app}) do
    channel_ok? = is_pid(channel) and Process.alive?(channel)

    redis_ok? =
      case app.config.redis_conn do
        pid when is_pid(pid) -> Process.alive?(pid)
        {_mod, pid} when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    channel_ok? and redis_ok?
  end

  # --- Pidbox Control Dispatcher ---

  defp handle_pidbox_message(%__MODULE__{pidbox_node: nil} = state, _body, _message), do: state

  defp handle_pidbox_message(%__MODULE__{pidbox_node: node} = state, body, message) do
    destination = Map.get(body, "destination")
    pattern = Map.get(body, "pattern")
    matcher = Map.get(body, "matcher")

    if message && message.headers && message.headers["clock"] do
      Kombu.Clocks.LamportClock.adjust(node.mailbox.clock, message.headers["clock"])
    end

    run_dispatch? =
      cond do
        destination != nil and is_list(destination) and destination != [] ->
          state.hostname in destination

        pattern != nil and matcher != nil ->
          Kombu.Matcher.match(state.hostname, pattern, matcher)

        true ->
          true
      end

    if run_dispatch? do
      method = to_string(Map.get(body, "method"))
      args = Map.get(body, "arguments") || %{}
      args = if is_map(args), do: args, else: %{}
      reply_to = Map.get(body, "reply_to")
      ticket = Map.get(body, "ticket")

      {result, state} = execute_control_method(state, method, args)

      if is_map(reply_to) and not is_nil(ticket) do
        Kombu.Pidbox.Node.reply(
          node,
          %{state.hostname => result},
          reply_to["exchange"],
          reply_to["routing_key"],
          ticket,
          channel: state.channel
        )
      end

      state
    else
      state
    end
  end

  defp execute_control_method(state, "ping", _args) do
    {%{"ok" => "pong"}, state}
  end

  defp execute_control_method(state, "registered", args) do
    {Control.format_registered_tasks(state.app, args), state}
  end

  defp execute_control_method(state, "active", _args) do
    {build_active_info(state), state}
  end

  defp execute_control_method(state, "reserved", _args) do
    {build_reserved_info(state), state}
  end

  defp execute_control_method(state, "scheduled", _args) do
    {build_scheduled_info(state), state}
  end

  defp execute_control_method(state, "revoked", _args) do
    {MapSet.to_list(state.revoked), state}
  end

  defp execute_control_method(state, "active_queues", _args) do
    {Control.format_active_queues(state.queues), state}
  end

  defp execute_control_method(state, "conf", _args) do
    {Config.to_json_map(state.app.config), state}
  end

  defp execute_control_method(state, "stats", _args) do
    {build_stats_info(state), state}
  end

  defp execute_control_method(state, "report", _args) do
    {%{
       "ok" =>
         "software -> celery-elixir:0.1.1 (kombu:0.1.1) Elixir/#{System.version()} OTP/#{System.otp_release()}"
     }, state}
  end

  defp execute_control_method(state, "enable_events", _args) do
    state =
      %{state | send_events: true}
      |> maybe_emit_worker_event("worker-online")
      |> maybe_emit_worker_event("worker-heartbeat")

    {%{"ok" => "events enabled"}, state}
  end

  defp execute_control_method(state, "disable_events", _args) do
    {%{"ok" => "events disabled"}, %{state | send_events: false}}
  end

  defp execute_control_method(state, "heartbeat", _args) do
    state = maybe_emit_worker_event(state, "worker-heartbeat")
    {%{"ok" => "heartbeat sent"}, state}
  end

  defp execute_control_method(state, "revoke", args) do
    ids =
      case Map.get(args, "task_id") || Map.get(args, :task_id) do
        list when is_list(list) -> list
        single when is_binary(single) -> [single]
        _ -> []
      end

    terminate? = Map.get(args, "terminate", false) == true
    state = do_revoke(state, ids, terminate?)
    {%{"ok" => "tasks #{inspect(ids)} flagged as revoked"}, state}
  end

  defp execute_control_method(state, "shutdown", _args) do
    send(self(), :remote_shutdown)
    {%{"ok" => "shutting down"}, state}
  end

  defp execute_control_method(state, other, _args) do
    {%{"error" => "Unknown method #{other}"}, state}
  end

  # --- Inspect Info Builders ---

  defp build_active_info(state) do
    state.active
    |> Map.values()
    |> Enum.map(fn %{request: req} -> Request.info(req) end)
  end

  defp build_reserved_info(state) do
    state.reserved
    |> :queue.to_list()
    |> Enum.map(&Request.info/1)
  end

  defp build_scheduled_info(state) do
    state.scheduled
    |> Map.values()
    |> Enum.map(fn %{request: req, eta: eta} ->
      %{
        "eta" => eta,
        "priority" => 0,
        "request" => Request.info(req)
      }
    end)
  end

  defp build_stats_info(state) do
    uri = URI.parse(state.app.config.broker_url)
    uptime_s = max(div(System.monotonic_time(:millisecond) - state.started_at, 1000), 0)
    os_pid = Events.os_pid()

    %{
      "hostname" => state.hostname,
      "pid" => os_pid,
      "clock" => to_string(state.clock),
      "uptime" => uptime_s,
      "total" => state.total_by_task,
      "pool" => %{
        "max-concurrency" => state.concurrency,
        "processes" => [os_pid],
        "max-tasks-per-child" => "N/A",
        "put-guarded-by-semaphore" => false,
        "timeouts" => [
          state.app.config.task_soft_time_limit || 0,
          state.app.config.task_time_limit || 0
        ]
      },
      "broker" => %{
        "hostname" => uri.host || "localhost",
        "port" => uri.port || 6379,
        "virtual_host" => String.trim_leading(uri.path || "/0", "/"),
        "transport" => uri.scheme || "redis",
        "insist" => false,
        "ssl" => uri.scheme == "rediss",
        "connect_timeout" => 4,
        "connection_timeout" => 4,
        "transport_options" => %{},
        "login_method" => nil,
        "uri_prefix" => nil,
        "heartbeat" => 120.0,
        "failover_strategy" => "round-robin",
        "alternates" => []
      },
      "prefetch_count" => state.concurrency * state.app.config.worker_prefetch_multiplier,
      "rusage" => %{
        "utime" => 0.0,
        "stime" => 0.0,
        "maxrss" => div(:erlang.memory(:total), 1024),
        "idrss" => 0,
        "isrss" => 0,
        "minflt" => 0,
        "majflt" => 0,
        "nswap" => 0,
        "inblock" => 0,
        "oublock" => 0,
        "msgsnd" => 0,
        "msgrcv" => 0,
        "nsignals" => 0,
        "nvcsw" => 0,
        "nivcsw" => 0
      }
    }
  end

  defp do_revoke(state, ids, terminate?) do
    new_revoked = Enum.reduce(ids, state.revoked, &MapSet.put(&2, &1))

    state =
      if terminate? do
        Enum.reduce(state.active, state, fn {task_pid, %{ref: ref, request: req}}, acc ->
          if req.id in ids do
            Process.demonitor(ref, [:flush])
            Process.exit(task_pid, :kill)
            RedisBackend.mark_as_revoked(acc.app, req.id, "terminated", request: req.context)
            {_acked_req, _} = Request.acknowledge(req)
            %{acc | active: Map.delete(acc.active, task_pid)}
          else
            acc
          end
        end)
      else
        state
      end

    %{state | revoked: new_revoked}
  end

  # --- Internal Worker Pipeline ---

  defp handle_request_arrival(state, %Request{context: ctx} = req) do
    state =
      maybe_emit_task_event(state, "task-received", req, %{
        "name" => ctx.task,
        "args" => Events.to_python_repr(Tuple.to_list(List.to_tuple(ctx.args || []))),
        "kwargs" => Events.to_python_repr(ctx.kwargs || %{}),
        "retries" => ctx.retries || 0,
        "eta" => ctx.eta,
        "expires" => ctx.expires
      })

    cond do
      MapSet.member?(state.revoked, req.id) ->
        RedisBackend.mark_as_revoked(state.app, req.id, "revoked", request: ctx)
        {acked_req, _} = Request.acknowledge(req)
        notify_done(state, acked_req.id, {:revoked, "revoked"})

        state
        |> increment_processed(ctx.task)
        |> maybe_emit_task_event("task-revoked", req, %{
          "terminated" => false,
          "signum" => nil,
          "expired" => false
        })

      Protocol.expired?(ctx.expires) ->
        RedisBackend.mark_as_revoked(state.app, req.id, "expired", request: ctx)
        {acked_req, _} = Request.acknowledge(req)
        notify_done(state, acked_req.id, {:revoked, "expired"})

        state
        |> increment_processed(ctx.task)
        |> maybe_emit_task_event("task-revoked", req, %{
          "terminated" => false,
          "signum" => nil,
          "expired" => true
        })

      true ->
        delay_ms = Protocol.eta_delay_ms(ctx.eta)

        if delay_ms > 0 do
          timer_ref = Process.send_after(self(), {:eta_ready, req.id}, delay_ms)

          scheduled_entry = %{
            request: req,
            eta: ctx.eta,
            timer_ref: timer_ref
          }

          %{state | scheduled: Map.put(state.scheduled, req.id, scheduled_entry)}
        else
          %{state | reserved: :queue.in(req, state.reserved)}
        end
    end
  end

  defp dispatch_ready_tasks(state) do
    if map_size(state.active) >= state.concurrency do
      state
    else
      case :queue.out(state.reserved) do
        {:empty, _} ->
          state

        {{:value, %Request{} = req}, new_reserved} ->
          state = %{state | reserved: new_reserved}
          state = start_task_execution(state, req)
          dispatch_ready_tasks(state)
      end
    end
  end

  defp start_task_execution(state, %Request{context: ctx} = req) do
    acks_late? = resolve_acks_late(state.app, ctx.task)

    req =
      if acks_late? do
        req
      else
        {acked_req, _} = Request.acknowledge(req)
        acked_req
      end

    worker_self = self()
    app = state.app
    revoked_set = state.revoked

    {task_pid, mon_ref} =
      spawn_monitor(fn ->
        outcome = Tracer.trace_task(app, ctx, revoked_set)
        send(worker_self, {:task_finished, self(), outcome})
      end)

    req = %{
      req
      | worker_pid: task_pid,
        time_start: System.system_time(:millisecond) / 1000.0
    }

    state
    |> Map.put(:active, Map.put(state.active, task_pid, %{ref: mon_ref, request: req}))
    |> maybe_emit_task_event("task-started", req, %{})
  end

  defp finalize_request(state, %Request{context: ctx} = req, outcome) do
    runtime =
      if is_number(req.time_start) do
        max(System.system_time(:millisecond) / 1000.0 - req.time_start, 0.0)
      else
        0.0
      end

    state =
      case outcome do
        {:ok, retval} ->
          Request.acknowledge(req)

          maybe_emit_task_event(state, "task-succeeded", req, %{
            "result" => Events.to_python_repr(retval),
            "runtime" => runtime
          })

        {:retry, %Celery.Retry{} = retry_exc} ->
          Request.acknowledge(req)

          maybe_emit_task_event(state, "task-retried", req, %{
            "exception" => inspect(retry_exc.exc || retry_exc),
            "traceback" => ""
          })

        {:reject, %Celery.Reject{requeue: requeue}} ->
          Request.reject(req, requeue: requeue)
          state

        {:error, exc, tb} ->
          ack_on_failure? = resolve_ack_on_failure(state.app, ctx.task)

          if ack_on_failure? do
            Request.acknowledge(req)
          else
            Request.reject(req, requeue: false)
          end

          maybe_emit_task_event(state, "task-failed", req, %{
            "exception" => Exception.message(exc),
            "traceback" => tb || ""
          })

        _ ->
          Request.acknowledge(req)
          state
      end

    notify_done(state, req.id, outcome)
    increment_processed(state, ctx.task)
  end

  defp increment_processed(state, task_name) do
    total_by_task =
      if is_binary(task_name) do
        Map.update(state.total_by_task, task_name, 1, &(&1 + 1))
      else
        state.total_by_task
      end

    %{state | processed_count: state.processed_count + 1, total_by_task: total_by_task}
  end

  defp maybe_emit_worker_event(%__MODULE__{send_events: false} = state, _type), do: state

  defp maybe_emit_worker_event(%__MODULE__{} = state, type) do
    clock = state.clock + 1

    fields =
      Events.sw_info()
      |> Map.merge(%{
        "freq" =>
          if(is_integer(state.heartbeat_interval_ms) and state.heartbeat_interval_ms > 0,
            do: state.heartbeat_interval_ms / 1000.0,
            else: 5.0
          ),
        "active" => map_size(state.active),
        "processed" => state.processed_count,
        "loadavg" => [0.0, 0.0, 0.0]
      })

    Events.publish(state.channel, type, state.hostname, fields, clock: clock)
    %{state | clock: clock}
  end

  defp maybe_emit_task_event(%__MODULE__{send_events: false} = state, _type, _req, _extra),
    do: state

  defp maybe_emit_task_event(%__MODULE__{} = state, type, %Request{} = req, extra) do
    clock = state.clock + 1
    fields = Map.put(extra, "uuid", req.id)
    Events.publish(state.channel, type, state.hostname, fields, clock: clock)
    %{state | clock: clock}
  end

  defp maybe_schedule_heartbeat(%__MODULE__{heartbeat_interval_ms: ms} = state)
       when is_integer(ms) and ms > 0 do
    if state.heartbeat_timer, do: Process.cancel_timer(state.heartbeat_timer)
    timer = Process.send_after(self(), :heartbeat, ms)
    %{state | heartbeat_timer: timer}
  end

  defp maybe_schedule_heartbeat(state), do: state

  defp resolve_acks_late(%Celery{} = app, task_name) do
    case Celery.Registry.lookup(task_name, app) do
      {:ok, %Definition{acks_late: val}} when is_boolean(val) -> val
      _ -> app.config.task_acks_late
    end
  end

  defp resolve_ack_on_failure(%Celery{} = app, task_name) do
    case Celery.Registry.lookup(task_name, app) do
      {:ok, %Definition{ack_on_failure_or_timeout: val}} when is_boolean(val) -> val
      _ -> app.config.task_ack_on_failure_or_timeout
    end
  end

  defp notify_done(%{notify_pid: pid}, task_id, outcome) when is_pid(pid) do
    send(pid, {:celery_task_done, task_id, outcome})
  end

  defp notify_done(_state, _task_id, _outcome), do: :ok

  defp schedule_poll(state, ms) do
    if state.poll_timer, do: Process.cancel_timer(state.poll_timer)
    timer = Process.send_after(self(), :poll, ms)
    %{state | poll_timer: timer}
  end

  defp default_hostname do
    {:ok, host} = :inet.gethostname()
    "celery@#{List.to_string(host)}"
  end
end
