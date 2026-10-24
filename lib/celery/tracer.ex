defmodule Celery.Tracer do
  @moduledoc """
  Task execution tracer (ported from `celery.app.trace.build_tracer`).

  Handles the complete lifecycle of executing a single `%Celery.Task.Context{}`:
  - Revocation and expiration checks
  - `STARTED` state tracking (`task_track_started`)
  - Task invocation with optional hard time limit (`time_limit`)
  - `SUCCESS` result persistence (`Celery.Backend.Redis.mark_as_done/4`)
  - `callbacks` and `chain` continuation dispatch
  - `RETRY` handling (both explicit `Celery.Task.retry/2` and automatic `autoretry_for`)
  - `Ignore` and `Reject` handling
  - `FAILURE` persistence and `errbacks` dispatch
  """

  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Canvas.Signature
  alias Celery.Task
  alias Celery.Task.Context
  alias Celery.Task.Definition

  @type trace_outcome ::
          {:ok, term()}
          | {:retry, Celery.Retry.t()}
          | {:ignore, Celery.Ignore.t()}
          | {:reject, Celery.Reject.t()}
          | {:revoked, String.t()}
          | {:error, Exception.t(), String.t()}

  @doc """
  Executes a task described by `context` using `app` and optional `revoked_set`.
  """
  @spec trace_task(Celery.t(), Context.t(), MapSet.t()) :: trace_outcome()
  def trace_task(%Celery{} = app, %Context{} = ctx, revoked_set \\ MapSet.new()) do
    cond do
      MapSet.member?(revoked_set, ctx.id) ->
        RedisBackend.mark_as_revoked(app, ctx.id, "revoked", request: ctx)
        {:revoked, "revoked"}

      Celery.Protocol.expired?(ctx.expires) ->
        RedisBackend.mark_as_revoked(app, ctx.id, "expired", request: ctx)
        {:revoked, "expired"}

      true ->
        case Celery.Registry.lookup(ctx.task, app) do
          {:ok, %Definition{} = defn} ->
            execute_registered_task(app, defn, ctx)

          {:error, not_reg_exc} ->
            tb = Exception.format(:error, not_reg_exc, [])
            RedisBackend.mark_as_failure(app, ctx.id, not_reg_exc, traceback: tb, request: ctx)
            {:error, not_reg_exc, tb}
        end
    end
  end

  defp execute_registered_task(%Celery{} = app, %Definition{} = defn, %Context{} = ctx) do
    config = app.config

    track_started? =
      if is_nil(defn.track_started), do: config.task_track_started, else: defn.track_started

    ignore_result? =
      ctx.ignore_result ||
        if(is_nil(defn.ignore_result), do: config.task_ignore_result, else: defn.ignore_result)

    store_errors? =
      not ignore_result? ||
        if(is_nil(defn.store_errors_even_if_ignored),
          do: config.task_store_errors_even_if_ignored,
          else: defn.store_errors_even_if_ignored
        )

    if track_started? and not ignore_result? do
      RedisBackend.mark_as_started(
        app,
        ctx.id,
        %{"pid" => inspect(self()), "hostname" => ctx.hostname},
        request: ctx
      )
    end

    time_limit_ms = resolve_time_limit_ms(ctx, defn, config)

    try do
      retval = run_with_time_limit(defn, ctx, app, time_limit_ms)

      RedisBackend.mark_as_done(app, ctx.id, retval,
        request: ctx,
        store_result: not ignore_result?
      )

      dispatch_callbacks(app, ctx, retval)
      dispatch_chain(app, ctx, retval)

      {:ok, retval}
    rescue
      retry_exc in [Celery.Retry] ->
        handle_retry(app, defn, ctx, retry_exc, store_errors?)

      ignore_exc in [Celery.Ignore] ->
        {:ignore, ignore_exc}

      reject_exc in [Celery.Reject] ->
        {:reject, reject_exc}

      exc ->
        stack = __STACKTRACE__

        if should_autoretry?(defn, ctx, exc) do
          retry_exc = build_autoretry(defn, ctx, exc)
          handle_retry(app, defn, ctx, retry_exc, store_errors?)
        else
          tb = Exception.format(:error, exc, stack)

          RedisBackend.mark_as_failure(app, ctx.id, exc,
            traceback: tb,
            request: ctx,
            store_result: store_errors?
          )

          dispatch_errbacks(app, ctx, exc, tb)
          {:error, exc, tb}
        end
    catch
      kind, reason ->
        tb = Exception.format(kind, reason, __STACKTRACE__)
        exc = Celery.Error.exception(message: "#{kind}: #{inspect(reason)}", reason: reason)

        RedisBackend.mark_as_failure(app, ctx.id, exc,
          traceback: tb,
          request: ctx,
          store_result: store_errors?
        )

        dispatch_errbacks(app, ctx, exc, tb)
        {:error, exc, tb}
    end
  end

  defp run_with_time_limit(defn, ctx, app, nil) do
    Task.invoke(defn, ctx.args, ctx.kwargs, request: ctx, app: app)
  end

  defp run_with_time_limit(defn, ctx, app, timeout_ms)
       when is_integer(timeout_ms) and timeout_ms > 0 do
    caller = self()
    ref = make_ref()

    {pid, mon_ref} =
      spawn_monitor(fn ->
        res =
          try do
            {:ok, Task.invoke(defn, ctx.args, ctx.kwargs, request: ctx, app: app)}
          rescue
            e -> {:rescue, e, __STACKTRACE__}
          catch
            k, r -> {:catch, k, r, __STACKTRACE__}
          end

        send(caller, {ref, res})
      end)

    receive do
      {^ref, {:ok, val}} ->
        Process.demonitor(mon_ref, [:flush])
        val

      {^ref, {:rescue, e, stack}} ->
        Process.demonitor(mon_ref, [:flush])
        reraise e, stack

      {^ref, {:catch, k, r, _stack}} ->
        Process.demonitor(mon_ref, [:flush])
        :erlang.raise(k, r, [])

      {:DOWN, ^mon_ref, :process, ^pid, reason} ->
        raise Celery.Error, message: "Task process exited: #{inspect(reason)}"
    after
      timeout_ms ->
        Process.demonitor(mon_ref, [:flush])
        Process.exit(pid, :kill)

        raise Celery.TimeoutError,
          message: "TimeLimitExceeded(#{Float.round(timeout_ms / 1000, 2)}s)"
    end
  end

  defp handle_retry(app, _defn, %Context{} = ctx, %Celery.Retry{} = retry_exc, store_errors?) do
    tb = Exception.format(:error, retry_exc.exc || retry_exc, [])

    if store_errors? do
      RedisBackend.mark_as_retry(app, ctx.id, retry_exc.exc || retry_exc,
        traceback: tb,
        request: ctx
      )
    end

    retry_opts =
      retry_exc.options
      |> Keyword.put(:args, retry_exc.args || ctx.args)
      |> Keyword.put(:kwargs, retry_exc.kwargs || ctx.kwargs)

    Celery.send_task(app, ctx.task, retry_opts)
    {:retry, retry_exc}
  end

  defp should_autoretry?(
         %Definition{autoretry_for: list, max_retries: max_r},
         %Context{} = ctx,
         %{
           __struct__: exc_mod
         }
       )
       when is_list(list) and list != [] do
    matches? = Enum.any?(list, fn mod -> mod == exc_mod or mod == Exception end)
    under_limit? = max_r == :infinity or is_nil(max_r) or (ctx.retries || 0) < max_r
    matches? and under_limit?
  end

  defp should_autoretry?(_defn, _ctx, _exc), do: false

  defp build_autoretry(%Definition{} = defn, %Context{} = ctx, exc) do
    retries = ctx.retries || 0

    countdown =
      case defn.retry_backoff do
        false ->
          defn.default_retry_delay

        nil ->
          defn.default_retry_delay

        true ->
          compute_backoff(1, retries, defn.retry_backoff_max, defn.retry_jitter)

        factor when is_number(factor) ->
          compute_backoff(factor, retries, defn.retry_backoff_max, defn.retry_jitter)
      end

    retry_opts = [
      task_id: ctx.id,
      retries: retries + 1,
      eta: Celery.Protocol.format_time_option(nil, countdown),
      root_id: ctx.root_id,
      parent_id: ctx.parent_id,
      group_id: ctx.group,
      group_index: ctx.group_index,
      chord: ctx.chord,
      chain: ctx.chain,
      callbacks: ctx.callbacks,
      errbacks: ctx.errbacks
    ]

    Celery.Retry.exception(
      exc: exc,
      when: countdown,
      args: ctx.args,
      kwargs: ctx.kwargs,
      options: retry_opts
    )
  end

  defp compute_backoff(factor, retries, max_backoff, jitter?) do
    raw = min(factor * :math.pow(2, retries), max_backoff || 600)

    if jitter? do
      :rand.uniform() * raw
    else
      raw
    end
  end

  defp dispatch_callbacks(_app, %Context{callbacks: nil}, _retval), do: :ok
  defp dispatch_callbacks(_app, %Context{callbacks: []}, _retval), do: :ok

  defp dispatch_callbacks(app, %Context{callbacks: callbacks} = ctx, retval)
       when is_list(callbacks) do
    Enum.each(callbacks, fn cb ->
      sig = Signature.from_map(cb)

      args =
        if sig.immutable do
          sig.args
        else
          [retval | sig.args]
        end

      opts =
        sig.options
        |> Enum.map(fn {k, v} -> {String.to_atom(to_string(k)), v} end)
        |> Keyword.put(:args, args)
        |> Keyword.put(:kwargs, sig.kwargs)
        |> Keyword.put_new(:root_id, ctx.root_id || ctx.id)
        |> Keyword.put_new(:parent_id, ctx.id)

      Celery.send_task(app, sig.task, opts)
    end)
  end

  defp dispatch_chain(_app, %Context{chain: nil}, _retval), do: :ok
  defp dispatch_chain(_app, %Context{chain: []}, _retval), do: :ok

  defp dispatch_chain(app, %Context{chain: chain} = ctx, retval) when is_list(chain) do
    # Python Celery stores chain elements in reverse order and pops from the end.
    {next_raw, remaining_chain} = List.pop_at(chain, -1)
    sig = Signature.from_map(next_raw)

    args =
      if sig.immutable do
        sig.args
      else
        [retval | sig.args]
      end

    opts =
      sig.options
      |> Enum.map(fn {k, v} -> {String.to_atom(to_string(k)), v} end)
      |> Keyword.put(:args, args)
      |> Keyword.put(:kwargs, sig.kwargs)
      |> Keyword.put(:chain, if(remaining_chain == [], do: nil, else: remaining_chain))
      |> Keyword.put_new(:root_id, ctx.root_id || ctx.id)
      |> Keyword.put(:parent_id, ctx.id)

    Celery.send_task(app, sig.task, opts)
  end

  defp dispatch_errbacks(_app, %Context{errbacks: nil}, _exc, _tb), do: :ok
  defp dispatch_errbacks(_app, %Context{errbacks: []}, _exc, _tb), do: :ok

  defp dispatch_errbacks(app, %Context{errbacks: errbacks} = ctx, _exc, _tb)
       when is_list(errbacks) do
    Enum.each(errbacks, fn eb ->
      sig = Signature.from_map(eb)

      args =
        if sig.immutable do
          sig.args
        else
          [ctx.id | sig.args]
        end

      opts =
        sig.options
        |> Enum.map(fn {k, v} -> {String.to_atom(to_string(k)), v} end)
        |> Keyword.put(:args, args)
        |> Keyword.put(:kwargs, sig.kwargs)
        |> Keyword.put_new(:root_id, ctx.root_id || ctx.id)
        |> Keyword.put_new(:parent_id, ctx.id)

      Celery.send_task(app, sig.task, opts)
    end)
  end

  defp resolve_time_limit_ms(%Context{timelimit: [hard | _]}, defn, config) do
    sec = hard || defn.time_limit || config.task_time_limit
    if is_number(sec) and sec > 0, do: round(sec * 1000), else: nil
  end

  defp resolve_time_limit_ms(_ctx, defn, config) do
    sec = defn.time_limit || config.task_time_limit
    if is_number(sec) and sec > 0, do: round(sec * 1000), else: nil
  end
end
