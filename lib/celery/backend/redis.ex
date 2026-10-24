defmodule Celery.Backend.Redis do
  @moduledoc """
  Redis result store backend for Celery (ported from `celery.backends.redis.RedisBackend`
  and `celery.backends.base.KeyValueStoreBackend`).

  Stores and retrieves task state, results, tracebacks, group metadata, and chord counters
  in Redis using the exact key layout and JSON wire structure of Python Celery:
  - Task metadata key: `celery-task-meta-<task_id>`
  - Group metadata key: `celery-taskset-meta-<group_id>`
  - Chord unlock key: `chord-unlock-<group_id>`
  - Pub/Sub channel: `celery-task-meta-<task_id>` (published on every `store_result`)
  """

  alias Celery.Config
  alias Celery.States
  alias Celery.Task.Context

  @task_keyprefix "celery-task-meta-"
  @group_keyprefix "celery-taskset-meta-"
  @chord_keyprefix "chord-unlock-"

  @doc """
  Returns the Redis key for a task ID (`"celery-task-meta-<task_id>"`, prefixed with
  `global_keyprefix` if configured).
  """
  @spec get_key_for_task(Celery.t() | Config.t(), String.t(), String.t()) :: String.t()
  def get_key_for_task(app_or_config, task_id, key \\ "") when is_binary(task_id) do
    prefix = global_keyprefix(app_or_config)
    suffix = if key != "", do: ".#{key}", else: ""
    "#{prefix}#{@task_keyprefix}#{task_id}#{suffix}"
  end

  @doc """
  Returns the Redis key for a group/taskset ID (`"celery-taskset-meta-<group_id>"`).
  """
  @spec get_key_for_group(Celery.t() | Config.t(), String.t(), String.t()) :: String.t()
  def get_key_for_group(app_or_config, group_id, key \\ "") when is_binary(group_id) do
    prefix = global_keyprefix(app_or_config)
    suffix = if key != "", do: ".#{key}", else: ""
    "#{prefix}#{@group_keyprefix}#{group_id}#{suffix}"
  end

  @doc """
  Returns the Redis key for a chord ID (`"chord-unlock-<group_id>"`).
  """
  @spec get_key_for_chord(Celery.t() | Config.t(), String.t(), String.t()) :: String.t()
  def get_key_for_chord(app_or_config, group_id, key \\ "") when is_binary(group_id) do
    prefix = global_keyprefix(app_or_config)
    suffix = if key != "", do: ".#{key}", else: ""
    "#{prefix}#{@chord_keyprefix}#{group_id}#{suffix}"
  end

  @doc """
  Encodes a task result metadata dictionary into the exact JSON format stored in Redis
  by Python Celery.
  """
  @spec encode_result_meta(
          Celery.t() | Config.t(),
          String.t(),
          term(),
          String.t(),
          keyword()
        ) :: {:ok, String.t(), map()} | {:error, term()}
  def encode_result_meta(app_or_config, task_id, result, state, opts \\ []) do
    config = extract_config(app_or_config)
    traceback = Keyword.get(opts, :traceback)
    request = Keyword.get(opts, :request)
    children = Keyword.get(opts, :children) || []

    date_done =
      if States.ready?(state) do
        Keyword.get_lazy(opts, :date_done, fn ->
          DateTime.utc_now() |> NaiveDateTime.to_iso8601()
        end)
      else
        nil
      end

    encoded_result = prepare_result_payload(result, state)

    meta =
      %{
        "status" => state,
        "result" => encoded_result,
        "traceback" => traceback,
        "children" => children,
        "date_done" => date_done,
        "task_id" => task_id
      }
      |> maybe_add_request_meta(request, config.result_extended)

    {_ct, _ce, payload} = Kombu.Serialization.dumps(meta, config.result_serializer)
    {:ok, payload, meta}
  end

  @doc """
  Stores task state and result in Redis and publishes the metadata to the task's
  Redis Pub/Sub channel (`SETEX key expires value` + `PUBLISH key value`).

  Matches Python Celery's deduplication behavior: if the existing state in Redis is
  already `"SUCCESS"`, does not overwrite it.
  """
  @spec store_result(
          Celery.t() | Config.t(),
          String.t(),
          term(),
          String.t(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  def store_result(app_or_config, task_id, result, state, opts \\ []) do
    config = extract_config(app_or_config)

    if is_nil(config.result_backend) and is_nil(config.redis_conn) do
      {:ok, result}
    else
      key = get_key_for_task(config, task_id)
      {:ok, payload, _meta} = encode_result_meta(config, task_id, result, state, opts)

      with_redis(config, fn conn ->
        current_raw =
          case redis_command(conn, ["GET", key]) do
            {:ok, val} -> val
            _ -> nil
          end

        already_succeeded? =
          case decode_raw_meta(config, current_raw) do
            {:ok, %{"status" => "SUCCESS"}} -> true
            _ -> false
          end

        if already_succeeded? do
          {:ok, result}
        else
          set_cmd =
            case config.result_expires do
              exp when is_integer(exp) and exp > 0 ->
                ["SETEX", key, Integer.to_string(exp), payload]

              _ ->
                ["SET", key, payload]
            end

          case redis_pipeline(conn, [set_cmd, ["PUBLISH", key, payload]]) do
            {:ok, _} ->
              {:ok, result}

            {:error, reason} ->
              {:error,
               Celery.BackendStoreError.exception(
                 task_id: task_id,
                 state: state,
                 reason: reason
               )}
          end
        end
      end)
    end
  end

  @doc """
  Marks a task as `STARTED` in the result backend (`meta` typically includes `%{pid: ..., hostname: ...}`).
  """
  @spec mark_as_started(Celery.t() | Config.t(), String.t(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def mark_as_started(app_or_config, task_id, meta \\ %{}, opts \\ []) do
    store_result(app_or_config, task_id, meta, States.started(), opts)
  end

  @doc """
  Marks a task as `SUCCESS` in the result backend and triggers chord completion if the
  request is part of a chord.
  """
  @spec mark_as_done(Celery.t() | Config.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def mark_as_done(app_or_config, task_id, result, opts \\ []) do
    request = Keyword.get(opts, :request)
    store_flag = Keyword.get(opts, :store_result, true)
    state = Keyword.get(opts, :state, States.success())
    ignore_result = request_ignore_result?(request)

    res =
      if store_flag and not ignore_result do
        store_result(app_or_config, task_id, result, state, opts)
      else
        {:ok, result}
      end

    if request && request.chord do
      on_chord_part_return(app_or_config, request, state, result)
    end

    res
  end

  @doc """
  Marks a task as `FAILURE` in the result backend, triggers chord failure handling if part
  of a chord, propagates failure down any attached chain, and invokes errbacks.
  """
  @spec mark_as_failure(Celery.t() | Config.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def mark_as_failure(app_or_config, task_id, exc, opts \\ []) do
    request = Keyword.get(opts, :request)
    store_flag = Keyword.get(opts, :store_result, true)
    state = Keyword.get(opts, :state, States.failure())

    res =
      if store_flag do
        store_result(app_or_config, task_id, exc, state, opts)
      else
        {:ok, exc}
      end

    if request do
      if request.chord do
        on_chord_part_return(app_or_config, request, state, exc)
      end

      if is_list(request.chain) and request.chain != [] do
        fail_chain(app_or_config, request.chain, exc, opts)
      end
    end

    res
  end

  @doc """
  Marks a task as `RETRY` in the result backend.
  """
  @spec mark_as_retry(Celery.t() | Config.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def mark_as_retry(app_or_config, task_id, exc, opts \\ []) do
    state = Keyword.get(opts, :state, States.retry())
    store_result(app_or_config, task_id, exc, state, opts)
  end

  @doc """
  Marks a task as `REVOKED` in the result backend.
  """
  @spec mark_as_revoked(Celery.t() | Config.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def mark_as_revoked(app_or_config, task_id, reason \\ "revoked", opts \\ []) do
    request = Keyword.get(opts, :request)
    store_flag = Keyword.get(opts, :store_result, true)
    state = Keyword.get(opts, :state, States.revoked())
    exc = Celery.TaskRevokedError.exception(reason: reason)

    res =
      if store_flag do
        store_result(app_or_config, task_id, exc, state, opts)
      else
        {:ok, exc}
      end

    if request && request.chord do
      on_chord_part_return(app_or_config, request, state, exc)
    end

    res
  end

  @doc """
  Fetches the decoded task metadata dictionary from Redis.
  If the task does not exist in Redis, returns `%{"status" => "PENDING", "result" => nil}`.
  """
  @spec get_task_meta(Celery.t() | Config.t(), String.t()) :: map()
  def get_task_meta(app_or_config, task_id) when is_binary(task_id) do
    config = extract_config(app_or_config)
    key = get_key_for_task(config, task_id)

    raw =
      with_redis(config, fn conn ->
        case redis_command(conn, ["GET", key]) do
          {:ok, val} -> val
          _ -> nil
        end
      end)

    case decode_raw_meta(config, raw) do
      {:ok, nil} ->
        %{"status" => States.pending(), "result" => nil}

      {:ok, meta} when is_map(meta) ->
        normalize_loaded_meta(meta)

      _ ->
        %{"status" => States.pending(), "result" => nil}
    end
  end

  @doc """
  Fetches metadata for multiple task IDs in a single `MGET` call.
  """
  @spec get_many(Celery.t() | Config.t(), [String.t()]) :: %{String.t() => map()}
  def get_many(_app_or_config, []), do: %{}

  def get_many(app_or_config, task_ids) when is_list(task_ids) do
    config = extract_config(app_or_config)
    keys = Enum.map(task_ids, &get_key_for_task(config, &1))

    values =
      with_redis(config, fn conn ->
        case redis_command(conn, ["MGET" | keys]) do
          {:ok, list} when is_list(list) -> list
          _ -> Enum.map(keys, fn _ -> nil end)
        end
      end)

    task_ids
    |> Enum.zip(values)
    |> Map.new(fn {tid, raw} ->
      meta =
        case decode_raw_meta(config, raw) do
          {:ok, m} when is_map(m) -> normalize_loaded_meta(m)
          _ -> %{"status" => States.pending(), "result" => nil}
        end

      {tid, meta}
    end)
  end

  @doc "Returns the current task state (`\"PENDING\"`, `\"SUCCESS\"`, `\"FAILURE\"`, etc.)."
  @spec get_state(Celery.t() | Config.t(), String.t()) :: String.t()
  def get_state(app_or_config, task_id) do
    app_or_config
    |> get_task_meta(task_id)
    |> Map.get("status", States.pending())
  end

  @doc "Returns the task result (or reconstructed `%Celery.RemoteError{}` if failed)."
  @spec get_result(Celery.t() | Config.t(), String.t()) :: term()
  def get_result(app_or_config, task_id) do
    app_or_config
    |> get_task_meta(task_id)
    |> Map.get("result")
  end

  @doc "Returns the task traceback string (or `nil`)."
  @spec get_traceback(Celery.t() | Config.t(), String.t()) :: String.t() | nil
  def get_traceback(app_or_config, task_id) do
    app_or_config
    |> get_task_meta(task_id)
    |> Map.get("traceback")
  end

  @doc "Deletes the task's result metadata from Redis."
  @spec forget(Celery.t() | Config.t(), String.t()) :: :ok
  def forget(app_or_config, task_id) when is_binary(task_id) do
    config = extract_config(app_or_config)
    key = get_key_for_task(config, task_id)

    with_redis(config, fn conn ->
      redis_command(conn, ["DEL", key])
    end)

    :ok
  end

  @doc """
  Waits for a task to reach one of `States.ready_states()` (`SUCCESS`, `FAILURE`, `REVOKED`),
  returning the result or raising if `propagate: true` (default) and the task failed/revoked.

  Options:
  - `:timeout` - timeout in milliseconds or seconds (float, e.g. `5.0`, or integer ms `5000`, or `:infinity`; default `:infinity`)
  - `:interval` - polling interval in milliseconds (default `50`)
  - `:propagate` - whether to raise on `FAILURE` / `REVOKED` (default `true`)
  """
  @spec wait_for(Celery.t() | Config.t(), String.t(), keyword()) :: term()
  def wait_for(app_or_config, task_id, opts \\ []) do
    timeout_ms = normalize_timeout_ms(Keyword.get(opts, :timeout, :infinity))
    interval_ms = normalize_interval_ms(Keyword.get(opts, :interval, 50))
    propagate = Keyword.get(opts, :propagate, true)

    deadline =
      if timeout_ms == :infinity,
        do: :infinity,
        else: System.monotonic_time(:millisecond) + timeout_ms

    meta = do_wait_for(app_or_config, task_id, interval_ms, deadline)
    status = Map.get(meta, "status")
    result = Map.get(meta, "result")

    if propagate and States.propagate?(status) do
      case result do
        %{__exception__: true} = exc -> raise exc
        other -> raise Celery.Error, message: "Task #{task_id} failed: #{inspect(other)}"
      end
    else
      result
    end
  end

  @doc """
  Saves group result metadata under `celery-taskset-meta-<group_id>`.
  """
  @spec save_group(Celery.t() | Config.t(), String.t(), map()) :: :ok | {:error, term()}
  def save_group(app_or_config, group_id, group_meta) when is_binary(group_id) do
    config = extract_config(app_or_config)
    key = get_key_for_group(config, group_id)
    payload_map = %{"result" => group_meta}
    {_ct, _ce, payload} = Kombu.Serialization.dumps(payload_map, config.result_serializer)

    with_redis(config, fn conn ->
      set_cmd =
        case config.result_expires do
          exp when is_integer(exp) and exp > 0 ->
            ["SETEX", key, Integer.to_string(exp), payload]

          _ ->
            ["SET", key, payload]
        end

      case redis_command(conn, set_cmd) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  @doc """
  Restores group result metadata from `celery-taskset-meta-<group_id>`.
  """
  @spec restore_group(Celery.t() | Config.t(), String.t()) :: map() | nil
  def restore_group(app_or_config, group_id) when is_binary(group_id) do
    config = extract_config(app_or_config)
    key = get_key_for_group(config, group_id)

    raw =
      with_redis(config, fn conn ->
        case redis_command(conn, ["GET", key]) do
          {:ok, val} -> val
          _ -> nil
        end
      end)

    case decode_raw_meta(config, raw) do
      {:ok, %{"result" => res}} -> res
      _ -> nil
    end
  end

  @doc """
  Deletes group metadata from `celery-taskset-meta-<group_id>`.
  """
  @spec delete_group(Celery.t() | Config.t(), String.t()) :: :ok
  def delete_group(app_or_config, group_id) when is_binary(group_id) do
    config = extract_config(app_or_config)
    key = get_key_for_group(config, group_id)

    with_redis(config, fn conn ->
      redis_command(conn, ["DEL", key])
    end)

    :ok
  end

  @doc """
  Sets the expected chord size in Redis (`celery-taskset-meta-<group_id>.s`), matching
  Python Celery's `RedisBackend.set_chord_size/2`.
  """
  @spec set_chord_size(Celery.t() | Config.t(), String.t(), non_neg_integer()) :: :ok
  def set_chord_size(app_or_config, group_id, chord_size)
      when is_binary(group_id) and is_integer(chord_size) do
    config = extract_config(app_or_config)
    skey = get_key_for_group(config, group_id, "s")

    with_redis(config, fn conn ->
      redis_command(conn, ["SET", skey, Integer.to_string(chord_size)])
    end)

    :ok
  end

  @doc """
  Handles completion of a chord header part (`on_chord_part_return` in `celery/backends/redis.py`).

  Records `[1, task_id, state, encoded_result]` in `celery-taskset-meta-<gid>.j` (using `ZADD`
  when `result_chord_ordered` is true and `group_index` is present, or `RPUSH` otherwise) and
  triggers the chord callback once all parts have completed.
  """
  @spec on_chord_part_return(Celery.t() | Config.t(), Context.t(), String.t(), term()) :: :ok
  def on_chord_part_return(app_or_config, %Context{} = request, state, result) do
    config = extract_config(app_or_config)
    gid = request.group
    tid = request.id
    chord = request.chord

    if is_binary(gid) and is_map(chord) do
      jkey = get_key_for_group(config, gid, "j")
      tkey = get_key_for_group(config, gid, "t")
      skey = get_key_for_group(config, gid, "s")

      encoded_res = prepare_result_payload(result, state)

      {_ct, _ce, serialized_entry} =
        Kombu.Serialization.dumps([1, tid, state, encoded_res], config.result_serializer)

      group_index = request.group_index

      add_cmd =
        if config.result_chord_ordered and not is_nil(group_index) do
          ["ZADD", jkey, to_string(group_index), serialized_entry]
        else
          ["RPUSH", jkey, serialized_entry]
        end

      count_cmd =
        if config.result_chord_ordered and not is_nil(group_index) do
          ["ZCARD", jkey]
        else
          ["LLEN", jkey]
        end

      with_redis(config, fn conn ->
        {:ok, [_, readycount_raw, totaldiff_raw, chord_size_raw | _]} =
          redis_pipeline(conn, [
            add_cmd,
            count_cmd,
            ["GET", tkey],
            ["GET", skey]
          ])

        readycount = to_int(readycount_raw, 0)
        totaldiff = to_int(totaldiff_raw, 0)

        chord_size =
          case chord_size_raw do
            nil -> Map.get(chord, "chord_size") || get_in(chord, ["options", "chord_size"])
            val -> to_int(val, 0)
          end

        total = if is_integer(chord_size), do: chord_size + totaldiff, else: nil

        if is_integer(total) and total > 0 and readycount == total do
          raw_entries =
            if config.result_chord_ordered and not is_nil(group_index) do
              case redis_command(conn, ["ZRANGE", jkey, "0", "-1"]) do
                {:ok, list} when is_list(list) -> list
                _ -> []
              end
            else
              case redis_command(conn, ["LRANGE", jkey, "0", "-1"]) do
                {:ok, list} when is_list(list) -> list
                _ -> []
              end
            end

          redis_pipeline(conn, [["DEL", jkey], ["DEL", tkey], ["DEL", skey]])

          decoded_entries =
            Enum.map(raw_entries, fn raw_item ->
              [_ver, _part_tid, part_state, part_res] =
                Kombu.Serialization.loads(raw_item, "application/json", "utf-8",
                  accept: Kombu.Serialization.prepare_accept_content(["json"])
                )

              {part_state, part_res}
            end)

          failed_entry =
            Enum.find(decoded_entries, fn {part_state, _} ->
              States.exception?(part_state)
            end)

          case failed_entry do
            {_err_state, err_payload} ->
              callback_id =
                get_in(chord, ["options", "task_id"]) || Map.get(chord, "task_id")

              if is_binary(callback_id) do
                chord_exc =
                  Celery.ChordError.exception("Dependency failed: #{inspect(err_payload)}")

                store_result(config, callback_id, chord_exc, States.failure())
              end

            nil ->
              header_results = Enum.map(decoded_entries, fn {_st, r} -> r end)
              dispatch_chord_callback(app_or_config, chord, header_results, request)
          end
        end
      end)
    end

    :ok
  end

  @doc """
  Serializes an Elixir exception or error term into Python Celery's JSON exception dictionary:
  `%{"exc_type" => ..., "exc_message" => [...], "exc_module" => ...}`.
  """
  @spec prepare_exception(term()) :: map()
  def prepare_exception(%{"exc_type" => _, "exc_message" => _} = already_prepared) do
    already_prepared
  end

  def prepare_exception(%Celery.RemoteError{} = remote) do
    %{
      "exc_type" => remote.exc_type || "Exception",
      "exc_message" => normalize_exc_message_list(remote.exc_message),
      "exc_module" => remote.exc_module || "builtins"
    }
  end

  def prepare_exception(%Celery.TaskRevokedError{reason: reason}) do
    %{
      "exc_type" => "TaskRevokedError",
      "exc_message" => [to_string(reason || "revoked")],
      "exc_module" => "celery.exceptions"
    }
  end

  def prepare_exception(%Celery.MaxRetriesExceededError{message: msg}) do
    %{
      "exc_type" => "MaxRetriesExceededError",
      "exc_message" => [to_string(msg)],
      "exc_module" => "celery.exceptions"
    }
  end

  def prepare_exception(%Celery.ChordError{message: msg}) do
    %{
      "exc_type" => "ChordError",
      "exc_message" => [to_string(msg)],
      "exc_module" => "celery.exceptions"
    }
  end

  def prepare_exception(%{__exception__: true, __struct__: mod} = exc) do
    {exc_module, exc_type} = map_exception_type(mod)
    msg = Exception.message(exc)

    %{
      "exc_type" => exc_type,
      "exc_message" => [msg],
      "exc_module" => exc_module
    }
  end

  def prepare_exception(bin) when is_binary(bin) do
    %{
      "exc_type" => "RuntimeError",
      "exc_message" => [bin],
      "exc_module" => "builtins"
    }
  end

  def prepare_exception(other) do
    %{
      "exc_type" => "RuntimeError",
      "exc_message" => [inspect(other)],
      "exc_module" => "builtins"
    }
  end

  @doc """
  Converts a stored exception dictionary (`%{"exc_type" => ..., "exc_message" => ..., "exc_module" => ...}`)
  into an Elixir exception struct (`%Celery.RemoteError{}` or `%Celery.TaskRevokedError{}`).
  """
  @spec exception_to_elixir(term(), String.t() | nil) :: Exception.t()
  def exception_to_elixir(%{__exception__: true} = exc, _tb), do: exc

  def exception_to_elixir(%{"exc_type" => "TaskRevokedError"} = map, _tb) do
    msg =
      case Map.get(map, "exc_message") do
        [first | _] -> to_string(first)
        bin when is_binary(bin) -> bin
        _ -> "revoked"
      end

    Celery.TaskRevokedError.exception(reason: msg)
  end

  def exception_to_elixir(%{"exc_type" => exc_type} = map, tb) do
    Celery.RemoteError.exception(
      exc_type: exc_type,
      exc_message: Map.get(map, "exc_message", ""),
      exc_module: Map.get(map, "exc_module", "builtins"),
      traceback: tb
    )
  end

  def exception_to_elixir(other, tb) do
    Celery.RemoteError.exception(
      exc_type: "Exception",
      exc_message: inspect(other),
      exc_module: "builtins",
      traceback: tb
    )
  end

  # --- Internal Helpers ---

  defp do_wait_for(app_or_config, task_id, interval_ms, deadline) do
    meta = get_task_meta(app_or_config, task_id)
    status = Map.get(meta, "status")

    if States.ready?(status) do
      meta
    else
      now = System.monotonic_time(:millisecond)

      if deadline != :infinity and now >= deadline do
        raise Celery.TimeoutError, message: "The operation timed out."
      else
        sleep_time =
          if deadline == :infinity do
            interval_ms
          else
            min(interval_ms, max(deadline - now, 1))
          end

        Process.sleep(sleep_time)
        do_wait_for(app_or_config, task_id, interval_ms, deadline)
      end
    end
  end

  defp fail_chain(app_or_config, chain, exc, opts) when is_list(chain) do
    Enum.each(chain, fn elem ->
      elem_map =
        case elem do
          %{__struct__: Celery.Canvas.Signature} = sig -> Celery.Canvas.Signature.to_map(sig)
          m when is_map(m) -> m
          _ -> %{}
        end

      options = Map.get(elem_map, "options") || %{}
      tid = Map.get(options, "task_id")

      if is_binary(tid) do
        store_result(app_or_config, tid, exc, States.failure(), Keyword.delete(opts, :request))
      end
    end)
  end

  defp dispatch_chord_callback(app_or_config, chord_sig_map, header_results, %Context{} = request) do
    config = extract_config(app_or_config)
    sig = Celery.Canvas.Signature.from_map(chord_sig_map)

    args =
      if sig.immutable do
        sig.args
      else
        [header_results | sig.args]
      end

    opts =
      sig.options
      |> Enum.map(fn {k, v} -> {String.to_atom(to_string(k)), v} end)
      |> Keyword.put_new(:root_id, request.root_id || request.id)
      |> Keyword.put_new(:parent_id, request.id)

    msg = Celery.AMQP.create_task_message(config, sig.task, args, sig.kwargs, opts)
    Celery.AMQP.send_task_message(config, msg, opts)
  end

  defp prepare_result_payload(result, state) do
    if States.exception?(state) do
      prepare_exception(result)
    else
      result
    end
  end

  defp normalize_loaded_meta(%{"status" => status, "result" => result} = meta) do
    if States.exception?(status) do
      tb = Map.get(meta, "traceback")
      Map.put(meta, "result", exception_to_elixir(result, tb))
    else
      meta
    end
  end

  defp normalize_loaded_meta(meta), do: meta

  defp maybe_add_request_meta(meta, nil, _extended), do: meta

  defp maybe_add_request_meta(meta, %Context{} = req, extended) do
    meta =
      meta
      |> maybe_put("group_id", req.group)
      |> maybe_put("parent_id", req.parent_id)

    if extended do
      delivery_info = req.delivery_info || %{}

      meta
      |> Map.put("name", req.task)
      |> Map.put("args", req.args)
      |> Map.put("kwargs", req.kwargs)
      |> Map.put("worker", req.hostname)
      |> Map.put("retries", req.retries)
      |> Map.put(
        "queue",
        Map.get(delivery_info, "routing_key") || Map.get(delivery_info, :routing_key)
      )
    else
      meta
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, val), do: Map.put(map, key, val)

  defp request_ignore_result?(nil), do: false
  defp request_ignore_result?(%Context{ignore_result: ignore}), do: ignore == true
  defp request_ignore_result?(%{ignore_result: ignore}), do: ignore == true
  defp request_ignore_result?(_), do: false

  defp decode_raw_meta(_config, nil), do: {:ok, nil}

  defp decode_raw_meta(config, raw) when is_binary(raw) do
    ct = Kombu.Serialization.name_to_type(config.result_serializer) || "application/json"
    accept = Kombu.Serialization.prepare_accept_content(config.accept_content)

    try do
      {:ok, Kombu.Serialization.loads(raw, ct, "utf-8", accept: accept)}
    rescue
      e -> {:error, e}
    end
  end

  defp global_keyprefix(app_or_config) do
    config = extract_config(app_or_config)
    opts = Map.new(config.result_backend_transport_options || [])

    prefix =
      Map.get(opts, :global_keyprefix) ||
        Map.get(opts, "global_keyprefix") ||
        ""

    if prefix != "" and not String.ends_with?(prefix, [":", "_", "-", "."]) do
      prefix <> "_"
    else
      prefix
    end
  end

  defp extract_config(%Config{} = config), do: config
  defp extract_config(%{config: %Config{} = config}), do: config

  defp with_redis(%Config{redis_conn: conn}, fun) when not is_nil(conn) do
    fun.(conn)
  end

  defp with_redis(%Config{result_backend: url} = config, fun) when is_binary(url) do
    {:ok, conn} = Redix.start_link(url, redix_opts(config))

    try do
      fun.(conn)
    after
      try do
        Redix.stop(conn)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp redix_opts(%Config{} = config) do
    opts = Map.new(config.result_backend_transport_options || [])

    case Map.get(opts, :ssl) || Map.get(opts, "ssl") do
      true -> [ssl: true]
      ssl_opts when is_list(ssl_opts) -> [ssl: ssl_opts]
      _ -> []
    end
  end

  defp redis_command({mod, ref}, command) when is_atom(mod) do
    mod.command(ref, command)
  end

  defp redis_command(conn, command) do
    if fake_redis?(conn) do
      apply(Kombu.Transport.Redis.FakeRedis, :command, [conn, command])
    else
      Redix.command(conn, command)
    end
  end

  defp redis_pipeline({mod, ref}, commands) when is_atom(mod) do
    mod.pipeline(ref, commands)
  end

  defp redis_pipeline(conn, commands) do
    if fake_redis?(conn) do
      apply(Kombu.Transport.Redis.FakeRedis, :pipeline, [conn, commands])
    else
      Redix.pipeline(conn, commands)
    end
  end

  defp fake_redis?(conn) when is_pid(conn) do
    case Process.info(conn, :dictionary) do
      {:dictionary, dict} ->
        case Keyword.get(dict, :"$initial_call") do
          {Kombu.Transport.Redis.FakeRedis, :init, 1} -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  defp fake_redis?(_), do: false

  defp map_exception_type(RuntimeError), do: {"builtins", "RuntimeError"}
  defp map_exception_type(ArgumentError), do: {"builtins", "ValueError"}
  defp map_exception_type(KeyError), do: {"builtins", "KeyError"}
  defp map_exception_type(ArithmeticError), do: {"builtins", "ArithmeticError"}
  defp map_exception_type(FunctionClauseError), do: {"builtins", "TypeError"}
  defp map_exception_type(MatchError), do: {"builtins", "ValueError"}
  defp map_exception_type(UndefinedFunctionError), do: {"builtins", "AttributeError"}
  defp map_exception_type(Celery.TimeoutError), do: {"celery.exceptions", "TimeoutError"}

  defp map_exception_type(mod) when is_atom(mod) do
    short_name =
      mod
      |> Module.split()
      |> List.last()

    {"builtins", short_name}
  end

  defp normalize_exc_message_list(list) when is_list(list), do: Enum.map(list, &to_string/1)
  defp normalize_exc_message_list(bin) when is_binary(bin), do: [bin]
  defp normalize_exc_message_list(other), do: [inspect(other)]

  defp normalize_timeout_ms(:infinity), do: :infinity
  defp normalize_timeout_ms(nil), do: :infinity
  defp normalize_timeout_ms(sec) when is_float(sec), do: round(sec * 1000)
  defp normalize_timeout_ms(ms) when is_integer(ms), do: ms

  defp normalize_interval_ms(sec) when is_float(sec), do: max(round(sec * 1000), 1)
  defp normalize_interval_ms(ms) when is_integer(ms), do: max(ms, 1)

  defp to_int(nil, default), do: default
  defp to_int(val, _default) when is_integer(val), do: val

  defp to_int(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {i, _} -> i
      :error -> default
    end
  end
end
