defmodule Kombu.Transport.Redis.FakeRedis do
  @moduledoc """
  In-memory Redis server mock supporting the Redis commands used by both
  `Kombu.Transport.Redis` and `Celery.Backend.Redis` (Strings, Hashes, Lists,
  Sets, Sorted Sets, Pub/Sub, Multi/Exec, Eval) so unit tests run with zero
  external dependencies.
  """
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def command(pid, cmd, _opts \\ []) when is_list(cmd) do
    GenServer.call(pid, {:command, Enum.map(cmd, &to_string/1)})
  end

  def pipeline(pid, cmds, _opts \\ []) when is_list(cmds) do
    GenServer.call(pid, {:pipeline, Enum.map(cmds, fn c -> Enum.map(c, &to_string/1) end)})
  end

  def psubscribe(pid, patterns, subscriber_pid \\ self()) do
    GenServer.call(pid, {:psubscribe, List.wrap(patterns), subscriber_pid})
  end

  def punsubscribe(pid, patterns, subscriber_pid \\ self()) do
    GenServer.call(pid, {:punsubscribe, List.wrap(patterns), subscriber_pid})
  end

  def flushall(pid) do
    GenServer.call(pid, :flushall)
  end

  @impl true
  def init(_opts) do
    {:ok, empty_state()}
  end

  defp empty_state do
    %{
      kv: %{},
      hashes: %{},
      lists: %{},
      sets: %{},
      zsets: %{},
      pubsub: %{},
      psub: %{}
    }
  end

  @impl true
  def handle_call(:flushall, _from, _state) do
    {:reply, {:ok, "OK"}, empty_state()}
  end

  def handle_call({:command, cmd}, _from, state) do
    {reply, new_state} = exec_cmd(cmd, state)
    {:reply, {:ok, reply}, new_state}
  end

  def handle_call({:pipeline, cmds}, _from, state) do
    {replies, new_state} =
      cmds
      |> flatten_multi_exec()
      |> Enum.map_reduce(state, fn cmd, acc -> exec_cmd(cmd, acc) end)

    {:reply, {:ok, replies}, new_state}
  end

  def handle_call({:psubscribe, patterns, pid}, _from, state) do
    new_psub =
      Enum.reduce(patterns, state.psub, fn pat, acc ->
        Map.update(acc, pat, MapSet.new([pid]), &MapSet.put(&1, pid))
      end)

    {:reply, {:ok, make_ref()}, %{state | psub: new_psub}}
  end

  def handle_call({:punsubscribe, patterns, pid}, _from, state) do
    new_psub =
      Enum.reduce(patterns, state.psub, fn pat, acc ->
        Map.update(acc, pat, MapSet.new(), &MapSet.delete(&1, pid))
      end)

    {:reply, :ok, %{state | psub: new_psub}}
  end

  defp flatten_multi_exec(cmds) do
    if Enum.any?(cmds, &(&1 == ["MULTI"])) and Enum.any?(cmds, &(&1 == ["EXEC"])) do
      cmds
    else
      cmds
    end
  end

  defp exec_cmd(["PING"], state), do: {"PONG", state}
  defp exec_cmd(["MULTI"], state), do: {"OK", state}
  defp exec_cmd(["EXEC"], state), do: {[], state}

  defp exec_cmd(["GET", key], state) do
    {Map.get(state.kv, key), state}
  end

  defp exec_cmd(["MGET" | keys], state) do
    vals = Enum.map(keys, &Map.get(state.kv, &1))
    {vals, state}
  end

  defp exec_cmd(["SET", key, val | _rest], state) do
    {"OK", %{state | kv: Map.put(state.kv, key, val)}}
  end

  defp exec_cmd(["SETEX", key, _seconds, val], state) do
    {"OK", %{state | kv: Map.put(state.kv, key, val)}}
  end

  defp exec_cmd(["SETNX", key, val], state) do
    if Map.has_key?(state.kv, key) do
      {0, state}
    else
      {1, %{state | kv: Map.put(state.kv, key, val)}}
    end
  end

  defp exec_cmd(["INCR", key], state) do
    exec_cmd(["INCRBY", key, "1"], state)
  end

  defp exec_cmd(["INCRBY", key, by_str], state) do
    by = String.to_integer(by_str)
    cur = state.kv |> Map.get(key, "0") |> String.to_integer()
    next_val = cur + by
    {next_val, %{state | kv: Map.put(state.kv, key, Integer.to_string(next_val))}}
  end

  defp exec_cmd(["DEL" | keys], state) do
    new_state =
      Enum.reduce(keys, state, fn k, acc ->
        %{
          acc
          | kv: Map.delete(acc.kv, k),
            hashes: Map.delete(acc.hashes, k),
            lists: Map.delete(acc.lists, k),
            sets: Map.delete(acc.sets, k),
            zsets: Map.delete(acc.zsets, k)
        }
      end)

    {length(keys), new_state}
  end

  defp exec_cmd(["EXISTS" | keys], state) do
    count =
      Enum.count(keys, fn k ->
        Map.has_key?(state.kv, k) or
          Map.has_key?(state.hashes, k) or
          Map.has_key?(state.lists, k) or
          Map.has_key?(state.sets, k) or
          Map.has_key?(state.zsets, k)
      end)

    {count, state}
  end

  defp exec_cmd(["EXPIRE", _key, _sec], state), do: {1, state}

  defp exec_cmd(["LPUSH", key | vals], state) do
    list = Map.get(state.lists, key, [])
    new_list = Enum.reverse(vals) ++ list
    {length(new_list), %{state | lists: Map.put(state.lists, key, new_list)}}
  end

  defp exec_cmd(["RPUSH", key | vals], state) do
    list = Map.get(state.lists, key, [])
    new_list = list ++ vals
    {length(new_list), %{state | lists: Map.put(state.lists, key, new_list)}}
  end

  defp exec_cmd(["LPOP", key], state) do
    case Map.get(state.lists, key, []) do
      [] -> {nil, state}
      [h | t] -> {h, %{state | lists: Map.put(state.lists, key, t)}}
    end
  end

  defp exec_cmd(["RPOP", key], state) do
    case Map.get(state.lists, key, []) do
      [] ->
        {nil, state}

      list ->
        val = List.last(list)
        rest = Enum.drop(list, -1)
        {val, %{state | lists: Map.put(state.lists, key, rest)}}
    end
  end

  defp exec_cmd(["BRPOP" | args], state) do
    keys = Enum.drop(args, -1)

    found =
      Enum.find_value(keys, fn k ->
        case Map.get(state.lists, k, []) do
          [] -> nil
          list -> {k, List.last(list), Enum.drop(list, -1)}
        end
      end)

    case found do
      nil ->
        {nil, state}

      {k, val, rest} ->
        {[k, val], %{state | lists: Map.put(state.lists, k, rest)}}
    end
  end

  defp exec_cmd(["LLEN", key], state) do
    {length(Map.get(state.lists, key, [])), state}
  end

  defp exec_cmd(["LRANGE", key, start_s, stop_s], state) do
    list = Map.get(state.lists, key, [])
    {slice_redis(list, String.to_integer(start_s), String.to_integer(stop_s)), state}
  end

  defp exec_cmd(["LREM", key, _count, val], state) do
    list = Map.get(state.lists, key, [])
    new_list = Enum.reject(list, &(&1 == val))
    {length(list) - length(new_list), %{state | lists: Map.put(state.lists, key, new_list)}}
  end

  defp exec_cmd(["HSET", key, field, val], state) do
    h = Map.get(state.hashes, key, %{})
    added = if Map.has_key?(h, field), do: 0, else: 1
    {added, %{state | hashes: Map.put(state.hashes, key, Map.put(h, field, val))}}
  end

  defp exec_cmd(["HGET", key, field], state) do
    val = state.hashes |> Map.get(key, %{}) |> Map.get(field)
    {val, state}
  end

  defp exec_cmd(["HDEL", key | fields], state) do
    h = Map.get(state.hashes, key, %{})
    new_h = Map.drop(h, fields)
    {map_size(h) - map_size(new_h), %{state | hashes: Map.put(state.hashes, key, new_h)}}
  end

  defp exec_cmd(["SADD", key | members], state) do
    s = Map.get(state.sets, key, MapSet.new())
    new_s = Enum.reduce(members, s, &MapSet.put(&2, &1))
    {MapSet.size(new_s) - MapSet.size(s), %{state | sets: Map.put(state.sets, key, new_s)}}
  end

  defp exec_cmd(["SREM", key | members], state) do
    s = Map.get(state.sets, key, MapSet.new())
    new_s = Enum.reduce(members, s, &MapSet.delete(&2, &1))
    {MapSet.size(s) - MapSet.size(new_s), %{state | sets: Map.put(state.sets, key, new_s)}}
  end

  defp exec_cmd(["SMEMBERS", key], state) do
    s = Map.get(state.sets, key, MapSet.new())
    {MapSet.to_list(s), state}
  end

  defp exec_cmd(["ZADD", key, "NX", score_s, member], state) do
    z = Map.get(state.zsets, key, %{})

    if Map.has_key?(z, member) do
      {0, state}
    else
      {score, _} = Float.parse(score_s)
      {1, %{state | zsets: Map.put(state.zsets, key, Map.put(z, member, score))}}
    end
  end

  defp exec_cmd(["ZADD", key, score_s, member], state) do
    z = Map.get(state.zsets, key, %{})
    {score, _} = Float.parse(score_s)
    added = if Map.has_key?(z, member), do: 0, else: 1
    {added, %{state | zsets: Map.put(state.zsets, key, Map.put(z, member, score))}}
  end

  defp exec_cmd(["ZREM", key | members], state) do
    z = Map.get(state.zsets, key, %{})
    new_z = Map.drop(z, members)
    {map_size(z) - map_size(new_z), %{state | zsets: Map.put(state.zsets, key, new_z)}}
  end

  defp exec_cmd(["ZCARD", key], state) do
    {map_size(Map.get(state.zsets, key, %{})), state}
  end

  defp exec_cmd(["ZRANGE", key, start_s, stop_s], state) do
    z = Map.get(state.zsets, key, %{})

    sorted =
      z
      |> Enum.sort_by(fn {m, sc} -> {sc, m} end)
      |> Enum.map(fn {m, _sc} -> m end)

    {slice_redis(sorted, String.to_integer(start_s), String.to_integer(stop_s)), state}
  end

  defp exec_cmd(["ZRANGEBYSCORE", key, min_s, max_s | rest], state) do
    z = Map.get(state.zsets, key, %{})
    min_v = parse_score_bound(min_s, :neg_inf)
    max_v = parse_score_bound(max_s, :pos_inf)

    filtered =
      z
      |> Enum.filter(fn {_m, sc} -> score_gte?(sc, min_v) and score_lte?(sc, max_v) end)
      |> Enum.sort_by(fn {m, sc} -> {sc, m} end)
      |> Enum.map(fn {m, _sc} -> m end)

    sliced =
      case rest do
        ["LIMIT", off_s, count_s] ->
          Enum.slice(filtered, String.to_integer(off_s), String.to_integer(count_s))

        _ ->
          filtered
      end

    {sliced, state}
  end

  defp exec_cmd(["PUBLISH", channel, payload], state) do
    pids = Map.get(state.pubsub, channel, MapSet.new())

    Enum.each(pids, fn pid ->
      send(pid, {:redix_pubsub, self(), :ref, :message, %{channel: channel, payload: payload}})
    end)

    psub_count =
      Enum.reduce(state.psub, 0, fn {pat, sub_pids}, acc ->
        if glob_match?(pat, channel) do
          Enum.each(sub_pids, fn pid ->
            send(
              pid,
              {:redix_pubsub, self(), :ref, :pmessage,
               %{channel: channel, pattern: pat, payload: payload}}
            )
          end)

          acc + MapSet.size(sub_pids)
        else
          acc
        end
      end)

    {MapSet.size(pids) + psub_count, state}
  end

  defp exec_cmd(["EVALSHA", sha, numkeys_str | rest], state) do
    refresh_sha =
      Kombu.Transport.Redis.LuaScripts.sha1(
        Kombu.Transport.Redis.LuaScripts.refresh_binding_script()
      )

    if sha == refresh_sha do
      exec_refresh_binding(String.to_integer(numkeys_str), rest, state)
    else
      {"NOSCRIPT No matching script. Please use EVAL.", state}
    end
  end

  defp exec_cmd(["EVAL", script, numkeys_str | rest], state) do
    if script == Kombu.Transport.Redis.LuaScripts.refresh_binding_script() do
      exec_refresh_binding(String.to_integer(numkeys_str), rest, state)
    else
      {"OK", state}
    end
  end

  defp exec_cmd(_other, state), do: {"OK", state}

  defp exec_refresh_binding(2, [key1, key2, member, exchange, ttl_str], state) do
    {_, state} = exec_cmd(["SADD", key1, member], state)
    deadline = System.system_time(:millisecond) + String.to_integer(ttl_str)
    {_, state} = exec_cmd(["ZADD", key2, Integer.to_string(deadline), exchange], state)
    {1, state}
  end

  defp glob_match?(pattern, channel) do
    regex_src =
      pattern
      |> Regex.escape()
      |> String.replace("\\*", ".*")
      |> String.replace("\\?", ".")

    Regex.match?(Regex.compile!("\\A" <> regex_src <> "\\z"), channel)
  end

  defp slice_redis([], _start, _stop), do: []

  defp slice_redis(list, start, stop) do
    len = length(list)
    s = if start < 0, do: max(len + start, 0), else: start
    e = if stop < 0, do: len + stop, else: min(stop, len - 1)
    if s > e or s >= len or e < 0, do: [], else: Enum.slice(list, s..e)
  end

  defp parse_score_bound("-inf", _default), do: :neg_inf
  defp parse_score_bound("+inf", _default), do: :pos_inf
  defp parse_score_bound("inf", _default), do: :pos_inf

  defp parse_score_bound(str, _default) do
    {val, _} = Float.parse(String.trim_leading(str, "("))
    val
  end

  defp score_gte?(_sc, :neg_inf), do: true
  defp score_gte?(sc, val), do: sc >= val

  defp score_lte?(_sc, :pos_inf), do: true
  defp score_lte?(sc, val), do: sc <= val
end
