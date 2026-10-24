defmodule Celery.UnitTest do
  use ExUnit.Case, async: false

  alias Celery.Backend.Redis, as: RedisBackend
  alias Celery.Canvas
  alias Celery.Protocol
  alias Celery.Result
  alias Celery.Result.GroupResult
  alias Celery.States
  alias Kombu.Transport.Redis.FakeRedis

  setup do
    {:ok, fake_redis} = FakeRedis.start_link()

    app =
      Celery.new(
        broker: "redis://localhost:6379/0",
        backend: "redis://localhost:6379/0",
        include: ["src.tasks"],
        redis_conn: fake_redis
      )

    on_exit(fn ->
      if Process.alive?(fake_redis), do: GenServer.stop(fake_redis)
    end)

    %{app: app, fake_redis: fake_redis}
  end

  describe "Target Python Celery user scenario (src.tasks.command)" do
    test "registers Src.Tasks.command via include: [\"src.tasks\"] with bind: true, name, and metadata",
         %{app: app} do
      assert "src.tasks.command" in Celery.Registry.keys(app)
      {:ok, defn} = Celery.Registry.lookup("src.tasks.command", app)

      assert defn.name == "src.tasks.command"
      assert defn.bind == true
      assert defn.metadata == %{"owner" => "pipeline", "version" => "1.0"}

      assert defn.arg_names == [
               :pipe_result,
               :input_files,
               :output_path,
               :workflow_id,
               :task_config
             ]

      assert defn.defaults == %{
               pipe_result: nil,
               input_files: nil,
               output_path: nil,
               workflow_id: nil,
               task_config: nil
             }
    end

    test "executes Src.Tasks.command with default args via worker and returns 'This is a command'",
         %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      {:ok, res} = Celery.send_task(app, "src.tasks.command")
      assert Result.get(res, timeout: 2_000, interval: 10) == "This is a command"
      assert Result.successful?(res)
      assert Result.state(res) == States.success()
    end

    test "executes Src.Tasks.command with positional and keyword arguments mapped to defaults",
         %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      {:ok, res} =
        Src.Tasks.apply_async(app, :command,
          args: ["prev_step_output"],
          kwargs: %{
            "input_files" => ["a.txt", "b.txt"],
            "output_path" => "/tmp/out",
            "workflow_id" => "wf-123",
            "task_config" => %{"echo" => true, "mode" => "fast"}
          }
        )

      val = Result.get(res, timeout: 2_000, interval: 10)
      assert val["message"] == "This is a command"
      assert val["task_id"] == res.id
      assert val["task_name"] == "src.tasks.command"
      assert val["metadata"] == %{"owner" => "pipeline", "version" => "1.0"}
      assert val["pipe_result"] == "prev_step_output"
      assert val["input_files"] == ["a.txt", "b.txt"]
      assert val["output_path"] == "/tmp/out"
      assert val["workflow_id"] == "wf-123"
      assert val["task_config"] == %{"echo" => true, "mode" => "fast"}
    end

    test "supports eager local execution via apply_local and task_always_eager" do
      eager_app = Celery.new(include: ["src.tasks"], task_always_eager: true)
      {:ok, res} = Src.Tasks.delay(eager_app, :command, [], %{"workflow_id" => "wf-eager"})
      assert res.is_eager == true
      assert Result.get(res) == "This is a command"
    end
  end

  describe "Celery.Protocol v1 & v2 wire format" do
    test "encodes and decodes Protocol v2 task messages" do
      msg =
        Protocol.as_task_v2(
          "src.tasks.command",
          ["pipe_out"],
          %{"workflow_id" => "wf-99"},
          task_id: "11111111-2222-3333-4444-555555555555",
          retries: 2,
          group_id: "grp-1",
          group_index: 0
        )

      assert msg.headers["lang"] == "py"
      assert msg.headers["task"] == "src.tasks.command"
      assert msg.headers["id"] == "11111111-2222-3333-4444-555555555555"
      assert msg.headers["retries"] == 2
      assert msg.headers["group"] == "grp-1"
      assert msg.headers["group_index"] == 0
      assert msg.headers["argsrepr"] == "('pipe_out',)"
      assert msg.headers["kwargsrepr"] == "{'workflow_id': 'wf-99'}"
      assert msg.properties["correlation_id"] == "11111111-2222-3333-4444-555555555555"

      [args, kwargs, embed] = msg.body
      assert args == ["pipe_out"]
      assert kwargs == %{"workflow_id" => "wf-99"}

      assert embed == %{
               "callbacks" => nil,
               "errbacks" => nil,
               "chain" => nil,
               "chord" => nil
             }

      {:ok, ctx} = Protocol.decode_message(msg)
      assert ctx.id == "11111111-2222-3333-4444-555555555555"
      assert ctx.task == "src.tasks.command"
      assert ctx.args == ["pipe_out"]
      assert ctx.kwargs == %{"workflow_id" => "wf-99"}
      assert ctx.retries == 2
      assert ctx.group == "grp-1"
      assert ctx.group_index == 0
    end

    test "encodes and decodes Protocol v1 task messages" do
      msg =
        Protocol.as_task_v1(
          "src.tasks.add",
          [10, 20],
          %{},
          task_id: "v1-task-id",
          retries: 1
        )

      assert msg.headers == %{}
      assert msg.body["task"] == "src.tasks.add"
      assert msg.body["id"] == "v1-task-id"
      assert msg.body["args"] == [10, 20]
      assert msg.body["kwargs"] == %{}

      {:ok, ctx} = Protocol.decode_message(msg)
      assert ctx.id == "v1-task-id"
      assert ctx.task == "src.tasks.add"
      assert ctx.args == [10, 20]
      assert ctx.retries == 1
    end

    test "checks expiration and ETA delay calculation" do
      now = ~U[2026-06-01 12:00:00Z]
      past = "2026-06-01T11:59:50Z"
      future = "2026-06-01T12:00:05Z"

      assert Protocol.expired?(past, now) == true
      assert Protocol.expired?(future, now) == false
      assert Protocol.expired?(nil, now) == false

      assert Protocol.eta_delay_ms(past, now) == 0
      assert Protocol.eta_delay_ms(future, now) == 5_000
      assert Protocol.eta_delay_ms(nil, now) == 0
    end
  end

  describe "Celery.Backend.Redis result store" do
    test "stores and retrieves task states, results, and extended metadata", %{
      fake_redis: fake_redis
    } do
      ext_app =
        Celery.new(
          broker: "redis://localhost:6379/0",
          backend: "redis://localhost:6379/0",
          redis_conn: fake_redis,
          result_extended: true
        )

      tid = Protocol.uuid()
      assert RedisBackend.get_state(ext_app, tid) == States.pending()

      ctx = %Celery.Task.Context{
        id: tid,
        task: "src.tasks.add",
        args: [3, 4],
        kwargs: %{},
        hostname: "celery@test",
        retries: 0,
        delivery_info: %{"routing_key" => "celery"}
      }

      {:ok, 7} = RedisBackend.mark_as_done(ext_app, tid, 7, request: ctx)

      meta = RedisBackend.get_task_meta(ext_app, tid)
      assert meta["status"] == "SUCCESS"
      assert meta["result"] == 7
      assert meta["task_id"] == tid
      assert meta["name"] == "src.tasks.add"
      assert meta["args"] == [3, 4]
      assert meta["worker"] == "celery@test"
      assert meta["queue"] == "celery"
      assert is_binary(meta["date_done"])

      # Does not overwrite once SUCCESS
      {:ok, _} = RedisBackend.mark_as_failure(ext_app, tid, RuntimeError.exception("late fail"))
      assert RedisBackend.get_state(ext_app, tid) == "SUCCESS"
      assert RedisBackend.get_result(ext_app, tid) == 7

      # Forget deletes key
      assert :ok == RedisBackend.forget(ext_app, tid)
      assert RedisBackend.get_state(ext_app, tid) == "PENDING"
    end

    test "serializes exceptions into Python Celery format and reconstructs RemoteError", %{
      app: app
    } do
      tid = Protocol.uuid()
      exc = ArgumentError.exception("invalid input value")

      {:ok, _} =
        RedisBackend.mark_as_failure(app, tid, exc, traceback: "Traceback: invalid input")

      assert RedisBackend.get_state(app, tid) == "FAILURE"
      assert RedisBackend.get_traceback(app, tid) == "Traceback: invalid input"

      loaded_exc = RedisBackend.get_result(app, tid)

      assert %Celery.RemoteError{
               exc_type: "ValueError",
               exc_module: "builtins",
               exc_message: ["invalid input value"]
             } = loaded_exc

      assert_raise Celery.RemoteError, ~r/ValueError/, fn ->
        RedisBackend.wait_for(app, tid, timeout: 100)
      end

      assert %Celery.RemoteError{} = RedisBackend.wait_for(app, tid, propagate: false)
    end

    test "supports get_many and group save/restore/delete", %{app: app} do
      t1 = Protocol.uuid()
      t2 = Protocol.uuid()
      RedisBackend.mark_as_done(app, t1, "one")
      RedisBackend.mark_as_done(app, t2, "two")

      many = RedisBackend.get_many(app, [t1, t2])
      assert many[t1]["result"] == "one"
      assert many[t2]["result"] == "two"

      gid = Protocol.uuid()

      group_res =
        GroupResult.new(gid, [Result.new(t1, app: app), Result.new(t2, app: app)], app: app)

      assert :ok == GroupResult.save(group_res)

      restored = GroupResult.restore(gid, app: app)
      assert restored.id == gid
      assert GroupResult.get(restored, timeout: 500) == ["one", "two"]

      assert :ok == GroupResult.delete(restored)
      assert GroupResult.restore(gid, app: app) == nil
    end
  end

  describe "Celery.Worker execution, retries, failure, revocation, and Canvas" do
    test "executes retries and succeeds once condition is met", %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      {:ok, res} = Src.Tasks.delay(app, :flaky_task, [2], %{})
      assert Result.get(res, timeout: 3_000, interval: 15) == "recovered_after_2_retries"
    end

    test "propagates failure when task raises an unhandled exception", %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      {:ok, res} = Src.Tasks.delay(app, :fail_task, ["kaboom"], %{})

      assert_raise Celery.RemoteError, ~r/RuntimeError.*kaboom/, fn ->
        Result.get(res, timeout: 2_000, interval: 10)
      end

      assert Result.failed?(res)
      assert is_binary(Result.traceback(res))
    end

    test "enforces hard time_limit and marks task as TimeoutError", %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      {:ok, res} = Src.Tasks.delay(app, :slow_task, [500], %{})

      assert_raise Celery.RemoteError, ~r/TimeoutError/, fn ->
        Result.get(res, timeout: 2_000, interval: 10)
      end
    end

    test "skips revoked and expired tasks and marks them REVOKED", %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      revoked_id = Protocol.uuid()
      :ok = Celery.Worker.revoke(worker, revoked_id)

      {:ok, res1} = Celery.send_task(app, "src.tasks.add", args: [1, 2], task_id: revoked_id)

      assert_raise Celery.TaskRevokedError, fn ->
        Result.get(res1, timeout: 2_000, interval: 10)
      end

      assert Result.state(res1) == States.revoked()

      # Expired task
      past_iso = DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.to_iso8601()
      {:ok, res2} = Celery.send_task(app, "src.tasks.add", args: [1, 2], expires: past_iso)

      assert_raise Celery.TaskRevokedError, fn ->
        Result.get(res2, timeout: 2_000, interval: 10)
      end
    end

    test "executes Canvas chain, group, and chord workflows", %{app: app} do
      {:ok, worker} = Celery.Worker.start_link(app: app, poll_interval: 10)
      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      # 1. Chain: add(2, 3) -> add(5, 10) = 15
      s1 = Canvas.s("src.tasks.add", args: [2, 3], app: app)
      s2 = Canvas.s("src.tasks.add", args: [10], app: app)
      {:ok, chain_res} = Canvas.chain([s1, s2], app: app)

      assert Result.get(chain_res, timeout: 3_000, interval: 10) == 15
      assert Result.get(chain_res.parent, timeout: 1_000, interval: 10) == 5

      # 2. Group: [add(1, 2), add(3, 4), add(5, 6)] -> [3, 7, 11]
      {:ok, grp_res} =
        Canvas.group(
          [
            Canvas.s("src.tasks.add", args: [1, 2], app: app),
            Canvas.s("src.tasks.add", args: [3, 4], app: app),
            Canvas.s("src.tasks.add", args: [5, 6], app: app)
          ],
          app: app
        )

      assert GroupResult.get(grp_res, timeout: 3_000, interval: 10) == [3, 7, 11]
      assert GroupResult.successful?(grp_res)

      # 3. Chord: header [add(2, 3), add(4, 6), add(10, 5)] -> callback sum_list([5, 10, 15]) = 30
      {:ok, chord_res} =
        Canvas.chord(
          [
            Canvas.s("src.tasks.add", args: [2, 3], app: app),
            Canvas.s("src.tasks.add", args: [4, 6], app: app),
            Canvas.s("src.tasks.add", args: [10, 5], app: app)
          ],
          Canvas.s("src.tasks.sum_list", app: app),
          app: app
        )

      assert Result.get(chord_res, timeout: 3_000, interval: 10) == 30
    end

    test "responds to fanout pidbox control/inspect commands (ping, registered with metadata, active_queues, conf, stats)",
         %{app: app} do
      {:ok, worker} =
        Celery.Worker.start_link(
          app: app,
          hostname: "celery@pidbox-test",
          poll_interval: 10
        )

      on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

      # 1. Ping
      ping_replies = Celery.Worker.Control.ping(app, timeout: 1.0, limit: 1)
      assert ping_replies == [%{"celery@pidbox-test" => %{"ok" => "pong"}}]

      # 2. Registered without taskinfoitems (Flower style)
      reg_plain =
        Celery.Worker.Control.inspect_call(app, "registered", %{}, timeout: 1.0, limit: 1)

      assert [%{"celery@pidbox-test" => tasks_plain}] = reg_plain
      assert "src.tasks.command" in tasks_plain

      # 3. Registered with taskinfoitems: ["metadata"] (OpenRelik style)
      reg_meta =
        Celery.Worker.Control.inspect_call(
          app,
          "registered",
          %{"taskinfoitems" => ["metadata"]},
          timeout: 1.0,
          limit: 1
        )

      assert [%{"celery@pidbox-test" => tasks_meta}] = reg_meta

      cmd_entry =
        Enum.find(tasks_meta, &String.starts_with?(&1, "src.tasks.command [metadata="))

      assert cmd_entry ==
               "src.tasks.command [metadata={'owner': 'pipeline', 'version': '1.0'}]"

      # 4. Active queues
      aq_replies =
        Celery.Worker.Control.inspect_call(app, "active_queues", %{}, timeout: 1.0, limit: 1)

      assert [%{"celery@pidbox-test" => [%{"name" => "celery", "routing_key" => "celery"} | _]}] =
               aq_replies

      # 5. Conf & Stats
      conf_replies =
        Celery.Worker.Control.inspect_call(app, "conf", %{}, timeout: 1.0, limit: 1)

      assert [%{"celery@pidbox-test" => %{"task_default_queue" => "celery"}}] = conf_replies

      stats_replies =
        Celery.Worker.Control.inspect_call(app, "stats", %{}, timeout: 1.0, limit: 1)

      assert [%{"celery@pidbox-test" => %{"hostname" => "celery@pidbox-test", "pool" => pool}}] =
               stats_replies

      assert is_list(pool["processes"])
    end
  end
end
