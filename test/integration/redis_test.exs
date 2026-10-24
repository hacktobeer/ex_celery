defmodule Celery.Integration.RedisTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Celery.Canvas
  alias Celery.Result
  alias Celery.States
  alias Kombu.Utils.JSON

  @redis_url System.get_env("REDIS_URL", "redis://localhost:6379/11")

  setup do
    {:ok, redix} = Redix.start_link(@redis_url)
    {:ok, "OK"} = Redix.command(redix, ["FLUSHDB"])

    app =
      Celery.new(
        broker: @redis_url,
        backend: @redis_url,
        include: ["src.tasks"],
        result_extended: true
      )

    on_exit(fn ->
      if Process.alive?(redix) do
        _ = Redix.command(redix, ["FLUSHDB"])
        Redix.stop(redix)
      end
    end)

    %{app: app, redix: redix}
  end

  test "live Redis worker executes Src.Tasks.command with default and custom arguments", %{
    app: app
  } do
    {:ok, worker} =
      Celery.Worker.start_link(
        app: app,
        hostname: "celery@integration-host",
        concurrency: 2,
        poll_interval: 10
      )

    on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

    {:ok, res1} = Src.Tasks.delay(app, :command)
    assert Result.get(res1, timeout: 5_000, interval: 10) == "This is a command"
    assert Result.state(res1) == States.success()

    {:ok, res2} =
      Src.Tasks.apply_async(app, :command,
        kwargs: %{
          "pipe_result" => "prev_step",
          "input_files" => ["/data/in1.txt", "/data/in2.txt"],
          "output_path" => "/data/out",
          "workflow_id" => "wf-live-1",
          "task_config" => %{"mode" => "fast"}
        }
      )

    assert Result.get(res2, timeout: 5_000, interval: 10) == "This is a command"

    meta = Celery.Backend.Redis.get_task_meta(app, res2.id)
    assert meta["status"] == "SUCCESS"
    assert meta["result"] == "This is a command"
    assert meta["name"] == "src.tasks.command"
    assert meta["worker"] == "celery@integration-host"
    assert meta["kwargs"]["workflow_id"] == "wf-live-1"
  end

  test "live Redis worker executes chain and chord workflows and stores standard celery-task-meta keys",
       %{
         app: app,
         redix: redix
       } do
    {:ok, worker} =
      Celery.Worker.start_link(
        app: app,
        hostname: "celery@integration-chord",
        concurrency: 4,
        poll_interval: 10
      )

    on_exit(fn -> if Process.alive?(worker), do: Celery.Worker.stop(worker) end)

    # Chain: 5 + 5 = 10 -> 10 + 20 = 30
    {:ok, chain_res} =
      Canvas.chain(
        [
          Canvas.s("src.tasks.add", args: [5, 5], app: app),
          Canvas.s("src.tasks.add", args: [20], app: app)
        ],
        app: app
      )

    assert Result.get(chain_res, timeout: 5_000, interval: 10) == 30

    # Chord: [add(1, 2), add(3, 4), add(5, 6)] -> sum_list([3, 7, 11]) = 21
    {:ok, chord_res} =
      Canvas.chord(
        [
          Canvas.s("src.tasks.add", args: [1, 2], app: app),
          Canvas.s("src.tasks.add", args: [3, 4], app: app),
          Canvas.s("src.tasks.add", args: [5, 6], app: app)
        ],
        Canvas.s("src.tasks.sum_list", app: app),
        app: app
      )

    assert Result.get(chord_res, timeout: 5_000, interval: 10) == 21

    # Verify Redis keys use standard `celery-task-meta-<id>` format
    {:ok, raw_json} = Redix.command(redix, ["GET", "celery-task-meta-#{chord_res.id}"])
    assert is_binary(raw_json)
    decoded = JSON.loads(raw_json)
    assert decoded["status"] == "SUCCESS"
    assert decoded["result"] == 21
    assert decoded["task_id"] == chord_res.id
  end
end
