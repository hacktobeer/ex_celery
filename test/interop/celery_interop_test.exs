defmodule Celery.Interop.CeleryInteropTest do
  use ExUnit.Case, async: false

  @moduletag :interop

  alias Kombu.Utils.JSON

  @redis_url System.get_env("REDIS_URL", "redis://localhost:6379/12")
  @uv_bin System.find_executable("uv") || "/Users/ramsesdebeer/.local/bin/uv"
  @celery_pythonpath Path.expand("../../python-original-celery/celery", __DIR__)

  setup do
    {:ok, redix} = Redix.start_link(@redis_url)
    {:ok, "OK"} = Redix.command(redix, ["FLUSHDB"])

    app =
      Celery.new(
        broker: @redis_url,
        backend: @redis_url,
        include: ["src.tasks"]
      )

    {:ok, worker} =
      Celery.Worker.start_link(
        app: app,
        hostname: "celery@elixir-interop-worker",
        concurrency: 4,
        poll_interval: 10
      )

    on_exit(fn ->
      if Process.alive?(worker), do: Celery.Worker.stop(worker)

      if Process.alive?(redix) do
        _ = Redix.command(redix, ["FLUSHDB"])
        Redix.stop(redix)
      end
    end)

    %{app: app, worker: worker, redix: redix}
  end

  defp run_python!(script) do
    args = [
      "run",
      "--quiet",
      "--with",
      "redis>=4.5",
      "--with",
      "kombu>=5.3",
      "--with",
      "billiard>=4.2",
      "--with",
      "vine>=5.1",
      "--with",
      "click>=8.1",
      "--with",
      "click-didyoumean>=0.3",
      "--with",
      "click-repl>=0.2",
      "--with",
      "click-plugins>=1.1",
      "--with",
      "python-dateutil>=2.8",
      "--with",
      "tzlocal",
      "python3",
      "-c",
      script
    ]

    {output, status} =
      System.cmd(@uv_bin, args,
        env: [
          {"PYTHONPATH", @celery_pythonpath},
          {"REDIS_URL", @redis_url}
        ],
        stderr_to_stdout: true
      )

    if status != 0 do
      flunk("Python Celery script failed (exit #{status}):\n#{output}")
    end

    String.trim(output)
  end

  test "Python Celery dispatches src.tasks.command and receives result from Elixir Celery.Worker" do
    py_script = """
    import json, os
    from celery import Celery

    REDIS_URL = os.environ["REDIS_URL"]
    celery = Celery(broker=REDIS_URL, backend=REDIS_URL)

    # 1. Default invocation matching the user's target task signature
    r1 = celery.send_task("src.tasks.command")
    val1 = r1.get(timeout=5)

    # 2. Invocation with positional + keyword arguments
    r2 = celery.send_task(
        "src.tasks.command",
        args=["upstream_pipe_data"],
        kwargs={
            "input_files": ["in1.parquet", "in2.parquet"],
            "output_path": "/s3/bucket/out",
            "workflow_id": "wf-py-elixir-001",
            "task_config": {"echo": True, "threads": 4},
        },
    )
    val2 = r2.get(timeout=5)

    print(json.dumps({
        "val1": val1,
        "state1": r1.state,
        "val2": val2,
        "state2": r2.state,
        "id2": r2.id,
    }))
    """

    out = run_python!(py_script)
    data = JSON.loads(out)

    assert data["val1"] == "This is a command"
    assert data["state1"] == "SUCCESS"

    assert data["state2"] == "SUCCESS"
    assert data["val2"]["message"] == "This is a command"
    assert data["val2"]["task_id"] == data["id2"]
    assert data["val2"]["task_name"] == "src.tasks.command"
    assert data["val2"]["metadata"] == %{"owner" => "pipeline", "version" => "1.0"}
    assert data["val2"]["pipe_result"] == "upstream_pipe_data"
    assert data["val2"]["input_files"] == ["in1.parquet", "in2.parquet"]
    assert data["val2"]["output_path"] == "/s3/bucket/out"
    assert data["val2"]["workflow_id"] == "wf-py-elixir-001"
    assert data["val2"]["task_config"] == %{"echo" => true, "threads" => 4}
  end

  test "Python Celery Canvas (chain & chord) and exception propagation with Elixir Celery.Worker" do
    py_script = """
    import json, os
    from celery import Celery, chain, chord, signature

    REDIS_URL = os.environ["REDIS_URL"]
    celery = Celery(broker=REDIS_URL, backend=REDIS_URL)

    # 1. Chain: add(10, 20) -> add(5) == 35
    c_res = (
        signature("src.tasks.add", args=(10, 20), app=celery)
        | signature("src.tasks.add", args=(5,), app=celery)
    ).apply_async()
    chain_val = c_res.get(timeout=5)

    # 2. Chord: [add(1, 2), add(3, 4), add(5, 6)] -> sum_list([3, 7, 11]) == 21
    ch_res = chord(
        [
            signature("src.tasks.add", args=(1, 2), app=celery),
            signature("src.tasks.add", args=(3, 4), app=celery),
            signature("src.tasks.add", args=(5, 6), app=celery),
        ],
        signature("src.tasks.sum_list", app=celery),
    ).apply_async()
    chord_val = ch_res.get(timeout=5)

    # 3. Exception propagation from Elixir worker to Python AsyncResult.get()
    err_res = celery.send_task("src.tasks.fail_task", args=["interop_boom"])
    caught_exc = None
    try:
        err_res.get(timeout=5)
    except Exception as exc:
        caught_exc = {"type": type(exc).__name__, "msg": str(exc), "state": err_res.state}

    print(json.dumps({
        "chain_val": chain_val,
        "chord_val": chord_val,
        "caught_exc": caught_exc,
    }))
    """

    out = run_python!(py_script)
    data = JSON.loads(out)

    assert data["chain_val"] == 35
    assert data["chord_val"] == 21
    assert data["caught_exc"]["state"] == "FAILURE"
    assert String.contains?(data["caught_exc"]["msg"], "interop_boom")
  end
end
