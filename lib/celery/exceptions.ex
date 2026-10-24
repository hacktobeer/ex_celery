defmodule Celery.Error do
  @moduledoc "Base Celery error."
  defexception [:message, :reason]
  @type t :: %__MODULE__{}

  @impl true
  def exception(msg) when is_binary(msg), do: %__MODULE__{message: msg}

  def exception(opts) when is_list(opts) do
    msg = Keyword.get(opts, :message, "Celery error")
    %__MODULE__{message: msg, reason: Keyword.get(opts, :reason)}
  end
end

defmodule Celery.Retry do
  @moduledoc """
  Exception raised by `Celery.Task.retry/2` to signal that the task is to be retried later.
  """
  defexception [:message, :exc, :when, :is_eager, :sig, :args, :kwargs, :options]
  @type t :: %__MODULE__{}

  @impl true
  def exception(opts) when is_list(opts) do
    exc = Keyword.get(opts, :exc)
    when_val = Keyword.get(opts, :when)

    msg =
      Keyword.get_lazy(opts, :message, fn ->
        cond do
          is_number(when_val) -> "Retry in #{when_val}s: #{inspect(exc)}"
          not is_nil(when_val) -> "Retry at #{inspect(when_val)}: #{inspect(exc)}"
          true -> "Retry: #{inspect(exc)}"
        end
      end)

    %__MODULE__{
      message: msg,
      exc: exc,
      when: when_val,
      is_eager: Keyword.get(opts, :is_eager, false),
      sig: Keyword.get(opts, :sig),
      args: Keyword.get(opts, :args),
      kwargs: Keyword.get(opts, :kwargs),
      options: Keyword.get(opts, :options, [])
    }
  end
end

defmodule Celery.Ignore do
  @moduledoc """
  Exception that a task may raise to ignore the task without recording a result or failure.
  """
  defexception message: "Task ignored"
  @type t :: %__MODULE__{}
end

defmodule Celery.Reject do
  @moduledoc """
  Exception that a task may raise to reject the message via AMQP/Kombu `reject` (`requeue: true | false`).
  """
  defexception [:message, :reason, requeue: false]
  @type t :: %__MODULE__{}

  @impl true
  def exception(opts) when is_list(opts) do
    reason = Keyword.get(opts, :reason, "Task rejected")
    requeue = Keyword.get(opts, :requeue, false)
    %__MODULE__{message: "#{reason} (requeue=#{requeue})", reason: reason, requeue: requeue}
  end

  def exception(msg) when is_binary(msg) do
    %__MODULE__{message: msg, reason: msg, requeue: false}
  end
end

defmodule Celery.MaxRetriesExceededError do
  @moduledoc "The task's max_retries limit was exceeded."
  defexception [:message, :task_name, :task_id, :args, :kwargs]
  @type t :: %__MODULE__{}

  @impl true
  def exception(opts) when is_list(opts) do
    task_name = Keyword.get(opts, :task_name)
    task_id = Keyword.get(opts, :task_id)

    msg =
      Keyword.get(
        opts,
        :message,
        "Can't retry #{task_name}[#{task_id}] args:#{inspect(opts[:args])} kwargs:#{inspect(opts[:kwargs])}"
      )

    %__MODULE__{
      message: msg,
      task_name: task_name,
      task_id: task_id,
      args: Keyword.get(opts, :args),
      kwargs: Keyword.get(opts, :kwargs)
    }
  end
end

defmodule Celery.TaskRevokedError do
  @moduledoc "The task has been revoked."
  defexception [:message, :reason]
  @type t :: %__MODULE__{}

  @impl true
  def exception(msg) when is_binary(msg) do
    %__MODULE__{message: "Task revoked: #{msg}", reason: msg}
  end

  def exception(opts) when is_list(opts) do
    reason = Keyword.get(opts, :reason, "revoked")
    %__MODULE__{message: Keyword.get(opts, :message, "Task revoked (#{reason})"), reason: reason}
  end
end

defmodule Celery.NotRegistered do
  @moduledoc "The task is not registered in the Celery registry."
  defexception [:message, :task_name]
  @type t :: %__MODULE__{}

  @impl true
  def exception(task_name) when is_binary(task_name) do
    %__MODULE__{
      message: "Task of kind #{inspect(task_name)} never registered",
      task_name: task_name
    }
  end

  def exception(opts) when is_list(opts) do
    task_name = Keyword.get(opts, :task_name)

    %__MODULE__{
      message: Keyword.get(opts, :message, "Task of kind #{inspect(task_name)} never registered"),
      task_name: task_name
    }
  end
end

defmodule Celery.InvalidTaskError do
  @moduledoc "The task has invalid arguments or options."
  defexception [:message]
end

defmodule Celery.TimeoutError do
  @moduledoc "The operation timed out waiting for a result or task execution."
  defexception message: "The operation timed out."
end

defmodule Celery.BackendStoreError do
  @moduledoc "An issue writing or reading from the result backend."
  defexception [:message, :state, :task_id, :reason]

  @impl true
  def exception(opts) when is_list(opts) do
    msg =
      Keyword.get(
        opts,
        :message,
        "Backend store error (task_id=#{opts[:task_id]}, state=#{opts[:state]}): #{inspect(opts[:reason])}"
      )

    %__MODULE__{
      message: msg,
      state: Keyword.get(opts, :state),
      task_id: Keyword.get(opts, :task_id),
      reason: Keyword.get(opts, :reason)
    }
  end
end

defmodule Celery.ChordError do
  @moduledoc "A task part of the chord raised an exception."
  defexception [:message]
end

defmodule Celery.RemoteError do
  @moduledoc """
  Exception reconstructed from a remote task failure stored in the Redis result backend.
  """
  defexception [:message, :exc_type, :exc_message, :exc_module, :traceback]

  @impl true
  def exception(opts) when is_list(opts) do
    exc_type = Keyword.get(opts, :exc_type, "Exception")
    exc_message = Keyword.get(opts, :exc_message, "")
    exc_module = Keyword.get(opts, :exc_module, "builtins")
    traceback = Keyword.get(opts, :traceback)

    formatted_msg =
      case exc_message do
        list when is_list(list) -> Enum.map_join(list, ", ", &to_string/1)
        bin when is_binary(bin) -> bin
        other -> inspect(other)
      end

    %__MODULE__{
      message: "#{exc_type}: #{formatted_msg}",
      exc_type: exc_type,
      exc_message: exc_message,
      exc_module: exc_module,
      traceback: traceback
    }
  end
end
