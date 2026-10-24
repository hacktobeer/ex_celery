defmodule Celery.Worker.Request do
  @moduledoc """
  Worker task request wrapper (ported from `celery.worker.request.Request`).

  Wraps a decoded `%Celery.Task.Context{}` and its underlying `%Kombu.Message{}`
  to manage message acknowledgement (`acks_late`), rejection, expiration, and
  inspection metadata.
  """

  alias Celery.Protocol
  alias Celery.Task.Context

  @type t :: %__MODULE__{
          id: String.t(),
          name: String.t(),
          context: Context.t(),
          message: Kombu.Message.t() | nil,
          hostname: String.t(),
          time_start: float() | nil,
          worker_pid: pid() | nil,
          acknowledged: boolean()
        }

  defstruct id: nil,
            name: nil,
            context: %Context{},
            message: nil,
            hostname: nil,
            time_start: nil,
            worker_pid: nil,
            acknowledged: false

  @doc """
  Creates a `%Celery.Worker.Request{}` from a `%Kombu.Message{}` and worker `hostname`.
  """
  @spec from_message(Kombu.Message.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def from_message(%Kombu.Message{} = message, hostname) do
    case Protocol.decode_message(message) do
      {:ok, %Context{} = ctx} ->
        ctx = %{ctx | hostname: hostname}

        req = %__MODULE__{
          id: ctx.id,
          name: ctx.task,
          context: ctx,
          message: message,
          hostname: hostname,
          acknowledged: Kombu.Message.acknowledged?(message)
        }

        {:ok, req}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Acknowledges the underlying `%Kombu.Message{}` if not already acknowledged.
  """
  @spec acknowledge(t()) :: {t(), :ok | {:error, term()}}
  def acknowledge(%__MODULE__{acknowledged: true} = req), do: {req, :ok}
  def acknowledge(%__MODULE__{message: nil} = req), do: {%{req | acknowledged: true}, :ok}

  def acknowledge(%__MODULE__{message: msg} = req) do
    try do
      acked_msg = Kombu.Message.ack(msg)
      {%{req | message: acked_msg, acknowledged: true}, :ok}
    rescue
      e ->
        {req, {:error, e}}
    end
  end

  @doc """
  Rejects the underlying `%Kombu.Message{}` (with `requeue: boolean()`).
  """
  @spec reject(t(), keyword()) :: {t(), :ok | {:error, term()}}
  def reject(%__MODULE__{acknowledged: true} = req, _opts), do: {req, :ok}
  def reject(%__MODULE__{message: nil} = req, _opts), do: {%{req | acknowledged: true}, :ok}

  def reject(%__MODULE__{message: msg} = req, opts) do
    try do
      rej_msg = Kombu.Message.reject(msg, opts)
      {%{req | message: rej_msg, acknowledged: true}, :ok}
    rescue
      e ->
        {req, {:error, e}}
    end
  end

  @doc """
  Returns an inspection dictionary matching Python Celery's `Request.info()`.
  """
  @spec info(t()) :: map()
  def info(%__MODULE__{context: ctx} = req) do
    %{
      "id" => req.id,
      "name" => req.name,
      "args" => ctx.args,
      "kwargs" => ctx.kwargs,
      "type" => req.name,
      "hostname" => req.hostname,
      "time_start" => req.time_start,
      "acknowledged" => req.acknowledged,
      "delivery_info" => ctx.delivery_info,
      "worker_pid" => if(req.worker_pid, do: inspect(req.worker_pid), else: nil)
    }
  end
end
