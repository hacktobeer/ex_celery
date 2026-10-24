defmodule Celery.Task.Context do
  @moduledoc """
  Task execution request context (`self.request` in Python Celery, ported from `celery.app.task.Context`).

  When a task is defined with `bind: true`, the first argument passed to the task
  function is a `%Celery.Task{}` struct whose `:request` field is a `%Celery.Task.Context{}`.
  """

  @type t :: %__MODULE__{
          id: String.t() | nil,
          task: String.t() | nil,
          args: list(),
          kwargs: map(),
          retries: non_neg_integer(),
          is_eager: boolean(),
          logfile: String.t() | nil,
          loglevel: atom() | non_neg_integer() | nil,
          hostname: String.t() | nil,
          callbacks: list() | nil,
          errbacks: list() | nil,
          chain: list() | nil,
          chord: map() | nil,
          eta: String.t() | DateTime.t() | nil,
          expires: String.t() | DateTime.t() | number() | nil,
          group: String.t() | nil,
          group_index: non_neg_integer() | nil,
          timelimit: [number() | nil] | {number() | nil, number() | nil},
          root_id: String.t() | nil,
          parent_id: String.t() | nil,
          argsrepr: String.t() | nil,
          kwargsrepr: String.t() | nil,
          origin: String.t() | nil,
          ignore_result: boolean(),
          shadow: String.t() | nil,
          replaced_task_nesting: non_neg_integer(),
          stamped_headers: list() | nil,
          stamps: map() | nil,
          reply_to: String.t() | nil,
          correlation_id: String.t() | nil,
          delivery_info: map(),
          headers: map(),
          properties: map(),
          called_directly: boolean()
        }

  defstruct id: nil,
            task: nil,
            args: [],
            kwargs: %{},
            retries: 0,
            is_eager: false,
            logfile: nil,
            loglevel: nil,
            hostname: nil,
            callbacks: nil,
            errbacks: nil,
            chain: nil,
            chord: nil,
            eta: nil,
            expires: nil,
            group: nil,
            group_index: nil,
            timelimit: [nil, nil],
            root_id: nil,
            parent_id: nil,
            argsrepr: nil,
            kwargsrepr: nil,
            origin: nil,
            ignore_result: false,
            shadow: nil,
            replaced_task_nesting: 0,
            stamped_headers: nil,
            stamps: %{},
            reply_to: nil,
            correlation_id: nil,
            delivery_info: %{},
            headers: %{},
            properties: %{},
            called_directly: true

  @doc """
  Creates a new `%Celery.Task.Context{}` from a map or keyword list.
  """
  @spec new(map() | keyword()) :: t()
  def new(attrs \\ %{}) do
    map =
      attrs
      |> Enum.into(%{})
      |> Enum.reduce(%{}, fn
        {k, v}, acc when is_atom(k) ->
          Map.put(acc, k, v)

        {k, v}, acc when is_binary(k) ->
          case string_key_to_field(k) do
            nil -> acc
            atom_key -> Map.put_new(acc, atom_key, v)
          end
      end)

    struct(__MODULE__, Map.to_list(map))
  end

  defp string_key_to_field("id"), do: :id
  defp string_key_to_field("task"), do: :task
  defp string_key_to_field("args"), do: :args
  defp string_key_to_field("kwargs"), do: :kwargs
  defp string_key_to_field("retries"), do: :retries
  defp string_key_to_field("is_eager"), do: :is_eager
  defp string_key_to_field("logfile"), do: :logfile
  defp string_key_to_field("loglevel"), do: :loglevel
  defp string_key_to_field("hostname"), do: :hostname
  defp string_key_to_field("callbacks"), do: :callbacks
  defp string_key_to_field("errbacks"), do: :errbacks
  defp string_key_to_field("chain"), do: :chain
  defp string_key_to_field("chord"), do: :chord
  defp string_key_to_field("eta"), do: :eta
  defp string_key_to_field("expires"), do: :expires
  defp string_key_to_field("group"), do: :group
  defp string_key_to_field("taskset"), do: :group
  defp string_key_to_field("group_index"), do: :group_index
  defp string_key_to_field("timelimit"), do: :timelimit
  defp string_key_to_field("root_id"), do: :root_id
  defp string_key_to_field("parent_id"), do: :parent_id
  defp string_key_to_field("argsrepr"), do: :argsrepr
  defp string_key_to_field("kwargsrepr"), do: :kwargsrepr
  defp string_key_to_field("origin"), do: :origin
  defp string_key_to_field("ignore_result"), do: :ignore_result
  defp string_key_to_field("shadow"), do: :shadow
  defp string_key_to_field("replaced_task_nesting"), do: :replaced_task_nesting
  defp string_key_to_field("stamped_headers"), do: :stamped_headers
  defp string_key_to_field("stamps"), do: :stamps
  defp string_key_to_field("reply_to"), do: :reply_to
  defp string_key_to_field("correlation_id"), do: :correlation_id
  defp string_key_to_field("delivery_info"), do: :delivery_info
  defp string_key_to_field("headers"), do: :headers
  defp string_key_to_field("properties"), do: :properties
  defp string_key_to_field("called_directly"), do: :called_directly
  defp string_key_to_field(_), do: nil
end
