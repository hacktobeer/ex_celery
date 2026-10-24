defmodule Celery.States do
  @moduledoc """
  Built-in Celery task states and precedence rules (ported from `celery.states`).
  """

  @pending "PENDING"
  @received "RECEIVED"
  @started "STARTED"
  @success "SUCCESS"
  @failure "FAILURE"
  @revoked "REVOKED"
  @retry "RETRY"
  @rejected "REJECTED"
  @ignored "IGNORED"

  @precedence [
    @success,
    @failure,
    @revoked,
    @started,
    @received,
    @retry,
    @pending,
    @ignored,
    @rejected,
    nil
  ]

  @precedence_lookup @precedence
                     |> Enum.with_index()
                     |> Map.new()

  @ready_states MapSet.new([@success, @failure, @revoked])
  @unready_states MapSet.new([@pending, @received, @started, @retry, @rejected, @ignored])
  @exception_states MapSet.new([@retry, @failure, @revoked])
  @propagate_states MapSet.new([@failure, @revoked])
  @all_states MapSet.new([
                @pending,
                @received,
                @started,
                @success,
                @failure,
                @revoked,
                @retry,
                @rejected,
                @ignored
              ])

  @doc "Task state is unknown (assumed pending since you know the id)."
  def pending, do: @pending

  @doc "Task was received by a worker (only used in events)."
  def received, do: @received

  @doc "Task was started by a worker (`task_track_started` enabled)."
  def started, do: @started

  @doc "Task succeeded."
  def success, do: @success

  @doc "Task failed."
  def failure, do: @failure

  @doc "Task was revoked."
  def revoked, do: @revoked

  @doc "Task is waiting for retry."
  def retry, do: @retry

  @doc "Task was rejected."
  def rejected, do: @rejected

  @doc "Task was ignored."
  def ignored, do: @ignored

  @doc "Set of states meaning the task result is ready (`SUCCESS`, `FAILURE`, `REVOKED`)."
  def ready_states, do: @ready_states

  @doc "Set of states meaning the task result is not yet ready."
  def unready_states, do: @unready_states

  @doc "Set of states meaning the task raised an exception (`RETRY`, `FAILURE`, `REVOKED`)."
  def exception_states, do: @exception_states

  @doc "Set of states meaning the exception should propagate to the caller (`FAILURE`, `REVOKED`)."
  def propagate_states, do: @propagate_states

  @doc "Set of all built-in task states."
  def all_states, do: @all_states

  @doc "Returns `true` if `state` is in `ready_states()`."
  @spec ready?(String.t() | nil) :: boolean()
  def ready?(state), do: MapSet.member?(@ready_states, state)

  @doc "Returns `true` if `state` is in `unready_states()`."
  @spec unready?(String.t() | nil) :: boolean()
  def unready?(state), do: MapSet.member?(@unready_states, state)

  @doc "Returns `true` if `state` is in `exception_states()`."
  @spec exception?(String.t() | nil) :: boolean()
  def exception?(state), do: MapSet.member?(@exception_states, state)

  @doc "Returns `true` if `state` is in `propagate_states()`."
  @spec propagate?(String.t() | nil) :: boolean()
  def propagate?(state), do: MapSet.member?(@propagate_states, state)

  @doc """
  Returns the precedence index for a task state.
  Lower index means higher precedence.
  """
  @spec precedence(String.t() | nil) :: non_neg_integer()
  def precedence(state) do
    Map.get(@precedence_lookup, state, Map.fetch!(@precedence_lookup, nil))
  end

  @doc """
  Returns `true` if `state1` can overwrite `state2` according to Celery state precedence.
  """
  @spec can_overwrite?(String.t() | nil, String.t() | nil) :: boolean()
  def can_overwrite?(state1, state2) do
    precedence(state1) <= precedence(state2)
  end
end
