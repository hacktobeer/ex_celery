defmodule Celery.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Celery.AppRegistry},
      Celery.Registry,
      {DynamicSupervisor, strategy: :one_for_one, name: Celery.DynamicSupervisor}
    ]

    opts = [strategy: :one_for_one, name: Celery.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
