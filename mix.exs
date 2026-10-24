defmodule Celery.MixProject do
  use Mix.Project

  @version "0.1.2"
  @source_url "https://github.com/hacktobeer/celery-elixir"

  def project do
    [
      app: :celery,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Celery.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:kombu, "~> 0.1.2"},
      {:redix, "~> 1.5"},
      {:decimal, "~> 2.1"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    Elixir port of Python's Celery distributed task queue — wire-compatible with
    Python Celery (Protocol v1 & v2) and backed by Kombu and Redis for transport
    and result storage.
    """
  end

  defp package do
    [
      name: "celery",
      maintainers: ["hacktobeer"],
      licenses: ["BSD-3-Clause"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md", "LICENSE"],
      groups_for_modules: [
        Core: [
          Celery,
          Celery.Config,
          Celery.States,
          Celery.Registry
        ],
        "Tasks & Canvas": [
          Celery.Task,
          Celery.Task.Context,
          Celery.Canvas,
          Celery.Canvas.Signature
        ],
        "Worker & Protocol": [
          Celery.Worker,
          Celery.Worker.Request,
          Celery.Worker.Control,
          Celery.Events,
          Celery.Tracer,
          Celery.Protocol,
          Celery.AMQP
        ],
        "Result Store": [
          Celery.Result,
          Celery.Result.GroupResult,
          Celery.Backend.Redis
        ],
        Exceptions: [
          Celery.Error,
          Celery.Retry,
          Celery.Ignore,
          Celery.Reject,
          Celery.MaxRetriesExceededError,
          Celery.TaskRevokedError,
          Celery.NotRegistered,
          Celery.InvalidTaskError,
          Celery.TimeoutError,
          Celery.BackendStoreError,
          Celery.ChordError
        ]
      ]
    ]
  end
end
