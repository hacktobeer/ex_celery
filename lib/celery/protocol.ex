defmodule Celery.Protocol do
  @moduledoc """
  Celery Task Message Protocol v1 and v2 encoder and decoder (ported from `celery.app.amqp`
  and `celery.worker.request`).

  ### Protocol v2 (Default in Celery >= 4.0)
  - `headers`:
    ```elixir
    %{
      "lang" => "py",
      "task" => name,
      "id" => task_id,
      "shadow" => shadow,
      "eta" => eta,
      "expires" => expires,
      "group" => group_id,
      "group_index" => group_index,
      "retries" => retries,
      "timelimit" => [time_limit, soft_time_limit],
      "root_id" => root_id,
      "parent_id" => parent_id,
      "argsrepr" => argsrepr,
      "kwargsrepr" => kwargsrepr,
      "origin" => origin,
      "ignore_result" => ignore_result,
      "replaced_task_nesting" => 0,
      "stamped_headers" => stamped_headers,
      "stamps" => stamps
    }
    ```
  - `properties`:
    `%{"correlation_id" => task_id, "reply_to" => reply_to || ""}`
  - `body`:
    `[args, kwargs, %{"callbacks" => callbacks, "errbacks" => errbacks, "chain" => chain, "chord" => chord}]`

  ### Protocol v1
  - `headers`: `%{}`
  - `properties`: `%{"correlation_id" => task_id, "reply_to" => reply_to || ""}`
  - `body`:
    ```elixir
    %{
      "task" => name,
      "id" => task_id,
      "args" => args,
      "kwargs" => kwargs,
      "retries" => retries,
      "eta" => eta,
      "expires" => expires,
      "utc" => true,
      "callbacks" => callbacks,
      "errbacks" => errbacks,
      "timelimit" => [time_limit, soft_time_limit],
      "taskset" => group_id,
      "chord" => chord
    }
    ```
  """

  alias Celery.Task.Context

  @type task_message :: %{
          headers: map(),
          properties: map(),
          body: term(),
          sent_event: map() | nil
        }

  @doc """
  Generates a UUIDv4 task ID compatible with Python Celery.
  """
  @spec uuid() :: String.t()
  def uuid do
    Kombu.Utils.UUID.uuid4()
  end

  @doc """
  Returns the current node origin string (e.g. `"pid@hostname"`), matching Python Celery's
  `anon@hostname` / `gen<pid>@hostname` convention.
  """
  @spec default_origin() :: String.t()
  def default_origin do
    {:ok, host} = :inet.gethostname()
    "elixir@#{List.to_string(host)}"
  end

  @doc """
  Creates a Celery Protocol v2 task message structure (`%{headers: ..., properties: ..., body: ...}`).
  """
  @spec as_task_v2(String.t(), list(), map(), keyword()) :: task_message()
  def as_task_v2(name, args \\ [], kwargs \\ %{}, opts \\ []) when is_binary(name) do
    norm_args = normalize_args(args)
    norm_kwargs = normalize_kwargs(kwargs)

    task_id = Keyword.get_lazy(opts, :task_id, &uuid/0)
    retries = Keyword.get(opts, :retries, 0)
    eta = format_time_option(Keyword.get(opts, :eta), Keyword.get(opts, :countdown))
    expires = format_time_option(Keyword.get(opts, :expires), nil)
    time_limit = Keyword.get(opts, :time_limit)
    soft_time_limit = Keyword.get(opts, :soft_time_limit)
    group_id = Keyword.get(opts, :group_id)
    group_index = Keyword.get(opts, :group_index)
    root_id = Keyword.get(opts, :root_id) || task_id
    parent_id = Keyword.get(opts, :parent_id)
    shadow = Keyword.get(opts, :shadow)
    ignore_result = Keyword.get(opts, :ignore_result, false)
    replaced_task_nesting = Keyword.get(opts, :replaced_task_nesting, 0)
    origin = Keyword.get_lazy(opts, :origin, &default_origin/0)
    reply_to = Keyword.get(opts, :reply_to, "") || ""
    lang = Keyword.get(opts, :lang, "py")

    argsrepr = Keyword.get_lazy(opts, :argsrepr, fn -> format_argsrepr(norm_args) end)
    kwargsrepr = Keyword.get_lazy(opts, :kwargsrepr, fn -> format_kwargsrepr(norm_kwargs) end)

    callbacks = normalize_signatures(Keyword.get(opts, :callbacks) || Keyword.get(opts, :link))

    errbacks =
      normalize_signatures(Keyword.get(opts, :errbacks) || Keyword.get(opts, :link_error))

    chain = normalize_signatures(Keyword.get(opts, :chain))
    chord = normalize_signature(Keyword.get(opts, :chord))

    stamps = Keyword.get(opts, :stamps) || %{}
    stamped_headers = Keyword.get(opts, :stamped_headers)
    custom_headers = Keyword.get(opts, :headers) || %{}

    embed = %{
      "callbacks" => callbacks,
      "errbacks" => errbacks,
      "chain" => chain,
      "chord" => chord
    }

    base_headers = %{
      "lang" => lang,
      "task" => name,
      "id" => task_id,
      "shadow" => shadow,
      "eta" => eta,
      "expires" => expires,
      "group" => group_id,
      "group_index" => group_index,
      "retries" => retries,
      "timelimit" => [time_limit, soft_time_limit],
      "root_id" => root_id,
      "parent_id" => parent_id,
      "argsrepr" => argsrepr,
      "kwargsrepr" => kwargsrepr,
      "origin" => origin,
      "ignore_result" => ignore_result,
      "replaced_task_nesting" => replaced_task_nesting,
      "stamped_headers" => stamped_headers,
      "stamps" => stamps
    }

    headers =
      custom_headers
      |> stringify_keys()
      |> Map.merge(base_headers)

    properties = %{
      "correlation_id" => task_id,
      "reply_to" => reply_to
    }

    %{
      headers: headers,
      properties: properties,
      body: [norm_args, norm_kwargs, embed],
      sent_event: nil
    }
  end

  @doc """
  Creates a Celery Protocol v1 task message structure (`%{headers: ..., properties: ..., body: ...}`).
  """
  @spec as_task_v1(String.t(), list(), map(), keyword()) :: task_message()
  def as_task_v1(name, args \\ [], kwargs \\ %{}, opts \\ []) when is_binary(name) do
    norm_args = normalize_args(args)
    norm_kwargs = normalize_kwargs(kwargs)

    task_id = Keyword.get_lazy(opts, :task_id, &uuid/0)
    retries = Keyword.get(opts, :retries, 0)
    eta = format_time_option(Keyword.get(opts, :eta), Keyword.get(opts, :countdown))
    expires = format_time_option(Keyword.get(opts, :expires), nil)
    time_limit = Keyword.get(opts, :time_limit)
    soft_time_limit = Keyword.get(opts, :soft_time_limit)
    group_id = Keyword.get(opts, :group_id)
    callbacks = normalize_signatures(Keyword.get(opts, :callbacks) || Keyword.get(opts, :link))

    errbacks =
      normalize_signatures(Keyword.get(opts, :errbacks) || Keyword.get(opts, :link_error))

    chord = normalize_signature(Keyword.get(opts, :chord))
    reply_to = Keyword.get(opts, :reply_to, "") || ""
    custom_headers = stringify_keys(Keyword.get(opts, :headers) || %{})

    body = %{
      "task" => name,
      "id" => task_id,
      "args" => norm_args,
      "kwargs" => norm_kwargs,
      "retries" => retries,
      "eta" => eta,
      "expires" => expires,
      "utc" => true,
      "callbacks" => callbacks,
      "errbacks" => errbacks,
      "timelimit" => [time_limit, soft_time_limit],
      "taskset" => group_id,
      "chord" => chord
    }

    properties = %{
      "correlation_id" => task_id,
      "reply_to" => reply_to
    }

    %{
      headers: custom_headers,
      properties: properties,
      body: body,
      sent_event: nil
    }
  end

  @doc """
  Decodes an incoming `%Kombu.Message{}` (or decoded payload/headers/properties map)
  into `{:ok, %Celery.Task.Context{}}` or `{:error, reason}`.

  Supports:
  - Protocol v2 (`headers["task"]` + `body = [args, kwargs, embed]`)
  - Protocol v1 (`body = %{"task" => ..., "id" => ..., "args" => ..., "kwargs" => ...}`)
  - Hybrid messages (`headers["task"]` + map body or v1 headers merged with body)
  """
  @spec decode_message(Kombu.Message.t() | map()) ::
          {:ok, Context.t()} | {:error, term()}
  def decode_message(%Kombu.Message{} = message) do
    try do
      decoded_body = Kombu.Message.decode(message)

      decode_parts(
        decoded_body,
        stringify_keys(message.headers || %{}),
        stringify_keys(message.properties || %{}),
        stringify_keys(message.delivery_info || %{})
      )
    rescue
      e ->
        {:error, {:decode_error, e}}
    end
  end

  def decode_message(%{} = raw) do
    body = Map.get(raw, :body) || Map.get(raw, "body")
    headers = stringify_keys(Map.get(raw, :headers) || Map.get(raw, "headers") || %{})
    properties = stringify_keys(Map.get(raw, :properties) || Map.get(raw, "properties") || %{})

    delivery_info =
      stringify_keys(Map.get(raw, :delivery_info) || Map.get(raw, "delivery_info") || %{})

    decode_parts(body, headers, properties, delivery_info)
  end

  @doc """
  Decodes decoded `body`, `headers`, `properties`, and `delivery_info` into `{:ok, %Celery.Task.Context{}}`.
  """
  @spec decode_parts(term(), map(), map(), map()) :: {:ok, Context.t()} | {:error, term()}
  def decode_parts(body, headers, properties, delivery_info \\ %{})

  # Protocol v2: body is [args, kwargs, embed] or [args, kwargs]
  def decode_parts([args, kwargs | rest], headers, properties, delivery_info)
      when is_list(args) and is_map(kwargs) do
    embed =
      case rest do
        [embed_map | _] when is_map(embed_map) -> stringify_keys(embed_map)
        _ -> %{}
      end

    task_name = Map.get(headers, "task")
    task_id = Map.get(headers, "id") || Map.get(properties, "correlation_id")

    if is_binary(task_name) and is_binary(task_id) do
      ctx = %Context{
        id: task_id,
        task: task_name,
        args: args,
        kwargs: kwargs,
        retries: Map.get(headers, "retries") || 0,
        is_eager: false,
        callbacks: Map.get(embed, "callbacks"),
        errbacks: Map.get(embed, "errbacks"),
        chain: Map.get(embed, "chain"),
        chord: Map.get(embed, "chord"),
        eta: Map.get(headers, "eta"),
        expires: Map.get(headers, "expires"),
        group: Map.get(headers, "group") || Map.get(headers, "taskset"),
        group_index: Map.get(headers, "group_index"),
        timelimit: normalize_timelimit(Map.get(headers, "timelimit")),
        root_id: Map.get(headers, "root_id") || task_id,
        parent_id: Map.get(headers, "parent_id"),
        argsrepr: Map.get(headers, "argsrepr"),
        kwargsrepr: Map.get(headers, "kwargsrepr"),
        origin: Map.get(headers, "origin"),
        ignore_result: Map.get(headers, "ignore_result", false) == true,
        shadow: Map.get(headers, "shadow"),
        replaced_task_nesting: Map.get(headers, "replaced_task_nesting") || 0,
        stamped_headers: Map.get(headers, "stamped_headers"),
        stamps: Map.get(headers, "stamps") || %{},
        reply_to: Map.get(properties, "reply_to"),
        correlation_id: Map.get(properties, "correlation_id") || task_id,
        delivery_info: delivery_info,
        headers: headers,
        properties: properties,
        called_directly: false
      }

      {:ok, ctx}
    else
      {:error, %Celery.InvalidTaskError{message: "Task message missing 'task' or 'id' header"}}
    end
  end

  # Protocol v1 (or hybrid): body is a map containing "task", "id", "args", "kwargs"
  def decode_parts(body, headers, properties, delivery_info) when is_map(body) do
    str_body = stringify_keys(body)
    merged = Map.merge(str_body, headers)

    task_name = Map.get(merged, "task")
    task_id = Map.get(merged, "id") || Map.get(properties, "correlation_id")
    args = Map.get(merged, "args") || []
    kwargs = Map.get(merged, "kwargs") || %{}

    cond do
      not is_binary(task_name) or not is_binary(task_id) ->
        {:error, %Celery.InvalidTaskError{message: "Task message v1 missing 'task' or 'id'"}}

      not is_list(args) ->
        {:error, %Celery.InvalidTaskError{message: "Task args must be a list"}}

      not is_map(kwargs) ->
        {:error, %Celery.InvalidTaskError{message: "Task kwargs must be a map"}}

      true ->
        ctx = %Context{
          id: task_id,
          task: task_name,
          args: args,
          kwargs: kwargs,
          retries: Map.get(merged, "retries") || 0,
          is_eager: false,
          callbacks: Map.get(merged, "callbacks"),
          errbacks: Map.get(merged, "errbacks"),
          chain: Map.get(merged, "chain"),
          chord: Map.get(merged, "chord"),
          eta: Map.get(merged, "eta"),
          expires: Map.get(merged, "expires"),
          group: Map.get(merged, "group") || Map.get(merged, "taskset"),
          group_index: Map.get(merged, "group_index"),
          timelimit: normalize_timelimit(Map.get(merged, "timelimit")),
          root_id: Map.get(merged, "root_id") || task_id,
          parent_id: Map.get(merged, "parent_id"),
          argsrepr: Map.get(merged, "argsrepr"),
          kwargsrepr: Map.get(merged, "kwargsrepr"),
          origin: Map.get(merged, "origin"),
          ignore_result: Map.get(merged, "ignore_result", false) == true,
          shadow: Map.get(merged, "shadow"),
          replaced_task_nesting: Map.get(merged, "replaced_task_nesting") || 0,
          stamped_headers: Map.get(merged, "stamped_headers"),
          stamps: Map.get(merged, "stamps") || %{},
          reply_to: Map.get(properties, "reply_to"),
          correlation_id: Map.get(properties, "correlation_id") || task_id,
          delivery_info: delivery_info,
          headers: headers,
          properties: properties,
          called_directly: false
        }

        {:ok, ctx}
    end
  end

  def decode_parts(other, _headers, _properties, _delivery_info) do
    {:error,
     %Celery.InvalidTaskError{message: "Unrecognized task message body: #{inspect(other)}"}}
  end

  @doc """
  Formats `eta` or `countdown` (in seconds) into an ISO-8601 UTC string.
  """
  @spec format_time_option(
          DateTime.t() | NaiveDateTime.t() | String.t() | number() | nil,
          number() | nil
        ) ::
          String.t() | nil
  def format_time_option(nil, nil), do: nil

  def format_time_option(nil, countdown) when is_number(countdown) do
    DateTime.utc_now()
    |> DateTime.add(round(countdown * 1000), :millisecond)
    |> DateTime.to_iso8601()
  end

  def format_time_option(%DateTime{} = dt, _countdown) do
    DateTime.to_iso8601(dt)
  end

  def format_time_option(%NaiveDateTime{} = ndt, _countdown) do
    ndt
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  def format_time_option(seconds, _countdown) when is_number(seconds) do
    DateTime.utc_now()
    |> DateTime.add(round(seconds * 1000), :millisecond)
    |> DateTime.to_iso8601()
  end

  def format_time_option(iso, _countdown) when is_binary(iso), do: iso

  @doc """
  Parses an ISO-8601 timestamp string, `DateTime`, or numeric timestamp into `{:ok, %DateTime{}}`.
  """
  @spec parse_datetime(term()) :: {:ok, DateTime.t()} | :error
  def parse_datetime(nil), do: :error
  def parse_datetime(%DateTime{} = dt), do: {:ok, dt}

  def parse_datetime(%NaiveDateTime{} = ndt) do
    {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
  end

  def parse_datetime(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _offset} ->
        {:ok, dt}

      {:error, _} ->
        case NaiveDateTime.from_iso8601(str) do
          {:ok, ndt} -> {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
          {:error, _} -> :error
        end
    end
  end

  def parse_datetime(unix_sec) when is_number(unix_sec) do
    DateTime.from_unix(round(unix_sec * 1000), :millisecond)
  end

  def parse_datetime(_), do: :error

  @doc """
  Returns `true` if `expires` is in the past compared to `now`.
  """
  @spec expired?(term(), DateTime.t()) :: boolean()
  def expired?(expires, now \\ DateTime.utc_now())
  def expired?(nil, _now), do: false

  def expired?(expires, now) do
    case parse_datetime(expires) do
      {:ok, exp_dt} -> DateTime.compare(now, exp_dt) == :gt
      :error -> false
    end
  end

  @doc """
  Returns the remaining delay in milliseconds until `eta`, or `0` if `eta` is nil or in the past.
  """
  @spec eta_delay_ms(term(), DateTime.t()) :: non_neg_integer()
  def eta_delay_ms(eta, now \\ DateTime.utc_now())
  def eta_delay_ms(nil, _now), do: 0

  def eta_delay_ms(eta, now) do
    case parse_datetime(eta) do
      {:ok, eta_dt} ->
        diff = DateTime.diff(eta_dt, now, :millisecond)
        if diff > 0, do: diff, else: 0

      :error ->
        0
    end
  end

  @doc """
  Formats positional arguments like Python's `saferepr(args)`:
  `[1, 2]` -> `"(1, 2)"`, `[1]` -> `"(1,)"`, `[]` -> `"()"`.
  """
  @spec format_argsrepr(list()) :: String.t()
  def format_argsrepr([]), do: "()"
  def format_argsrepr([single]), do: "(#{python_repr(single)},)"

  def format_argsrepr(args) when is_list(args) do
    inner = Enum.map_join(args, ", ", &python_repr/1)
    "(#{inner})"
  end

  @doc """
  Formats keyword arguments like Python's `saferepr(kwargs)`:
  `%{"a" => 1}` -> `"{'a': 1}"`, `%{}` -> `"{}"`.
  """
  @spec format_kwargsrepr(map()) :: String.t()
  def format_kwargsrepr(kwargs) when map_size(kwargs) == 0, do: "{}"

  def format_kwargsrepr(kwargs) when is_map(kwargs) do
    inner =
      kwargs
      |> Enum.sort_by(fn {k, _} -> to_string(k) end)
      |> Enum.map_join(", ", fn {k, v} ->
        "'#{k}': #{python_repr(v)}"
      end)

    "{#{inner}}"
  end

  defp python_repr(nil), do: "None"
  defp python_repr(true), do: "True"
  defp python_repr(false), do: "False"
  defp python_repr(bin) when is_binary(bin), do: "'#{String.replace(bin, "'", "\\'")}'"
  defp python_repr(num) when is_number(num), do: to_string(num)
  defp python_repr(atom) when is_atom(atom), do: "'#{Atom.to_string(atom)}'"

  defp python_repr(list) when is_list(list) do
    "[" <> Enum.map_join(list, ", ", &python_repr/1) <> "]"
  end

  defp python_repr(map) when is_map(map) do
    format_kwargsrepr(map)
  end

  defp python_repr(other), do: inspect(other)

  defp normalize_args(nil), do: []
  defp normalize_args(args) when is_list(args), do: args
  defp normalize_args(tuple) when is_tuple(tuple), do: Tuple.to_list(tuple)

  defp normalize_kwargs(nil), do: %{}

  defp normalize_kwargs(kwargs) when is_list(kwargs) do
    Map.new(kwargs, fn {k, v} -> {to_string(k), v} end)
  end

  defp normalize_kwargs(kwargs) when is_map(kwargs) do
    stringify_keys(kwargs)
  end

  defp normalize_timelimit([hard, soft]), do: [hard, soft]
  defp normalize_timelimit({hard, soft}), do: [hard, soft]
  defp normalize_timelimit(_), do: [nil, nil]

  defp normalize_signatures(nil), do: nil
  defp normalize_signatures([]), do: nil

  defp normalize_signatures(list) when is_list(list) do
    Enum.map(list, &normalize_signature/1)
  end

  defp normalize_signatures(single), do: [normalize_signature(single)]

  defp normalize_signature(nil), do: nil

  defp normalize_signature(%{__struct__: Celery.Canvas.Signature} = sig) do
    Celery.Canvas.Signature.to_map(sig)
  end

  defp normalize_signature(map) when is_map(map), do: stringify_keys(map)

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {to_string(k), v}
    end)
  end
end
