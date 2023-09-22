defmodule K8s.Client.Mint.ConnectionRegistry do
  @moduledoc """
  A registry for open connections. As HTTP/2 allows simultaneous requests,
  we handle multiple requests with one process. In this case, the registry
  stores the PID of the HTTPAdapter which is connected according to the
  given connection details (URI/opts).

  HTTP/1 only allows one request per connection at a time. In order to
  support simultaneous requests, we need a connection pool. This is where
  the `:poolboy` library comes in.
  """

  use GenServer

  alias K8s.Client.HTTPError
  alias K8s.Client.Mint.HTTPAdapter

  require Logger
  import K8s.Sys.Logger, only: [log_prefix: 1]

  @poolboy_config [
    worker_module: K8s.Client.Mint.HTTPAdapter,
    size: 10,
    max_overflow: 20,
    strategy: :fifo
  ]

  @type uriopts :: {URI.t(), keyword()}
  @type adapter_type_t :: :adapter_pool | :singleton
  @type adapter_pool_t :: %{required(:adapter) => pid(), required(:pool) => pid() | nil}

  @doc """
  Starts the registry.
  """
  @spec start_link(any) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  ets a `HTTPAdapter` process from the registry.

  If the returned process is an adapter pool, an adapter is checked out from
  the pool and a map with both PIDs is returned.

  If the returned process is an adapter process, a map with its PID and `pool`
  set to `nil` is returned.
  """
  @spec checkout(uriopts()) :: {:ok, adapter_pool_t()} | {:error, HTTPError.t()}
  def checkout({uri, opts}) do
    Logger.info(log_prefix("Checking out adapter=#{inspect(uri)} opts=#{inspect(opts)}"),
      library: :k8s
    )

    key = HTTPAdapter.connection_args(uri, opts)

    case GenServer.call(__MODULE__, {:get_or_open, key}, 5000) do
      {:ok, {:singleton, pid}} ->
        Logger.info(
          log_prefix(
            "Checkout singleton pid=#{inspect(pid)} uri=#{inspect(uri)} opts=#{inspect(opts)}"
          ),
          library: :k8s
        )

        # Check if the connection is open for writing.
        if HTTPAdapter.open?(pid, :write) do
          Logger.info(
            log_prefix(
              "Singleton Adapter singleton pid=#{inspect(pid)} uri=#{inspect(uri)} opts=#{inspect(opts)}"
            ),
            library: :k8s
          )

          {:ok, %{adapter: pid, pool: nil}}
        else
          Logger.info(
            log_prefix(
              "Singleton Adapter singleton not open pid=#{inspect(pid)} uri=#{inspect(uri)} opts=#{inspect(opts)}"
            ),
            library: :k8s
          )

          # The connection is closed for writing and needs to be removed from
          # the registry
          Logger.debug(
            log_prefix("Connection is not open for writing. Removing it."),
            library: :k8s
          )

          GenServer.cast(__MODULE__, {:remove, key})
          checkout({uri, opts})
        end

      {:ok, {:adapter_pool, pool_pid}} ->
        try do
          {:ok, %{adapter: :poolboy.checkout(pool_pid), pool: pool_pid}}
        catch
          :exit, {:timeout, _} ->
            {:error,
             HTTPError.new(message: "Failed getting a connection. The connection pool is empty")}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  @spec checkin(adapter_pool_t()) :: :ok
  def checkin(%{adapter: worker_pid, pool: nil}) do
    Logger.info(log_prefix("Checking in adapter=#{inspect(worker_pid)} pool_pid={nil}"),
      library: :k8s
    )

    :ok
  end

  def checkin(%{adapter: worker_pid, pool: pool_pid}) do
    Logger.info(
      log_prefix("Checking in adapter=#{inspect(worker_pid)} pool_pid=#{inspect(pool_pid)}"),
      library: :k8s
    )

    :poolboy.checkin(pool_pid, worker_pid)
  end

  @impl true
  def init(:ok) do
    adapters = %{}
    refs = %{}
    {:ok, {adapters, refs}}
  end

  @impl true
  def handle_call({:get_or_open, key}, _from, {adapters, refs}) when is_map_key(adapters, key) do
    Logger.info(
      log_prefix("Monitor handle_call/4 key=#{inspect(key)} - adapter_spec=#{inspect(adapters)}"),
      library: :k8s
    )

    {:reply, Map.fetch(adapters, key), {adapters, refs}}
  end

  def handle_call({:get_or_open, key}, _from, {adapters, refs}) do
    {scheme, host, port, opts} = key

    # Connect to the server to see if the server supports HTTP/2
    with {:ok, conn} <- Mint.HTTP.connect(scheme, host, port, opts),
         {type, adapter_spec} <- get_adapter_spec(conn, key),
         {:ok, adapter} <-
           DynamicSupervisor.start_child(K8s.Client.Mint.ConnectionSupervisor, adapter_spec) do
      Mint.HTTP.close(conn)

      Logger.info(
        log_prefix("Monitor connect=#{inspect(adapter)} - adapter_spec=#{inspect(adapter_spec)}"),
        library: :k8s
      )

      ref = Process.monitor(adapter)
      refs = Map.put(refs, ref, key)
      adapters = Map.put(adapters, key, {type, adapter})

      Logger.info(
        log_prefix("Monitor connect=#{inspect(adapter)} - adapters=#{inspect(adapters)}"),
        library: :k8s
      )

      {:reply, {:ok, {type, adapter}}, {adapters, refs}}
    else
      {:error, %HTTPError{} = error} ->
        {:reply, {:error, error}, {adapters, refs}}

      {:error, error} ->
        {:reply, {:error, HTTPError.from_exception(error)}, {adapters, refs}}
    end
  end

  @impl true
  def handle_cast({:remove, key}, {adapters, refs}) do
    Logger.info(log_prefix("handle_cast/2 to remove connection key=#{inspect(key)}"),
      library: :k8s
    )

    adapters = Map.delete(adapters, key)
    {:noreply, {adapters, refs}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, {adapters, refs}) do
    Logger.debug(log_prefix("DOWN of process #{inspect(pid)} received."), library: :k8s)
    {key, refs} = Map.pop(refs, ref)

    Logger.debug(
      log_prefix(
        "handle_info - DOWN - pid=#{inspect(pid)} Keys=#{inspect(key)} of process refs=#{inspect(refs)} received."
      ),
      library: :k8s
    )

    adapters = Map.delete(adapters, key)

    Logger.debug(
      log_prefix(
        "handle_info - DOWN - pid=#{inspect(pid)} Adapters=#{inspect(key)} of process refs=#{inspect(refs)} received."
      ),
      library: :k8s
    )

    {:noreply, {adapters, refs}}
  end

  def handle_info(other, state) do
    Logger.debug(log_prefix("other message received: #{inspect(other)}"), library: :k8s)
    {:noreply, state}
  end

  @spec get_adapter_spec(Mint.HTTP.t(), HTTPAdapter.connection_args_t()) ::
          {adapter_type_t(), :supervisor.child_spec()}
  defp get_adapter_spec(conn, conn_args) do
    case Mint.HTTP.protocol(conn) do
      :http1 ->
        Logger.debug(
          log_prefix("inspected adapter_pool http1 support for: #{inspect(conn_args)}"),
          library: :k8s
        )

        {:adapter_pool,
         %{id: conn_args, start: {:poolboy, :start_link, [@poolboy_config, conn_args]}}}

      :http2 ->
        Logger.debug(
          log_prefix("inspected adapter_pool http2 support for: #{inspect(conn_args)}"),
          library: :k8s
        )

        {:singleton, {HTTPAdapter, conn_args}}
    end
  end
end
