defmodule K8s.Client.Runner.BaseIntegrationTest do
  use ExUnit.Case, async: true
  import K8s.Test.IntegrationHelper

  import YamlElixir.Sigil

  setup_all do
    conn = conn()

    on_exit(fn ->
      K8s.Client.delete_all("v1", "Pod", namespace: "default")
      |> K8s.Selector.label({"k8s-ex-test", "base"})
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

      K8s.Client.delete_all("v1", "ConfigMap", namespace: "default")
      |> K8s.Selector.label({"k8s-ex-test", "base"})
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()
    end)

    [conn: conn]
  end

  setup do
    test_id = :rand.uniform(10_000)
    labels = %{"k8s-ex-base-test" => "#{test_id}", "k8s-ex-test" => "base"}

    {:ok, %{test_id: test_id, labels: labels}}
  end

  describe "cluster scoped resources" do
    @tag :integration
    test "creating a resource", %{conn: conn, test_id: test_id, labels: labels} do
      namespace = %{
        "apiVersion" => "v1",
        "kind" => "Namespace",
        "metadata" => %{"name" => "k8s-ex-#{test_id}", "labels" => labels}
      }

      operation = K8s.Client.create(namespace)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result

      operation = K8s.Client.delete(namespace)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result
    end

    @tag :integration
    test "getting a resource", %{conn: conn} do
      operation = K8s.Client.get("v1", "Namespace", name: "default")
      result = K8s.Client.run(conn, operation)

      assert {:ok, %{"apiVersion" => "v1", "kind" => "Namespace"}} = result
    end

    @tag :integration
    test "listing resources", %{conn: conn} do
      operation = K8s.Client.list("v1", "Namespace")

      assert {:ok,
              %{
                "items" => namespaces,
                "apiVersion" => "v1",
                "kind" => "NamespaceList"
              }} = K8s.Client.run(conn, operation)

      namespace_names = Enum.map(namespaces, fn ns -> get_in(ns, ["metadata", "name"]) end)
      assert Enum.member?(namespace_names, "default")
    end
  end

  describe "namespaced scoped resources" do
    @tag :integration
    test "creating a resource", %{conn: conn, test_id: test_id, labels: labels} do
      pod = build_pod("k8s-ex-#{test_id}", labels)

      assert {:ok, _pod} =
               pod
               |> K8s.Client.create()
               |> K8s.Client.put_conn(conn)
               |> K8s.Client.run()

      assert {:ok, _pod} =
               pod
               |> K8s.Client.delete()
               |> K8s.Client.put_conn(conn)
               |> K8s.Client.run()
    end

    @tag :integration
    test "when the request is unauthorized", %{conn: conn} do
      operation = K8s.Client.get("v1", "ServiceAccount", name: "default", namespace: "default")
      unauthorized = %K8s.Conn.Auth.Token{token: "nope"}
      unauthorized_conn = %K8s.Conn{conn | auth: unauthorized}

      {:error, error} = K8s.Client.run(unauthorized_conn, operation)

      assert %K8s.Client.APIError{
               __exception__: true,
               message: "Unauthorized",
               reason: "Unauthorized"
             } == error
    end

    @tag :integration
    test "getting a resource", %{conn: conn} do
      operation = K8s.Client.get("v1", "ServiceAccount", name: "default", namespace: "default")
      result = K8s.Client.run(conn, operation)

      assert {:ok, %{"apiVersion" => "v1", "kind" => "ServiceAccount"}} = result
    end

    @tag :integration
    test "getting a resource that doesn't exist returns an error", %{conn: conn} do
      operation = K8s.Client.get("v1", "ServiceAccount", name: "NOPE", namespace: "default")
      {:error, error} = K8s.Client.run(conn, operation)

      assert %K8s.Client.APIError{
               message: "serviceaccounts \"NOPE\" not found",
               reason: "NotFound"
             } == error
    end

    @tag :integration
    test "applying a resource that does not exist", %{
      conn: conn,
      test_id: test_id,
      labels: labels
    } do
      pod = build_pod("k8s-ex-#{test_id}", labels)
      operation = K8s.Client.apply(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result

      operation = K8s.Client.delete(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result
    end

    @tag :integration
    test "applying a resource that does already exist", %{
      conn: conn,
      test_id: test_id,
      labels: labels
    } do
      pod = build_pod("k8s-ex-#{test_id}", labels)
      # make sure pod is created with no label called "some"
      assert is_nil(pod["metadata"]["labels"]["some"])

      operation = K8s.Client.apply(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result

      pod = put_in(pod, ["metadata", "labels"], %{"some" => "change"})
      operation = K8s.Client.apply(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result

      operation = K8s.Client.get(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, pod} = result

      assert "change" == pod["metadata"]["labels"]["some"]

      operation = K8s.Client.delete(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result
    end

    @tag :integration
    test "applying a resource with different managers should return a conflict error", %{
      conn: conn,
      test_id: test_id,
      labels: labels
    } do
      pod = build_pod("k8s-ex-#{test_id}", Map.put(labels, "some", "init"))

      # make sure pod is created with label "some"
      assert "init" == pod["metadata"]["labels"]["some"]

      operation = K8s.Client.apply(pod, field_manager: "k8s_test_mgr_1", force: false)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result

      pod = put_in(pod, ["metadata", "labels"], %{"some" => "change"})
      operation = K8s.Client.apply(pod, field_manager: "k8s_test_mgr_2", force: false)
      result = K8s.Client.run(conn, operation)

      assert {:error,
              %K8s.Client.APIError{
                message:
                  "Apply failed with 1 conflict: conflict with \"k8s_test_mgr_1\": .metadata.labels.some",
                reason: "Conflict"
              }} == result

      operation = K8s.Client.delete(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result
    end

    @tag :integration
    test "applying a new status to a pod", %{conn: conn, test_id: test_id, labels: labels} do
      pod = build_pod("k8s-ex-#{test_id}", labels)
      operation = K8s.Client.apply(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result

      pod_with_status = Map.put(pod, "status", %{"message" => "some message"})

      operation =
        K8s.Client.apply(
          "v1",
          "pods/status",
          [namespace: "default", name: "k8s-ex-#{test_id}"],
          pod_with_status
        )

      result = K8s.Client.run(conn, operation)
      assert {:ok, pod} = result

      operation =
        K8s.Client.get(
          "v1",
          "pods",
          namespace: "default",
          name: "k8s-ex-#{test_id}"
        )

      result =
        K8s.Client.wait_until(conn, operation,
          find: ["status", "message"],
          eval: "some message",
          timeout: 60
        )

      assert {:ok, _} = result

      operation = K8s.Client.delete(pod)
      result = K8s.Client.run(conn, operation)
      assert {:ok, _pod} = result
    end

    @tag :integration
    test "listing resources", %{conn: conn} do
      operation = K8s.Client.list("v1", "ServiceAccount", namespace: "default")

      assert {:ok,
              %{
                "items" => service_accounts,
                "apiVersion" => "v1",
                "kind" => "ServiceAccountList"
              }} = K8s.Client.run(conn, operation)

      assert length(service_accounts) > 0
    end

    @tag :integration
    test "creating a operation without correct kind `pod/exec` should return an error", %{
      conn: conn
    } do
      operation =
        K8s.Client.connect("v1", "not-real", namespace: "default", name: "nginx-76d6c9b8c-sq56w")

      assert {:error,
              %K8s.Discovery.Error{message: "Unsupported Kubernetes resource: \"not-real\""}} =
               K8s.Client.run(conn, operation)
    end
  end

  @tag :integration
  @tag :websocket
  test "runs :connect operations for pods/exec and returns stdout", %{
    conn: conn,
    labels: labels,
    test_id: test_id
  } do
    {:ok, created_pod} =
      build_pod("k8s-ex-#{test_id}", labels)
      |> K8s.Client.create()
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

    {:ok, _} =
      K8s.Client.wait_until(conn, K8s.Client.get(created_pod),
        find: ["status", "containerStatuses", Access.filter(&(&1["ready"] == true))],
        eval: &match?([_ | _], &1),
        timeout: 60
      )

    {:ok, response} =
      K8s.Client.connect(
        created_pod["apiVersion"],
        "pods/exec",
        [namespace: K8s.Resource.namespace(created_pod), name: K8s.Resource.name(created_pod)],
        command: ["/bin/sh", "-c", ~s(echo "ok")],
        tty: false
      )
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

    assert response.stdout =~ "ok"
  end

  @tag :integration
  @tag :websocket
  @tag :wip
  @tailLines 5
  test "runs :connect operations for pods/log and returns stdout", %{
    conn: conn,
    labels: labels,
    test_id: test_id
  } do
    {:ok, created_pod} =
      build_pod("k8s-ex-#{test_id}", labels)
      |> K8s.Client.create()
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

    {:ok, _} =
      K8s.Client.wait_until(conn, K8s.Client.get(created_pod),
        find: ["status", "containerStatuses", Access.filter(&(&1["ready"] == true))],
        eval: &match?([_ | _], &1),
        timeout: 60
      )

    {:ok, response} =
      K8s.Client.connect(
        created_pod["apiVersion"],
        "pods/log",
        [namespace: K8s.Resource.namespace(created_pod), name: K8s.Resource.name(created_pod)],
        tailLines: @tailLines
      )
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

    assert String.printable?(response.stdout)
    lines = String.split(response.stdout)
    assert @tailLines == length(lines)
  end

  @tag :integration
  @tag :websocket
  test "runs :connect operations and returns errors", %{
    conn: conn,
    labels: labels,
    test_id: test_id
  } do
    {:ok, created_pod} =
      build_pod("k8s-ex-#{test_id}", labels)
      |> K8s.Client.create()
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

    {:ok, _} =
      K8s.Client.wait_until(conn, K8s.Client.get(created_pod),
        find: ["status", "containerStatuses", Access.filter(&(&1["ready"] == true))],
        eval: &match?([_ | _], &1),
        timeout: 60
      )

    {:ok, response} =
      K8s.Client.connect(
        created_pod["apiVersion"],
        "pods/exec",
        [namespace: K8s.Resource.namespace(created_pod), name: K8s.Resource.name(created_pod)],
        command: ["/bin/sh", "-c", "no-such-command"],
        tty: false
      )
      |> K8s.Client.put_conn(conn)
      |> K8s.Client.run()

    assert response.error =~ "error executing command"
    assert response.stderr =~ "not found"
  end

  @tag :integration
  @tag :websocket
  test "returns error if connection fails", %{conn: conn} do
    result =
      K8s.Client.run(
        conn,
        K8s.Client.connect(
          "v1",
          "pods/exec",
          [
            namespace: "default",
            name: "does-not-exist"
          ],
          command: ["/bin/sh"],
          tty: false
        )
      )

    assert {:error, error} = result
    assert error.message =~ "404"
  end

  @tag :integration
  @tag :reliability
  test "concurrent requests succeed", %{conn: conn} do
    results =
      0..100
      |> Task.async_stream(fn _ ->
        K8s.Client.list("v1", "pod")
        |> K8s.Operation.put_query_param(:limit, 100)
        |> K8s.Client.put_conn(conn)
        |> K8s.Client.run()
      end)
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, %{}}}, &1))
  end

  @tag :integration
  test "requests with big bodies succeed", %{conn: conn, labels: labels} do
    data = for nr <- 1..55_763, into: %{}, do: {"key#{nr}", "value"}

    # The body size of this ConfigMap is larger than the window size.
    cm =
      ~y"""
      apiVersion: v1
      kind: ConfigMap
      metadata:
        namespace: default
        name: big-cm
        annotations:
          key: value-😀
      """
      |> Map.put("data", data)
      |> put_in(~w(metadata labels), labels)

    assert {:ok, _} =
             K8s.Client.create(cm)
             |> K8s.Client.put_conn(conn)
             |> K8s.Client.run()
  end
end
