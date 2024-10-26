# `ezbake`

An xDS control-plane for your Kubernetes cluster that makes client-side load
balancing easy and accessible to everyone (even if you're allergic to YAML).

## About

`ezbake` is a simple control plane for
[Junction client][junction-client] and [proxyless gRPC][proxyless-grpc].

[proxyless-grpc]: https://cloud.google.com/service-mesh/docs/service-routing/proxyless-overview

`ezbake` watches a Kubernetes cluster and creates xDS configuration for all
running Services. For every `Service` in your cluster, `ezbake` makes sure you
can access it at `http://${name}.${namespace}.svc.cluster.local`. To configure
routing, load-balancing, retry policies and more check out the [Junction
client][junction-client] documentation.

[junction-client]: https://github.com/junction-labs/junction-client

## Using `ezbake`

`ezbake` is designed to run statelessly and replicated inside your cluster. Run
as many instances of `ezbake` as you'd like in your Kubernetes cluster behind a
`ClusterIP` Service and connect to them directly. To get set up, see the next
setion.

Once you have an `ezbake` running, use [Junction][junction-client] to generate
Routing and Load Balancing configuration that you can apply to this cluster.
`ezbake` will automatically pick up your configuration and distribute it to any
connected client.

> [!NOTE]
>
> `ezbake` returns Pod IPs directly without any NAT, so if your cluster
> isn't configured to allow talking directly to Pod IPs from outside the cluster,
> any client you run outside the cluster **won't be able to connect to any
> backends**.  Notably, local clusters created with `k3d`, `kind`, and Docker
> Desktop behave this way.

### Installing

`ezbake` is published as a docker image to
`ghcr.io/junction-labs/junction-labs/ezbake:latest` on every push. To install
the Gateway APIs and run an `ezbake` in a new namespace with permissions to
watch the whole cluster, run:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml
kubectl apply -f ./scripts/install-for-cluster.yml
```

This sets up a new namespace named `junction`, a `ServiceAccount` with
permissions to watch the whole cluster, and an `ezbake` Deployment.

To connect to your new `ezbake` with a Junction HTTP Client, use the newly
defined Kubernetes Service as your `JUNCTION_ADS_SERVER`:

```bash
export JUNCTION_ADS_SERVER="grpc://"`kubectl get svc ezbake --namespace junction -o jsonpath='{.spec.clusterIP}'`":8008"
```

> [!NOTE]
>
> These policies don't require the Gateway APIs to be defined before they're
> run, but they won't actually give the ServiceAccount permission to watch or
> list `HTTPRoute`s until the Gateway APIs are defined and the RBAC rules are
> re-created.
>
> To set up an `ezbake` with the Gateway APIs and our example policies, you
> need full administrative access to your cluster. If you don't have full
> access, see the advanced directions below.

### Uinstalling

To uninstall, run `kubectl delete` on the Gateway APIs and the objects that
`ezbake` installed:

```bash
kubectl delete -f ./scripts/install-for-cluster.yml
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml
```

### Building from source

You can build and run `ezbake` locally and connect to a remote Kubernetes, as
long as you have a valid kubeconfig available. `ezbake` checks the standard
Kubernetes configuration paths, and uses your currently set context. To change
your currently set context, use `kubectl config use-context`.

To build ezbake, you'll need a working Rust toolchain - we recommend installing
[rustup](https://rustup.rs) if you haven't already.

Build ezbake locally with `cargo build --release` and run it with `./target/release/ezbake`.

`ezbake` is a single-binary server. Run `ezbake --help` for full usage information:

```text
Usage: ezbake [OPTIONS]

Options:
      --log-pretty
          Log in a pretty, human-readable format

  -l, --listen-addr <LISTEN_ADDR>
          The address to listen on

          [default: 0.0.0.0:8008]

      --metrics-addr <METRICS_ADDR>
          The address to expose metrics on

          [default: 0.0.0.0:8009]

      --all-namespaces
          Watch all namespaces. Defaults to false.

          It's an error to set both --all-namespaces and --namespace.

      --namespace <NAMESPACE>
          The namespace to watch. If this option is not set explicitly, ezbake
          will watch the the namespace set in the kubeconfig's s current
          context, the namespace specified by the service account the server is
          running as, or the `default` namespace.

          It's an error to set both --all-namespaces and --namespace.

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

By default, `ezbake` listens on `0.0.0.0:8008`. Don't forget to set up [your
client][junction-client] to look for your server on that address.

### Building a container

```bash
docker build --tag ezbake --file ./scripts/Dockerfile-develop --load .
```

## Advanced

### Deploying to Kubernetes in a Single Namespace

On a cluster where you only have access to a single namespace, you can still run
`ezbake`. If you already have the Gateway APIs installed and the ability to
`get`, `list`, and `watch` everything in your own namespace, skip the following
section.

#### One-time admin actions

Have your cluster admin install the Gateway APIs by [following the official instructions][official-instructions].

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml
```

[official-instructions]: https://gateway-api.sigs.k8s.io/guides/#installing-gateway-api

Next, create a service account that has permissions to list and watch the API
server. The `ServiceAccount`, `Role` and `RoleBinding` in
`scripts/install-for-namespace-admin.yml` list all of the required privileges.
Feel free to copy that template, replace `foo` with your namespace, and apply it
to the cluster:

```bash
# run this as a cluster admin
sed 's/foo/$YOUR_NAMESPACE_HERE/' < scripts/install-for-namespace-admin.yml > ezbake-role.yml
kubectl apply -f ezbake-role.yml
```

#### Deploying ezbake

Deploy `ezbake` as you would any other Deployment, making sure to run it as the
`ServiceAccount` created with permissions. The template in
`install-for-namespace.yml` gives an example, and can be used as a template to
get started.

```bash
sed 's/foo/$YOUR_NAMESPACE_HERE/' < scripts/install-for-namespace.yml > ezbake.yml
kubectl apply -f ezbake.yml
```
