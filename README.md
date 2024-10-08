# `ezbake`

An xDS control-plane for your Kubernetes cluster that makes client-side load
balancing easy and accessible to everyone (even if you're allergic to YAML).

## About

`ezbake` is a single-binary, stateless [xDS][xds] control plane for the
[Junction client][junction-client] and [proxyless gRPC][proxyless-grpc] clients.

[proxyless-grpc]: https://cloud.google.com/service-mesh/docs/service-routing/proxyless-overview

`ezbake` watches a Kubernetes cluster and creates xDS configuration for all
running Services. For every `Service` in your cluster, `ezbake` generates a
Listener named `http://${name}.${namespace}.svc.cluster.local`,
RouteConfigurations based on any attached [`HTTPRoute`s][httproute], and
Clusters and Listeners based on the currently healthy endpoints and your load
balancing configuration.

## Roadmap

- [x] Basic xDS configuration for all running Services.
- [ ] Route configuration with Gateway API `HTTPRoute`s.
- [ ] Load balancing configuration with CRDs.

[junction-client]: https://github.com/junction-labs/junction-client
[xds]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol
[httproute]: https://gateway-api.sigs.k8s.io/api-types/httproute/

## Usage

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

## Using `ezbake`

> **NOTE**: `ezbake` is currently not available as a pre-built container. To build
> and run it, you'll need a working Rust toolchain - we recommend installing
> [rustup](https://rustup.rs) if you haven't already.

> **NOTE**: `ezbake` doesn't do anything without a Kubernetes cluster. If you don't
> have one running, we recommend either using the cluster built into Docker Desktop or
> OrbStack, or setting up a local cluster with `k3s`.

### Building and Running in Kubernetes

First, build a container:

```bash
docker build --tag ezbake --file ./scripts/Dockerfile-develop --load .
```

By default `ezbake` requires permissions to read and watch all services in all
namespaces, but can be run in a mode where it only watches a single namespace
with the `--namespace` flag.

**NOTE**: To set up an `ezbake` with the Gateway APIs and our example policies,
you need full administrative access to your cluster. If you don't have full
access, see the advanced directions below.

The example policies in `./scripts/install-for-cluster.yml` set up a new
namespace named `junction`, a `ServiceAccount` with permissions to watch the
whole cluster, and an `ezbake` Deployment.

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
kubectl apply -f ./scripts/install-for-cluster.yml
```

**NOTE**: These policies don't require the Gateway APIs to be defined before they're run, but
they won't actually give the ServiceAccount permission to watch or list
`HTTPRoute`s until the Gateway APIs are defined and the RBAC rules are
re-created.

To connect to your new `ezbake` with a Junction HTTP Client, use the newly
defined Kubernetes Service as your `JUNCTION_ADS_SERVER`:

```bash
export JUNCTION_ADS_SERVER="grpc://"`kubectl get svc ezbake --namespace junction -o jsonpath='{.spec.clusterIP}'`":8008"
```

To uninstall, run `kubectl delete` on the Gateway APIs and the Junction example objects:

```bash
kubectl delete -f ./scripts/install-for-cluster.yml
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

To install `ezbake` on a k8s cluster where you do not have full k8s administrative access, see the
advanced directions below.

### Building and running locally

**NOTE**: `ezbake` returns Pod IPs directly to clients, so if your cluster isn't
configured to allow talking directly to Pod IPs from outside the cluster, any
client you run outside the cluster **won't be able to connect to any backends**.
Notably, local clusters created with `k3d`, `kind`, and Docker Desktop behave
this way.

You can run `ezbake` locally and connect to a remote Kubernetes, as long as you
have a valid kubeconfig available. `ezbake` checks the standard Kubernetes
configuration paths, and uses your currently set context. To change your currently
set context, use `kubectl config use-context`.

Build ezbake locally with `cargo build --release` and run it with
`./target/release/ezbake`. Feel free to copy the binary wherever you'd like.

By default, `ezbake` listens on `0.0.0.0:8008`. Don't forget to set up [your client][junction-client]
to look for your server on that address.

## Advanced

### Deploying to Kubernetes in a Single Namespace

On a cluster where you only have access to a single namespace, you can still run
`ezbake`. If you already have the Gateway APIs installed and the ability to
`get`, `list`, and `watch` everything in your own namespace, skip the following
section.

#### One-time admin actions

Have your cluster admin install the Gateway APIs by [following the official instructions][official-instructions].

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
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

Deploy `ezbake` as you would any other Deployment, making sure to run it as the `ServiceAccount`
created with permissions. The template in `install-for-namespace.yml` gives an example, and
can be used as a template to get started.

```bash
sed 's/foo/$YOUR_NAMESPACE_HERE/' < scripts/install-for-namespace.yml > ezbake.yml
kubectl apply -f ezbake.yml
```
