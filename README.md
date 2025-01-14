# `ezbake`: A simple xDS control-plane for Junction.

## About

`ezbake` is a simple xDS control plane for Junction. `ezbake` runs in a
Kubernetes cluster, watches its running services, and creates appropriate xds
configuration.

### Installing

The simplest installation is as follows, which first sets up the Kubernetes
gateway API CRD, and then sets up ezbake as a 2 pod deployment in its own
namespace (junction), with permissions to monitor all services, endpoints, and
gateway API config in the cluster.

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml
kubectl apply -f https://github.com/junction-labs/ezbake/releases/latest/download/install-for-cluster.yml
```

Now, to communicate with ezbake, all clients will need the `JUNCTION_ADS_SERVER` environment 
variable set as follows:

```bash
export JUNCTION_ADS_SERVER="grpc://ezbake.junction.svc.cluster.local:8008"
```

> [!NOTE]
>
> `ezbake` returns Pod IPs directly without any NAT, so if your cluster
> isn't configured to allow talking directly to Pod IPs from outside the cluster,
> any client you run outside the cluster **won't be able to connect to any
> backends**.  Notably, local clusters created with `k3d`, `kind`, and Docker
> Desktop behave this way.


Once you have an `ezbake` running, use [Junction][junction-client] to generate
Routing and Load Balancing configuration that you can apply to this cluster.
`ezbake` will automatically pick up your configuration and distribute it to any
connected client. See the [Getting Stated Guide](https://docs.junctionlabs.io/getting-started/)
for more.

### Uninstalling

To uninstall, run `kubectl delete` on the Gateway APIs and the objects that
`ezbake` installed:

```bash
kubectl delete -f https://github.com/junction-labs/ezbake/releases/latest/download/install-for-cluster.yml
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
