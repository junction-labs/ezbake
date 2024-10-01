# `ezbake`

`ezbake` is a single-binary service discovery server for your Kubernetes cluster
that makes client-side load balancing easy and accessible to everyone, even if
you're allergic to YAML. At this time ezbake is tested to support the [Junction
HTTP client][junction-client]. In the future we will add support for proxyless
[gRPC][grpc].

`ezbake` watches the local Kubernetes cluster, and creates endpoints for all
running k8s services, giving access to clients using the [xDS protocol][xds].
`ezbake` also will pass through any [Gateway API HTTPRoute][httproute]
configuration.

For the [Junction HTTP client][junction-client], `ezbake` will route all HTTP
requests of the form `http://${name}.${namespace}.svc.cluster.local` to the
service named `name` in the namespaced `namespace`. 

[xds]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol
[junction-client]: https://github.com/junction-labs/junction-client
[grpc]: https://grpc.io/
[httproute]: https://gateway-api.sigs.k8s.io/api-types/httproute/

## Running inside a shell

To build and run locally outside of k8s, using your user Kubernetes config:
```bash
cargo run
```

When using the Junction HTTP Client, you must then set up the shell's environment variable as: 
```bash
export JUNCTION_ADS_SERVER="grpc://localhost:8008"
```

## Running inside k8s

To instead build a native container, say for running within [OrbStack][orb]
locally:
```bash
docker build --tag ezbake --file ./scripts/Dockerfile-develop --load .
```

To run this container on a k8s cluster where you have full k8s administrative
privilieges, the following will install `ezbake` in a new namespace called
'junction':

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
kubectl apply -f ./scripts/install-for-cluster.yml 
```

To install `ezbake` on a k8s cluster where you do not have full k8s
administrative access, see the advanced directions below.

When using the Junction HTTP Client, you must then set up its environmen variable with the following command:
```bash
export JUNCTION_ADS_SERVER="grpc://"`kubectl get svc ezbake --namespace junction -o jsonpath='{.spec.clusterIP}'`":8008"
```

To cleanup:
```bash
kubectl delete -f ./scripts/install-for-cluster.yml 
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

[orb]: https://orbstack.dev/

## Advanced

### Multiarch build

To create a x86 and ARM64 multiarch build container that comes with the cost of
a slower build, you will need to do a one off installation of buildx:
```bash
docker buildx create --name mybuilder2 --use
docker buildx install
```

Then:
```bash
docker build --tag ezbake --file ./scripts/Dockerfile-multiarch --load .
```

### Deploying to Kubernetes in a Single Namespace

On a cluster where you only have access to a single namespace, you can still run
`ezbake`, but it does require your administrator to do some one-off setup. 

#### For Administator

You first need to install the cluster-wise Gateway API CRDs:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

Next, you must create a service account that has permissions for pulling from
the API server, by editing `foo` to your user's namespace and running:
```bash
kubectl apply -f ./scripts/install-for-namespace-admin.yml 
```

#### For User

Now as a user to start `ezbake`, all you need to do is edit `foo` in the
following file to your namespace and apply it with:
```bash
kubectl apply -f ./scripts/install-for-namespace.yml 
```
