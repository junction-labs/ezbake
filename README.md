# ezbake

`ezbake` is a single-binary service discovery server for your Kubernetes cluster
that makes client-side load balancing and traffic routing for HTTP and GRPC
services easy and accessible to everyone, even if you're allergic to YAML.

`ezbake` watches the local Kubernetes cluster, and creates endpoints for all running ```Service``` 
accessed using the xDS protocol (https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol). 

At this time ezbake is tested to support two XDS clients:
- junction-client (https://github.com/junction-labs/junction-client)
- gRPC (https://grpc.io/)

For junction-client, `ezbake` will route all HTTP requests of the form `http-jct://${name}.${namespace}` 
to the service named `name` in the namespaced `namespace`.

For gRPC, ezbake will route all requests for the endpoint `grpc://${name}.${namespace}` to the 
service named `name` in the namespaced `namespace`.

The junction-client allows richer XDS behaviour to be specified in the client code. Both
options also support the Gateway API, documented below.

For full samples, see https://github.com/junction-labs/junction-test

## Building

For running locally outside of Kubernetes:
```bash
cargo run
```

For doing a native build docker say for running within orb:
```bash
docker build --tag ezbake --file ./scripts/Dockerfile-develop --load .
```

For a multiarch container that comes with the cost of a slower build,
you will need to do a one off installation of buildx:
```bash
docker buildx create --name mybuilder2 --use
docker buildx install
```

Then:
```bash
docker build --tag ezbake --file ./scripts/Dockerfile-multiarch --load .
```

## Deploying to Kubernetes

On a cluster where you have full administrative privilieges, this will 
install `ezbake` with its own service account in the 'juction' namespace:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
kubectl apply -f ./scripts/install-for-cluster.yml 
```

On a cluster where you only have access to a namespace, this simpler install
will set up  a `deployment` and `service` for `ezbake` within it. 

First, if you wish to use the Gateway API CRDs, then an admin must install them with:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

Secondly, if even within the namespace your services are locked down from accessing the 
API server, your admin must create a service account with permissions by updating `foo`
as needed and then running
```bash
kubectl apply -f ./scripts/install-for-namespace-admin.yml 
```

Finally, to run ezbake in your namespace, update `foo` to your namespace and start ezbake with:
```bash
kubectl apply -f ./scripts/install-for-namespace.yml 
```

## Using the Junction Gateway API extended policies

At this time, the Kubernetes Gateway API does not natively support many load 
balancing features. We have thus created extended policies, defined at FIXME.

At this point in time, we do not make them available as a CRD, as they
are still in development. Instead, they can be flattened as service annotations

FIXME
