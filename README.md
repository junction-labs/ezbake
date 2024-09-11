# ezbake

`ezbake` is a single-binary service discovery server for your Kubernetes cluster
that makes client-side load balancing and traffic routing for HTTP and GRPC
services easy and accessible to everyone, even if you're allergic to YAML.

`ezbake` watches the local Kubernetes cluster, and creates endpoints for all running ```Service``` 
accessed using the xDS protocol (https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol). 

At this time ezbake is tested to support two XDS clients:
- junction-client (https://github.com/junction-labs/junction-client)
- gRPC (https://grpc.io/)

For junction-client, ezbake will route all HTTP requests of the form `http-jct://${name}.${namespace}` 
to the service named `name` in the namespaced `namespace`.

For gRPC, ezbake will route all requests for the service `xds:///${name}.${namespace}` to the 
service named `name` in the namespaced `namespace`.

The junction-client allows richer XDS behaviour to be specified in the client code. Both
options also support the Gateway API, documented below.

## Building and Deploying to kubernetes

```
docker build --tag ezbake --file ./scripts/Dockerfile .
kubectl apply -f ./scripts/install.yml 
```

## Using the Gateway API

To configure routing rules, `ezbake` supports the HTTPRoute and GRPCRoute
Kubernetes Gateaway API.  This is a cluster-wide CRD, which can be installed with:

```
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

To set up routing information for all HTTP clients of a service, define an
`HTTPRoute` (https://gateway-api.sigs.k8s.io/guides/http-routing/)  for a Service in the same 
namespace the `Service` object is defined in.  For example, if we wanted to send all traffic to
`http-jct://cool-service.cool-user/v2/` to `cooler-service` instead, we could define a rule like:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: cool-route
spec:
  parentRefs:
  - name: cool-service
    kind: Service
    group: ""
  rules:
  - matches:
    - path:
      value: "/v2/"
    backendRefs:
    - name: cooler-service
      port: 8080
```

And then install it like:

FIXME

In the case of gRPC FIXME:

```yaml
apiVersion: gateway.networking.k8s.io/v1alpha2
kind: GRPCRoute
metadata:
  name: bar-route
spec:
  parentRefs:
  - name: cool-service
    kind: Service
  rules:
  - matches:
    - method:
        service: com.example.User
        method: Login
    backendRefs:
    - name: cooler-service
      port: 50051
```

## Using the Junction Gateway API extended policies

At this time, the Kubernetes Gateway API does not natively support many load 
balancing features. We have thus created extended policies, defined at FIXME.

At this point in time, we do not make them available as a CRD, as they
are still in development. Instead, they can be flattened as service annotations

FIXME
