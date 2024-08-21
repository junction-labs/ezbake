# ezbake

`ezbake` is a single-binary service discovery server for your Kubernetes cluster
that makes client-side load balancing and traffic routing for HTTP and GRPC
services easy and accessible to everyone, even if you're allergic to YAML.

## Running ezbake

TKTK

## Using ezbake with HTTP clients

TKTK

## Using ezbake with GRPC

TKTK

## What ezbake discovers

### HTTP and GRPC Routing

`ezbake` doesn't require defining any routes, and by default will route all
traffic for `xds://${name}.${namespace}.svc.cluster.local` to the service named
`name` in the namespaced `namespace`.

To configure more complex routing rules, `ezbake` uses the Kubernetes Gateaway
API.  To set up routing information for all clients of a service, define an
`HTTPRoute` or a `GRPCRoute` for a Service in the same namespace the `Service`
object is defined in.

For example, if we wanted to send all traffic to
`xds://cool-service.cool-user.svc.cluster.local/v2/` to `cooler-service`
instead, we could define a rule like:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  namespace: cool-user
  name: cool-route
spec:
  parentRefs:
  - name: cool-service
    kind: Service
    group: ""
  - name: cooler-service
    kind: Service
    group: ""
  rules:
  - matches:
    - path:
      value: "/v2/"
    backendRefs:
    - name: cooler-service
      port: 8080
  - backendRefs:
    - name: cool-service
      port: 8080
```

### HTTP (and GRPC) routing, mirroring, and traffic splits
