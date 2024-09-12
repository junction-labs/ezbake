use std::time::Duration;

use args::{Args, IngestScope};
use clap::Parser;
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use tonic::transport::Server;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
use xds::TypedWriters;
use xds_api::pb::envoy::service::{
    cluster::v3::cluster_discovery_service_server::ClusterDiscoveryServiceServer,
    discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer,
    endpoint::v3::endpoint_discovery_service_server::EndpointDiscoveryServiceServer,
    listener::v3::listener_discovery_service_server::ListenerDiscoveryServiceServer,
    route::v3::route_discovery_service_server::RouteDiscoveryServiceServer,
};

mod ingest;
mod k8s;
mod xds;
mod args;

// TODO: figure out multi-cluster?
// TODO: add HTTPRoute stuff
// TODO: actually figure out metrics/logs/etc. would be nice to have a flag that
//       dumps XDS requests in a chrome trace format or something.

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .with_target(true)
        .init();

    let client = kube::Client::try_default().await.unwrap();

    let args = Args::parse();
    let (snapshot, writers) = xds::new_snapshot();
    ingest(&client, &args.ingest_scope, writers);

    let ads_server = xds::AdsServer::new(snapshot);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(xds_api::FILE_DESCRIPTOR_SET)
        .with_service_name("envoy.service.discovery.v3.AggregatedDiscoveryService")
        .with_service_name("envoy.service.listener.v3.ListenerDiscoveryService")
        .with_service_name("envoy.service.route.v3.RouteDiscoveryService")
        .with_service_name("envoy.service.cluster.v3.ClusterDiscoveryService")
        .with_service_name("envoy.service.endpoint.v3.EndpointDiscoveryService")
        .build()
        .unwrap();

    Server::builder()
        .layer(grpc_access::layer!())
        .add_service(AggregatedDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(ListenerDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(RouteDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(ClusterDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(EndpointDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(reflection)
        .serve("0.0.0.0:8008".parse().unwrap())
        .await
        .unwrap()
}


fn get_k8s_resource_api<K>(
    client: &kube::Client, 
    scope: &IngestScope) 
-> kube::Api<K>
where
    K: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <K as kube::Resource>::DynamicType: Default,
{
    match scope {
        IngestScope::Cluster => kube::Api::all(client.clone()),
        IngestScope::DefaultNamespace => kube::Api::default_namespaced(client.clone()),
        //IngestScope::NamedNamespace(s) => kube::Api::namespaced(client.clone(), &s),
    }
}


fn ingest(client: &kube::Client, scope: &IngestScope, mut writers: TypedWriters) {

    let (route_watch, run_route_watch) = 
        k8s::watch(get_k8s_resource_api(client, scope), Duration::from_secs(2));
    let (svc_watch, run_svc_watch) = 
        k8s::watch::<Service>(get_k8s_resource_api(client, scope), Duration::from_secs(2));
    let (slice_watch, run_slice_watch) =
        k8s::watch::<EndpointSlice>(get_k8s_resource_api(client, scope), Duration::from_secs(2));

    // LDS ingest
    tokio::spawn({
        let writer = writers
            .for_type(xds::ResourceType::Listener)
            .expect("Listener writer used twice");

        let listeners = ingest::Listeners::new(writer, &svc_watch, &route_watch);
        listeners.run()
    });

    // CDS ingest
    tokio::spawn({
        let writer = writers
            .for_type(xds::ResourceType::Cluster)
            .expect("Cluster writer used twice");

        let clusters = ingest::Clusters::new(writer, &svc_watch);
        clusters.run()
    });

    // EDS ingest
    tokio::spawn({
        let writer = writers
            .for_type(xds::ResourceType::ClusterLoadAssignment)
            .expect("ClusterLoadAssignment writer used twice");

        let clas = ingest::LoadAssignments::new(writer, &slice_watch);
        clas.run()
    });

    // FIXME: do a better error handling here
    // cases to handle:
    //   intermittent faiulre of API
    //   gateway API CRD not being installed.
    tokio::spawn(async { 
        run_route_watch.await.unwrap() 
    });
    tokio::spawn(async { 
        run_svc_watch.await.unwrap() 
    });
    tokio::spawn(async { 
        run_slice_watch.await.unwrap() 
    });
}

pub(crate) mod grpc_access {
    use std::time::Duration;

    use tower_http::classify::GrpcFailureClass;
    use tracing::{info_span, Span};

    macro_rules! layer {
        () => {
            tower_http::trace::TraceLayer::new_for_grpc()
                .make_span_with(crate::grpc_access::make_span)
                .on_request(crate::grpc_access::on_request)
                .on_response(crate::grpc_access::on_response)
                .on_eos(crate::grpc_access::on_eos)
                .on_failure(crate::grpc_access::on_failure)
        };
    }
    pub(crate) use layer;

    pub(crate) fn make_span(request: &http::Request<tonic::transport::Body>) -> Span {
        info_span!(
            "grpc-access",
            method = %request.method(),
            uri = %request.uri(),
            version = ?request.version()
        )
    }

    pub(crate) fn on_request(_request: &http::Request<tonic::transport::Body>, _span: &Span) {
        tracing::info!("request-recieved")
    }

    pub(crate) fn on_response<B>(_response: &http::Response<B>, latency: Duration, _span: &Span) {
        let latency_us = latency.as_micros();
        tracing::info!(%latency_us, "response-sent")
    }

    pub(crate) fn on_eos(
        trailers: Option<&http::HeaderMap>,
        stream_duration: Duration,
        _span: &Span,
    ) {
        let status_code = trailers
            .and_then(tonic::Status::from_header_map)
            .map(|status| status.code());

        let duration_us = stream_duration.as_micros();
        tracing::info!(?status_code, %duration_us, "end-of-stream")
    }

    pub(crate) fn on_failure(
        failure_classification: GrpcFailureClass,
        latency: Duration,
        _span: &Span,
    ) {
        let duration_us = latency.as_micros();
        tracing::info!(
            classification = %failure_classification,
            %duration_us,
            "failed"
        )
    }
}
