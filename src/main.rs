use std::time::Duration;

use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use tonic::transport::Server;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
use xds::TypedWriters;
use xds_api::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer;

mod ingest;
mod k8s;
mod xds;

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
    let (snapshot, writers) = xds::new_snapshot();
    ingest(client, writers);

    let ads_server = xds::AdsServer::new(snapshot);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(xds_api::FILE_DESCRIPTOR_SET)
        .with_service_name("envoy.service.discovery.v3.AggregatedDiscoveryService")
        .with_service_name("envoy.service.status.v3.ClientStatusDiscoveryService")
        .build()
        .unwrap();

    Server::builder()
        .layer(grpc_access::layer!())
        .add_service(AggregatedDiscoveryServiceServer::new(ads_server))
        .add_service(reflection)
        .serve("127.0.0.1:8008".parse().unwrap())
        .await
        .unwrap()
}

fn ingest(client: kube::Client, mut writers: TypedWriters) {
    let (svc_store, svc_watch, run_svc_watch) =
        k8s::watch::<Service>(client.clone(), Duration::from_secs(2));
    let (slice_store, slice_watch, run_slice_watch) =
        k8s::watch::<EndpointSlice>(client, Duration::from_secs(2));

    // LDS ingest
    tokio::spawn({
        let writer = writers
            .for_type(xds::ResourceType::Listener)
            .expect("Listener writer used twice");

        let listeners = ingest::Listeners::new(writer, svc_store.clone(), svc_watch.subscribe());

        listeners.run()
    });

    // CDS ingest
    tokio::spawn({
        let writer = writers
            .for_type(xds::ResourceType::Cluster)
            .expect("Cluster writer used twice");

        let clusters = ingest::Clusters::new(writer, svc_store.clone(), svc_watch.subscribe());

        clusters.run()
    });

    // EDS ingest
    tokio::spawn({
        let writer = writers
            .for_type(xds::ResourceType::ClusterLoadAssignment)
            .expect("ClusterLoadAssignment writer used twice");

        let clas =
            ingest::LoadAssignments::new(writer, slice_store.clone(), slice_watch.subscribe());

        clas.run()
    });

    tokio::spawn(run_svc_watch);
    tokio::spawn(run_slice_watch);
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
