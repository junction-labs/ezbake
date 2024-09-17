use std::{future::Future, time::Duration};

use clap::{Args, Parser};
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

// TODO: multi-cluster
// TODO: actually figure out metrics/logs/etc. would be nice to have a flag that
//       dumps XDS requests in a chrome trace format or something.

/// an ez service discovery server
#[derive(Parser, Debug)]
#[command(version)]
struct CliArgs {
    /// The local address to listen on.
    #[arg(long, short, default_value = "127.0.0.1:8008")]
    listen_addr: String,

    #[command(flatten)]
    namespace_args: NamespaceArgs,
}

#[derive(Args, Debug)]
#[group(multiple = false)]
struct NamespaceArgs {
    /// Watch all namespaces. Defaults to false.
    ///
    /// It's an error to set both --all-namespaces and --namespace.
    #[arg(long)]
    all_namespaces: bool,

    /// The namespace to watch. If this option is not set explicitly, ezbake
    /// will watch the the namespace set in the kubeconfig's s current context,
    /// the namespace specified by the service account the server is running as,
    /// or the `default` namespace.
    ///
    /// It's an error to set both --all-namespaces and --namespace.
    #[arg(long)]
    namespace: Option<String>,
}

#[tokio::main]
async fn main() {
    let default_log_filter = "ezbake=info"
        .parse()
        .expect("default log filter must be valid");

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(default_log_filter)
                .from_env_lossy(),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_target(true)
        .init();

    let client = kube::Client::try_default().await.unwrap();

    let args = CliArgs::parse();
    let (snapshot, writers) = xds::new_snapshot();

    let ingest = ingest(
        &client,
        args.namespace_args.all_namespaces,
        args.namespace_args.namespace.as_deref(),
        writers,
    );
    let serve = serve(&args.listen_addr, snapshot);

    if let Err(e) = tokio::try_join!(ingest, serve) {
        tracing::error!(err = ?e, "exiting: {e}");
        std::process::exit(1);
    }
}

async fn serve(addr: &str, snapshot: xds::Snapshot) -> anyhow::Result<()> {
    let ads_server = xds::AdsServer::new(snapshot);
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(xds_api::FILE_DESCRIPTOR_SET)
        .with_service_name("envoy.service.discovery.v3.AggregatedDiscoveryService")
        .with_service_name("envoy.service.listener.v3.ListenerDiscoveryService")
        .with_service_name("envoy.service.route.v3.RouteDiscoveryService")
        .with_service_name("envoy.service.cluster.v3.ClusterDiscoveryService")
        .with_service_name("envoy.service.endpoint.v3.EndpointDiscoveryService")
        .build()?;

    let server = Server::builder()
        .layer(grpc_access::layer!())
        .add_service(AggregatedDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(ListenerDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(RouteDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(ClusterDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(EndpointDiscoveryServiceServer::new(ads_server.clone()))
        .add_service(reflection)
        .serve(addr.parse()?);

    server.await?;
    Ok(())
}

async fn ingest(
    client: &kube::Client,
    all_namespaces: bool,
    namespace: Option<&str>,
    mut writers: TypedWriters,
) -> anyhow::Result<()> {
    // watch Gateway API routes
    //
    // the watches here need a little bit of extra error handling, in case the APIs
    // are not installed, installed at an incompatible version, or someone removes
    // a CRD at a weird time.
    let (route_watch, run_route_watch) = k8s::watch(
        kube_api(client, all_namespaces, namespace),
        Duration::from_secs(2),
    );
    let run_route_watch = async {
        match run_route_watch.await {
            Err(e) if k8s::is_api_not_found(&e) => {
                tracing::info!("HTTPRoute API not found. Continuing without Gateway APIs");
                Ok(())
            }
            v => v,
        }
    };

    // watch Services and EndpointSlices
    let (svc_watch, run_svc_watch) = k8s::watch::<Service>(
        kube_api(client, all_namespaces, namespace),
        Duration::from_secs(2),
    );
    let (slice_watch, run_slice_watch) = k8s::watch::<EndpointSlice>(
        kube_api(client, all_namespaces, namespace),
        Duration::from_secs(2),
    );

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

    tokio::try_join!(
        spawn_watch(run_route_watch),
        spawn_watch(run_slice_watch),
        spawn_watch(run_svc_watch),
    )?;

    Ok(())
}

fn kube_api<K>(client: &kube::Client, all_namespaces: bool, namespace: Option<&str>) -> kube::Api<K>
where
    K: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <K as kube::Resource>::DynamicType: Default,
{
    match (all_namespaces, namespace) {
        (true, _) => kube::Api::all(client.clone()),
        (_, Some(namespace)) => kube::Api::namespaced(client.clone(), &namespace),
        _ => kube::Api::default_namespaced(client.clone()),
    }
}

async fn spawn_watch<F, E>(watch: F) -> anyhow::Result<()>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let handle = tokio::spawn(watch);

    match handle.await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => Err(e.into()),
    }
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
