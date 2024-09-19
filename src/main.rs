use std::{future::Future, time::Duration};

use clap::{Args, Parser};
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use tonic::{server::NamedService, transport::Server};
use tracing_subscriber::EnvFilter;
use xds::{AdsServer, TypedWriters};
use xds_api::pb::envoy::service::{
    cluster::v3::cluster_discovery_service_server::ClusterDiscoveryServiceServer,
    discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer,
    endpoint::v3::endpoint_discovery_service_server::EndpointDiscoveryServiceServer,
    listener::v3::listener_discovery_service_server::ListenerDiscoveryServiceServer,
    route::v3::route_discovery_service_server::RouteDiscoveryServiceServer,
    status::v3::client_status_discovery_service_server::ClientStatusDiscoveryServiceServer,
};

mod grpc_access;
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
    /// Log in a pretty, human-readable format.
    #[arg(long)]
    log_pretty: bool,

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
    let args = CliArgs::parse();
    setup_tracing(args.log_pretty);

    let client = kube::Client::try_default().await.unwrap();
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

fn setup_tracing(log_pretty: bool) {
    let default_log_filter = "ezbake=info"
        .parse()
        .expect("default log filter must be valid");
    let builder = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(default_log_filter)
                .from_env_lossy(),
        )
        .with_target(true);

    if log_pretty {
        // don't use .pretty(), it's too pretty
        builder.init();
    } else {
        builder
            .json()
            .flatten_event(true)
            // TODO: we're not really emitting events from deeply nested spans
            // often, and the span list is redundant with the current span.
            // omit it for now.
            .with_span_list(false)
            .init();
    }
}

async fn serve(addr: &str, snapshot: xds::Snapshot) -> anyhow::Result<()> {
    // tonic server structs have a ::NAME string that we register with
    // the reflection server so that reflection only shows what we're
    // implementing, instead of EVERY single xDS api.
    //
    // for whatever reason, this means that we have to explicitly re-register
    // the reflection service name. BUT we can't refer to the type without
    // knowing the generic, which is hidden, so we can't call ::NAME on the
    // reflection service.
    macro_rules! server_with_reflection {
        ($ads_server:expr => [$($service_type:tt),* $(,)?] $(,)?) => {{
            let reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(xds_api::FILE_DESCRIPTOR_SET)
                .with_service_name("grpc.reflection.v1alpha.ServerReflection");

            let mut server = Server::builder().layer(grpc_access::layer!());

            $(
                let svc = $service_type::new($ads_server.clone());
                let reflection = reflection.with_service_name($service_type::<AdsServer>::NAME);
                let server = server.add_service(svc);
            )*

            let server = server.add_service(reflection.build()?);
            server
        }};
    }

    let ads = xds::AdsServer::new(snapshot);
    let server = server_with_reflection!(
        ads => [
            AggregatedDiscoveryServiceServer,
            ListenerDiscoveryServiceServer,
            RouteDiscoveryServiceServer,
            ClusterDiscoveryServiceServer,
            EndpointDiscoveryServiceServer,
            ClientStatusDiscoveryServiceServer,
        ],
    );
    let server = server.serve(addr.parse()?);

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
        (_, Some(namespace)) => kube::Api::namespaced(client.clone(), namespace),
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
