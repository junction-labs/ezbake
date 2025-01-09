//! GRPC Access logging.
//!
//! This module groups together all of the access logging stuff we do so that
//! it's extremely easy to enable/disable together. Setting
//! `ezbake::grpc_access=LEVEL` adjusts all of the logging for ezbake - no more
//! remembering that tower_http needs to change log levels.

use std::time::Duration;

use tower_http::classify::GrpcFailureClass;
use tracing::{info_span, Span};
use xds_api::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryResponse,
};

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
use xds_api::pb::envoy::service::discovery::v3::DiscoveryRequest;

pub(crate) fn make_span(request: &http::Request<tonic::transport::Body>) -> Span {
    info_span!(
        "grpc",
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
    tracing::debug!(%latency_us, "response-sent")
}

pub(crate) fn on_eos(trailers: Option<&http::HeaderMap>, stream_duration: Duration, _span: &Span) {
    let status_code = trailers
        .and_then(tonic::Status::from_header_map)
        .map(|status| status.code());

    let duration_us = stream_duration.as_micros();
    tracing::debug!(?status_code, %duration_us, "end-of-stream")
}

pub(crate) fn on_failure(
    failure_classification: GrpcFailureClass,
    latency: Duration,
    _span: &Span,
) {
    let duration_us = latency.as_micros();
    tracing::warn!(
        classification = %failure_classification,
        %duration_us,
        "failed"
    )
}

pub(crate) fn xds_discovery_request(request: &DiscoveryRequest) {
    tracing::info!(
        v = request.version_info,
        n = request.response_nonce,
        ty = request.type_url,
        r = ?request.resource_names,
        error_code = request.error_detail.as_ref().map(|e| e.code),
        error_message = request.error_detail.as_ref().map(|e| &e.message),
        "DiscoveryRequest",
    );
}

pub(crate) fn xds_discovery_response(response: &DiscoveryResponse) {
    tracing::info!(
        v = response.version_info,
        n = response.nonce,
        ty = response.type_url,
        r_count = response.resources.len(),
        "DiscoveryResponse",
    );
}
pub(crate) fn xds_delta_discovery_request(request: &DeltaDiscoveryRequest) {
    tracing::info!(
        n = request.response_nonce,
        ty = request.type_url,
        r = ?request.resource_names_subscribe,
        init = ?request.initial_resource_versions,
        error_code = request.error_detail.as_ref().map(|e| e.code),
        error_message = request.error_detail.as_ref().map(|e| &e.message),
        "DeltaDiscoveryRequest",
    );
}

pub(crate) fn xds_delta_discovery_response(response: &DeltaDiscoveryResponse) {
    tracing::info!(
        n = response.nonce,
        ty = response.type_url,
        added = response.resources.len(),
        removed = response.removed_resources.len(),
        "DiscoveryResponse",
    );
}
