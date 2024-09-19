use std::{net::SocketAddr, time::Instant};

use metrics::{describe_counter, describe_gauge, Gauge, Histogram, IntoF64};
use metrics_exporter_prometheus::PrometheusBuilder;

/// Install and start a prometheus http exporter listening on `metrics_addr` and
/// defines all metrics.
pub(crate) fn install_prom(metrics_addr: &str) -> anyhow::Result<()> {
    let metrics_addr: SocketAddr = metrics_addr.parse()?;

    // it's 2024 prometheus still makes you define your own damn buckets for
    // histograms.
    //
    // start with an exponential series of bounds, starting at 250 micros and
    // see how it goes.
    const US_PER_SEC: f64 = 1000000.0;
    let buckets: Vec<f64> = (0..16)
        .map(|i| (2u32.pow(i) as f64) * 250.0 / US_PER_SEC)
        .collect();

    let builder = PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .set_buckets(&buckets)
        .expect("invalid bucket settings. this is a bug");
    builder.install()?;

    describe_metrics();

    Ok(())
}

// TODO: This sucks, metrics are defined all over the place. Make metric names
// and/or values module-level variables with actual symbols or even just pass
// the Arc around.
fn describe_metrics() {
    describe_timer!(
        "ingest_time",
        "Time to convert a batch of Kubernetes resources to xDS (seconds)",
    );
    describe_gauge!(
        "ads.active_connections",
        "The number of currently active ADS connections",
    );
    describe_counter!("ads.rx", "The total number of ADS messages sent");
    describe_counter!("ads.tx", "The total number of ADS messages received");
}

/// Increments a gauge by the given amount, then decrements it when the returned
/// guard goes out of scope.
///
/// ```no_run
/// // the gauge starts at zero (or undefined!)
/// {
///   // increment the gauge to 2 for the duration of this block
///   let _gauge = scoped_gauge!("my_cool_gauge", 2);
///   do_cool_stuff();
///   do_more_cool_stuff();
/// }
///  // after the gage drops, the guard resets to zero
/// ```
macro_rules! scoped_gauge {
    ($name:expr) => {
        crate::metrics::inc_gauge!($name, 1)
    };
    ($name:expr, $inc:expr) => {{
        let g = ::metrics::gauge!($name);
        g.increment($inc);
        crate::metrics::IncGuard::new(g, -$inc)
    }};
}
pub(crate) use scoped_gauge;

/// Describe a timer. Shorthand for `describe_histogram!(name, Unit::Seconds,
/// description)` so you don't have to remember what units timers are in.
macro_rules! describe_timer {
    ($name:expr, $description:expr $(,)?) => {{
        ::metrics::describe_histogram!($name, ::metrics::Unit::Seconds, $description)
    }};
}
pub(crate) use describe_timer;

/// Creates a timer that runs until it goes out of scope. Timed values are
/// tracked with a metrics histogram and assumes that durations are recorded as
/// an f64 number of seconds.
macro_rules! scoped_timer {
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        let hist = ::metrics::histogram!($name $(, $label_key $(=> $label_value)?)*);
        crate::metrics::TimerGuard::new_at(hist, std::time::Instant::now())
    }};
}
pub(crate) use scoped_timer;

/// An RAII guard that decrements a gauge on drop.
///
/// Created with [inc_gauge].
pub(crate) struct IncGuard {
    gauge: Gauge,
    value: f64,
}

impl IncGuard {
    pub(crate) fn new<T: IntoF64>(gauge: Gauge, value: T) -> Self {
        let value = value.into_f64();
        Self { gauge, value }
    }
}

impl Drop for IncGuard {
    fn drop(&mut self) {
        self.gauge.increment(self.value);
    }
}

/// An RAII timer guard that records its duration on drop.
///
/// Created with [time_scope].
pub(crate) struct TimerGuard {
    hist: Histogram,
    started_at: Instant,
}

impl TimerGuard {
    pub(crate) fn new_at(hist: Histogram, started_at: Instant) -> Self {
        Self { hist, started_at }
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        self.hist.record(self.started_at.elapsed().as_secs_f64());
    }
}
