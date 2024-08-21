use prost::Name;

use xds_api::pb::envoy::config::cluster::v3 as xds_cluster;
use xds_api::pb::envoy::config::endpoint::v3 as xds_endpoint;
use xds_api::pb::envoy::config::listener::v3 as xds_listener;
use xds_api::pb::envoy::config::route::v3 as xds_route;

macro_rules! xds_types {
    (enum $name:ident { $($variant:ident => $xds_type:ty),* $(,)* }) => {
        #[derive(Clone, Copy, Debug, enum_map::Enum)]
        pub(crate) enum $name {
            $(
                $variant,
            )*
        }

        impl $name {
            pub fn all() -> &'static [$name] {
                &[
                    $(
                        $name::$variant,
                    )*
                ]
            }

            pub fn type_url(&self) -> &'static str {
                static TO_TYPE_URL: once_cell::sync::Lazy<enum_map::EnumMap<$name, String>> = once_cell::sync::Lazy::new(|| {
                    enum_map::enum_map! {
                        $(
                            $name::$variant => <$xds_type>::type_url(),
                        )*
                    }
                });

                TO_TYPE_URL[*self].as_str()
            }

            pub fn from_type_url(type_url: &str) -> Option<Self> {
                static FROM_TYPE_URL: once_cell::sync::Lazy<Box<[(String, $name)]>> = once_cell::sync::Lazy::new(|| {
                    let urls = vec![
                        $(
                            (<$xds_type>::type_url(), $name::$variant),
                        )*
                    ];
                    urls.into_boxed_slice()
                });

                FROM_TYPE_URL.iter().find(|(k, _)| k == type_url).map(|(_, v)| *v)
            }
        }
    };
}

xds_types! {
    enum ResourceType {
        Listener => xds_listener::Listener,
        RouteConfiguration => xds_route::RouteConfiguration,
        Cluster => xds_cluster::Cluster,
        ClusterLoadAssignment => xds_endpoint::ClusterLoadAssignment,
    }
}

impl ResourceType {
    pub(crate) fn group_responses(&self) -> bool {
        matches!(self, ResourceType::Listener | ResourceType::Cluster)
    }
}
