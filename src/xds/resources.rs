use xds_api::WellKnownTypes;

#[derive(Clone, Copy, Debug, PartialEq, Eq, enum_map::Enum)]
pub(crate) enum ResourceType {
    Listener,
    RouteConfiguration,
    Cluster,
    ClusterLoadAssignment,
}

impl ResourceType {
    fn from_wkt(wkt: WellKnownTypes) -> Option<Self> {
        match wkt {
            WellKnownTypes::Listener => Some(Self::Listener),
            WellKnownTypes::RouteConfiguration => Some(Self::RouteConfiguration),
            WellKnownTypes::Cluster => Some(Self::Cluster),
            WellKnownTypes::ClusterLoadAssignment => Some(Self::ClusterLoadAssignment),
            _ => None,
        }
    }

    fn as_wkt(&self) -> WellKnownTypes {
        match self {
            ResourceType::Listener => WellKnownTypes::Listener,
            ResourceType::RouteConfiguration => WellKnownTypes::RouteConfiguration,
            ResourceType::Cluster => WellKnownTypes::Cluster,
            ResourceType::ClusterLoadAssignment => WellKnownTypes::ClusterLoadAssignment,
        }
    }

    #[allow(unused)]
    pub(crate) fn all() -> &'static [Self] {
        &[
            Self::Listener,
            Self::RouteConfiguration,
            Self::Cluster,
            Self::ClusterLoadAssignment,
        ]
    }

    pub(crate) fn type_url(&self) -> &'static str {
        self.as_wkt().type_url()
    }

    pub(crate) fn from_type_url(type_url: &str) -> Option<Self> {
        WellKnownTypes::from_type_url(type_url).and_then(Self::from_wkt)
    }

    pub(crate) fn group_responses(&self) -> bool {
        matches!(self, ResourceType::Listener | ResourceType::Cluster)
    }
}
