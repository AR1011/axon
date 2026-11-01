/// A traffic class between two nodes: each lane has a connection of its own, so a large transfer on one does not hold
/// up the frames of another. On the wire `Control` is 0, `Live` 1 and `Bulk` 2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize), serde(rename_all = "lowercase"))]
pub enum Lane {
    /// Membership, keepalives, leases, the cluster's own questions: small frames that decide reachability.
    Control,
    /// Calls and stream pushes.
    Live,
    /// Large transfers that may wait behind each other.
    Bulk,
}

impl Lane {
    pub const ALL: [Lane; 3] = [Lane::Control, Lane::Live, Lane::Bulk];

    /// Its name in metric labels, the console and JSON: `control`, `live`, `bulk`.
    pub const fn name(self) -> &'static str {
        match self {
            Lane::Control => "control",
            Lane::Live => "live",
            Lane::Bulk => "bulk",
        }
    }

    /// The byte that names it on the wire.
    pub const fn byte(self) -> u8 {
        self as u8
    }

    /// The lane a wire byte names, `None` for any other byte.
    pub const fn from_byte(byte: u8) -> Option<Lane> {
        match byte {
            0 => Some(Lane::Control),
            1 => Some(Lane::Live),
            2 => Some(Lane::Bulk),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Lane;

    #[test]
    fn each_lane_is_the_byte_that_names_it() {
        assert_eq!(Lane::ALL.map(Lane::byte), [0, 1, 2]);
        assert_eq!((0..=255).filter_map(Lane::from_byte).collect::<Vec<_>>(), Lane::ALL);
    }
}
