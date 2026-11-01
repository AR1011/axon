use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use axon::trace::mix;

use crate::discovery::NodeRecord;
use crate::membership::{Member, View};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Strategy {
    /// Any eligible node, chosen at random.
    #[default]
    Any,
    /// Probes two random eligible nodes and takes the one running fewer actors.
    LeastLoaded,
    /// Round-robin over eligible nodes, per calling node.
    Spread,
}

/// Which nodes may host an actor. Labels are matched against node metadata. Keyed spawns
/// ignore `strategy`: the owner is always the rendezvous winner, so every node agrees on it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Placement {
    pub required: BTreeMap<String, String>,
    /// Among eligible nodes, those matching the most preferred labels win.
    pub preferred: BTreeMap<String, String>,
    pub strategy: Strategy,
}

impl Placement {
    pub fn require(mut self, label: impl Into<String>, value: impl Into<String>) -> Self {
        self.required.insert(label.into(), value.into());
        self
    }

    pub fn prefer(mut self, label: impl Into<String>, value: impl Into<String>) -> Self {
        self.preferred.insert(label.into(), value.into());
        self
    }

    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Whether `node` carries every `required` label.
    pub(crate) fn admits(&self, node: &NodeRecord) -> bool {
        node.has_labels(&self.required)
    }

    /// Available members advertising `kind` that satisfy the labels. Of those, only the ones that serve the most of
    /// the schemas the others' own callers use: in a rolling upgrade, the nodes that serve the old schema beside the
    /// new one, so every caller reaches a key placed there. Every node reads the same view, so all pick the same.
    pub fn candidates<'a>(&self, view: &'a View, kind: &str) -> Vec<&'a Member> {
        let preferred =
            |m: &Member| self.preferred.iter().filter(|(k, v)| m.record.label(k) == Some(v.as_str())).count();
        let eligible: Vec<_> = view.admitted(self).filter_map(|m| Some((m, m.record.kind_schemas(kind)?))).collect();
        let own: BTreeSet<u16> = eligible.iter().filter_map(|(_, schemas)| schemas.first().copied()).collect();
        let spoken = |schemas: &[u16]| own.iter().filter(|s| schemas.contains(s)).count();
        let most = eligible.iter().map(|(_, schemas)| spoken(schemas)).max();
        let eligible: Vec<_> = eligible.iter().filter(|(_, s)| Some(spoken(s)) == most).map(|(m, _)| *m).collect();
        let best = eligible.iter().map(|m| preferred(m)).max().unwrap_or(0);
        eligible.into_iter().filter(|m| preferred(m) == best).collect()
    }
}

/// The candidates of the first kinds and placements owners are looked up for in one view, each worked out once. Read
/// without a lock or a count: every keyed start looks up its owner. A clone starts empty, since a view is cloned only
/// to be changed.
#[derive(Default)]
pub(crate) struct Candidates([OnceLock<Known>; 4]);

impl std::fmt::Debug for Candidates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Candidates(..)")
    }
}

/// A kind, a placement and its candidates' node ids.
type Known = (String, Placement, Box<[Box<str>]>);

impl Clone for Candidates {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl View {
    /// [`rendezvous`] over [`Placement::candidates`].
    pub(crate) fn owner(&self, kind: &str, key: &str, placement: &Placement) -> Option<&str> {
        self.owner_past(kind, key, placement, &[])
    }

    /// The owner were the nodes `passed` gone: the next of the rendezvous order, which a keyed start falls through.
    pub(crate) fn owner_past(&self, kind: &str, key: &str, placement: &Placement, passed: &[String]) -> Option<&str> {
        let left = |node: &&str| !passed.iter().any(|p| p == node);
        match self.known(kind, placement) {
            Some(nodes) => winner(kind, key, nodes.iter().map(|n| &**n).filter(left)),
            None => winner(
                kind,
                key,
                placement.candidates(self, kind).into_iter().map(|m| m.record.id.as_str()).filter(left),
            ),
        }
    }

    /// The members advertising `kind` that this view finds unavailable and that would rank ahead of `node` for the
    /// key: a node whose view still has them skips them for this caller.
    pub(crate) fn unavailable_ahead(&self, kind: &str, key: &str, node: &str) -> Vec<String> {
        let prefix = prefix(kind, key);
        let at = rank(prefix, node);
        (self.members.iter())
            .filter(|m| !m.available() && rank(prefix, &m.record.id) > at && m.record.has_kind(kind))
            .map(|m| m.record.id.clone())
            .collect()
    }

    /// The candidates' node ids, worked out once per view for the first kinds and placements asked.
    fn known(&self, kind: &str, placement: &Placement) -> Option<&[Box<str>]> {
        let ids = || placement.candidates(self, kind).iter().map(|m| m.record.id.as_str().into()).collect();
        for slot in &self.candidates.0 {
            let (k, p, nodes) = slot.get_or_init(|| (kind.to_owned(), placement.clone(), ids()));
            // Keyed placement ignores the strategy, so it does not take a slot of its own.
            if k == kind && p.required == placement.required && p.preferred == placement.preferred {
                return Some(nodes);
            }
        }
        // ponytail: past four kinds or placements in one view they are worked out on every lookup.
        None
    }
}

/// [`hash`] of `kind`, `key` and a node id, in two steps: FNV reads a byte at a time, so the kind and key are hashed
/// once for all the nodes.
fn prefix(kind: &str, key: &str) -> u64 {
    fnv(FNV_OFFSET, kind.bytes().chain([0]).chain(key.bytes()).chain([0]))
}

fn score(prefix: u64, node: &str) -> u64 {
    mix(fnv(prefix, node.bytes()))
}

/// FNV-1a from `seed`, finished with `mix`, as FNV's high bits mix poorly: the same on every node, whatever the process.
pub(crate) fn hash(seed: u64, bytes: impl Iterator<Item = u8>) -> u64 {
    mix(fnv(FNV_OFFSET ^ seed, bytes))
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a's rounds from `h`, unfinished.
pub(crate) fn fnv(mut h: u64, bytes: impl Iterator<Item = u8>) -> u64 {
    for byte in bytes {
        h = (h ^ byte as u64).wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Highest-random-weight owner: order-independent, and a membership change moves only the
/// keys whose winner changed.
pub fn rendezvous<'a>(kind: &str, key: &str, candidates: &[&'a Member]) -> Option<&'a Member> {
    let prefix = prefix(kind, key);
    candidates.iter().copied().max_by_key(|m| rank(prefix, &m.record.id))
}

/// [`rendezvous`]'s winner among node ids.
pub(crate) fn winner<'a>(kind: &str, key: &str, nodes: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let prefix = prefix(kind, key);
    nodes.max_by_key(|node| rank(prefix, node))
}

/// Every node ranked for `key`, first choice first: [`winner`]'s order for no kind, so a node leaving moves only the
/// keys it led.
pub(crate) fn order<'a>(key: &str, nodes: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let prefix = prefix("", key);
    let mut nodes: Vec<_> = nodes.collect();
    nodes.sort_by_cached_key(|node| std::cmp::Reverse(rank(prefix, node)));
    nodes
}

/// A node's place in a key's order, highest first; the id breaks a tie.
fn rank(prefix: u64, node: &str) -> (u64, &str) {
    (score(prefix, node), node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{KINDS, NodeRecord};

    fn view(nodes: &[(&str, &str)]) -> View {
        let members = nodes.iter().map(|(id, region)| {
            let metadata = [(KINDS.into(), "a,pricer".into()), ("region".into(), region.to_string())].into();
            let record = NodeRecord {
                id: id.to_string(),
                incarnation: 1,
                address: "127.0.0.1:1".parse().unwrap(),
                metadata,
                version: 0,
            };
            Member { record, reachable: true }
        });
        View { members: members.collect(), ..View::default() }
    }

    #[test]
    fn labels_filter_and_rendezvous_is_stable() {
        let full = view(&[("a", "eu"), ("b", "eu"), ("c", "us"), ("d", "us")]);
        let eu = Placement::default().require("region", "eu");
        assert_eq!(eu.candidates(&full, "pricer").len(), 2);
        assert!(eu.candidates(&full, "other").is_empty());
        assert_eq!(Placement::default().prefer("region", "us").candidates(&full, "pricer").len(), 2);
        assert_eq!(Placement::default().prefer("region", "mars").candidates(&full, "pricer").len(), 4);

        let any = Placement::default();
        let smaller = view(&[("a", "eu"), ("b", "eu"), ("c", "us")]);
        let (mut moved, mut per_node) = (0, std::collections::BTreeMap::<_, u32>::new());
        for i in 0..4000 {
            let key = format!("key-{i}");
            let before = &rendezvous("pricer", &key, &any.candidates(&full, "pricer")).unwrap().record.id;
            let after = &rendezvous("pricer", &key, &any.candidates(&smaller, "pricer")).unwrap().record.id;
            *per_node.entry(before.clone()).or_default() += 1;
            moved += (before != after) as u32;
            assert!(before == after || before == "d", "only the removed node's keys move");
        }
        assert!(per_node.values().all(|n| (800..1200).contains(n)), "{per_node:?}");
        assert!((800..1200).contains(&moved));
    }

    #[test]
    fn only_nodes_serving_every_callers_schema_of_a_kind_are_candidates() {
        let ids = |kinds: [&str; 3], kind| {
            let mut rolling = view(&[("a", "eu"), ("b", "eu"), ("c", "us")]);
            for (member, kinds) in rolling.members.iter_mut().zip(kinds) {
                member.record.metadata.insert(KINDS.into(), kinds.into());
            }
            Placement::default().candidates(&rolling, kind).iter().map(|m| m.record.id.clone()).collect::<Vec<_>>()
        };
        // b's callers use 2 and it serves 1 beside it: only b answers both.
        let one_step = ["a/1,pricer/1", "pricer/2+1", "pricer/1"];
        assert_eq!(ids(one_step, "pricer"), ["b"]);
        assert_eq!(ids(one_step, "a"), ["a"]);
        // b serves 2 before any caller uses it, so keys stay spread; then callers move to 2 where both are served.
        assert_eq!(ids(["pricer/1", "pricer/1+2", "pricer/1"], "pricer"), ["a", "b", "c"]);
        assert_eq!(ids(["pricer/1+2", "pricer/2+1", "pricer/1+2"], "pricer"), ["a", "b", "c"]);
        // Without `also`, neither build answers the other's callers: both stay candidates.
        assert_eq!(ids(["pricer/1", "pricer/2", "pricer/1"], "pricer"), ["a", "b", "c"]);
    }

    #[test]
    fn a_view_places_every_key_as_its_candidates_do_and_a_changed_copy_works_them_out_again() {
        let placements = [
            Placement::default(),
            Placement::default().require("region", "eu"),
            Placement::default().require("region", "eu").strategy(Strategy::Spread),
            Placement::default().require("region", "us"),
            Placement::default().prefer("region", "us"),
            Placement::default().prefer("region", "mars"),
            Placement::default().require("region", "mars"),
        ];
        let agree = |view: &View| {
            for key in (0..100).map(|i| format!("key-{i}")) {
                for (kind, placement) in
                    ["pricer", "a", "other"].iter().flat_map(|k| placements.iter().map(move |p| (k, p)))
                {
                    let fresh = rendezvous(kind, &key, &placement.candidates(view, kind)).map(|m| m.record.id.as_str());
                    assert_eq!(view.owner(kind, &key, placement), fresh, "{kind}/{key} under {placement:?}");
                }
            }
        };
        let full = view(&[("a", "eu"), ("b", "eu"), ("c", "us"), ("d", "us")]);
        agree(&full);
        let mut without_d = full.clone();
        without_d.members[3].reachable = false;
        agree(&without_d);
        assert!((0..100).any(|i| full.owner("pricer", &format!("key-{i}"), &placements[0]) == Some("d")));
        // The owner past the nodes ahead of it is the owner once they are gone, and a caller that finds them gone
        // names them to the node it asks.
        for key in (0..100).map(|i| format!("key-{i}")) {
            let (mut fewer, mut passed) = (full.clone(), Vec::new());
            while let Some(node) = full.owner_past("pricer", &key, &placements[0], &passed).map(String::from) {
                assert_eq!(fewer.owner("pricer", &key, &placements[0]), Some(node.as_str()), "{key}: {passed:?}");
                let (mut ahead, mut gone) = (fewer.unavailable_ahead("pricer", &key, &node), passed.clone());
                ahead.sort();
                gone.sort();
                assert_eq!(ahead, gone, "{key}");
                fewer.members.iter_mut().find(|m| m.record.id == node).expect("a member").reachable = false;
                // A clone works the candidates out again.
                fewer = fewer.clone();
                passed.push(node);
            }
            assert_eq!(passed.len(), 4, "{key}");
        }
    }

    #[test]
    fn a_node_set_orders_a_key_alike_everywhere_and_a_leaver_moves_only_its_keys() {
        let (four, backwards, three) = (["a", "b", "c", "d"], ["d", "c", "b", "a"], ["a", "b", "c"]);
        let mut heads = BTreeMap::<&str, u32>::new();
        for key in (0..4000).map(|i| format!("key-{i}")) {
            let full = order(&key, four.into_iter());
            assert_eq!(full, order(&key, backwards.into_iter()), "{key}: the members' order changed it");
            *heads.entry(full[0]).or_default() += 1;
            let without_d: Vec<_> = full.iter().copied().filter(|&n| n != "d").collect();
            assert_eq!(order(&key, three.into_iter()), without_d, "{key}: d leaving moved another node");
        }
        assert!(heads.values().all(|n| (800..1200).contains(n)), "{heads:?}");
        assert_eq!(order("key", std::iter::empty()), Vec::<&str>::new());
    }

    #[test]
    fn every_version_scores_a_key_alike() {
        let score = score(prefix("account", "42"), "n1");
        assert_eq!(score, 0x4e53_4851_142c_5769, "nodes of two versions would place it apart");
        assert_eq!(score, hash(0, "account\x0042\x00n1".bytes()));
    }
}
