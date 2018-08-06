// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-afg.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-afg. If not, see <http://www.gnu.org/licenses/>.

//! Finality gadget for blockchains.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Add;

/// A prevote for a block and its ancestors.
pub struct Prevote<H> {
	round: u64,
	target: H,
	weight: usize,
}

/// A precommit for a block and its ancestors.
pub struct Precommit<H> {
	round: u64,
	target: H,
	weight: usize,
}

/// Chain context necessary for implementation of the finality gadget.
pub trait Chain<H> {
	/// Compute block to target for finality, which is some fraction (rounding up) of the way between
	/// the base and the best descendent of that head.
	fn target(&self, base: H) -> H;

	/// Get the ancestry of a block up to but not including the base hash.
	/// Should be in reverse order from `block`'s parent.
	fn ancestry(&self, base: H, block: H) -> Option<Vec<H>>;

	/// Get children of block.
	fn children(&self, block: H) -> Vec<H>;
}

struct Entry<H> {
	number: usize,
	// ancestor hashes in reverse order, e.g. ancestors[0] is the parent
	// and the last entry is the hash of the parent vote-node.
	ancestors: Vec<H>,
	descendents: Vec<H>, // descendent vote-nodes
	cumulative_weight: usize,
}

impl<H: Hash + PartialEq + Clone> Entry<H> {
	// whether the given hash, number pair is a direct ancestor of this node.
	// `None` signifies that the graph must be traversed further back.
	fn in_direct_ancestry(&self, hash: &H, number: usize) -> Option<bool> {
		if number >= self.number { return None }
		let offset = self.number - number - 1;

		self.ancestors.get(offset).map(|h| h == hash)
	}

	// get ancestor vote-node.
	fn ancestor_node(&self) -> Option<H> {
		self.ancestors.last().map(|x| x.clone())
	}
}

struct GhostTracker<H: Hash + Eq> {
	entries: HashMap<H, Entry<H>>,
	heads: HashSet<H>,
	base: H,
	threshold: usize,
	ghost_leader: Option<(H, usize)>,
}

impl<H: Hash + Eq + Clone> GhostTracker<H> {
	fn new(base_hash: H, base_number: usize, threshold: usize) {
		let mut entries = HashMap::new();
		entries.insert(base_hash.clone(), Entry {
			base_number,
			ancestors: Vec::new(),
			descendents: Vec::new(),
			cumulative_weight: 0,
		});

		let mut heads = HashSet::new();
		heads.insert(base_hash.clone());

		GhostTracker {
			entries,
			heads,
			base: base_hash,
			threshold,
			ghost_leader: None,
		}
	}

	fn insert<C: Chain<H>>(&mut self, vote_target: (H, usize), weight: usize, chain: &C) {
		let (vote_hash, vote_number) = vote_target;

		match self.find_containing_node(vote_hash.clone(), vote_number) {
			Some(key) => self.split(to_split, vote_hash.clone(), vote_number),
			None => self.append(vote_hash.clone(), vote_number)?,
		}

		// NOTE: after this point, there is guaranteed to be a node with
		// hash `vote_hash`.
	}

	// attempts to find the containing node key for the given hash and number.
	fn find_containing_node(&self, hash: Hash, number: usize) -> Option<Hash> {
		let mut containing_key = if self.entries.contains_key(&hash) {
			Some(hash.clone())
		} else {
			None
		};

		if containing_key.is_none() {
			let mut visited = HashSet::new();

			// iterate vote-heads and their ancestry backwards until we find the one with
			// this target hash in that chain.
			'a:
			for mut head in self.heads.iter() {
				let mut active_entry;

				'b:
				loop {
					active_entry = match self.entries.get(head) {
						Some(e) => e,
						None => break 'b,
					};

					if !visited.insert(head.clone()) { continue 'a }

					match active_entry.in_direct_ancestry(hash, number) {
						Some(true) => {
							// set containing node.
							containing_key = Some(head.clone());
							break 'a;
						}
						Some(false) => continue 'a, // start from a different head.
						None => match active_entry.ancestor_node() {
							None => continue 'a, // we reached the base.
							Some(prev) = { head = prev; continue 'b } // iterate backwards
						},
					}
				}
			}
		}

		containing_key
	}

	// splits a vote-node into two at the given block number. panics if there is no vote-node for
	// `to_split` or if `to_split` doesn't have a non-node ancestor with `ancestor_number`
	fn split(&mut self, to_split: Hash, ancestor_hash: Hash, ancestor_number: usize) {
		if to_split == ancestor_hash { return }

		let new_entry = {
			let entry = self.entries.get_mut(&to_split)
				.expect("this function only invoked with keys of vote-nodes; qed");

			debug_assert!(entry.in_direct_ancestry(&ancestor_hash, ancestor_number).unwrap());

			// example: splitting number 10 at ancestor 4
			// before: [9 8 7 6 5 4 3 2 1]
			// after: [9 8 7 6 5 4], [3 2 1]
			let offset = entry.number.checked_sub(ancestor_number + 1)
				.expect("this function only invoked with direct ancestors; qed");

			Entry {
				number: ancestor_number,
				ancestors: entry.ancestors.drain(offset..),
				descendents: vec![to_split.clone()],
				cumulative_weight: entry.cumulative_weight,
			}
		};

		self.entries.insert(ancestor_hash, new_entry);
	}

	// append a vote-node onto the chain-tree. This should only be called if
	// no node in the tree keeps the target anyway.
	fn append<C: Chain<H>>(&mut self, hash: Hash, number: usize, chain: &C) {
		let ancestry = match chain.ancestry(self.base.clone(), hash.clone());
	}
}
