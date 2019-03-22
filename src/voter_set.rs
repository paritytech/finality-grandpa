// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-grandpa.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-grandpa. If not, see <http://www.gnu.org/licenses/>.

//! Maintains the VoterSet of the blockchain.
//!
//! See docs on `VoterSet` for more information.

use std::hash::Hash;

use crate::collections::{HashMap, Vec};
use super::threshold;

/// A voter set, with accompanying indices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoterSet<Id: Hash + Eq> {
	weights: HashMap<Id, VoterInfo>,
	voters: Vec<(Id, u64)>,
	threshold: u64,
}

impl<Id: Hash + Eq> VoterSet<Id> {
	/// Get the voter info for a voter.
	pub fn info<'a>(&'a self, id: &Id) -> Option<&'a VoterInfo> {
		self.weights.get(id)
	}

	/// Get the length of the set.
	pub fn len(&self) -> usize { self.voters.len() }

	/// Whether the set contains the key.
	pub fn contains_key(&self, id: &Id) -> bool {
		self.weights.contains_key(id)
	}

	// Get voter by index.
	pub fn voter_by_index(&self, idx: usize) -> &(Id, u64) {
		&self.voters[idx]
	}

	/// Get voter info by index.
	pub fn weight_by_index<'a>(&'a self, idx: usize) -> Option<u64> {
		self.voters.get(idx).map(|&(_, weight)| weight)
	}

	/// Get the threshold weight.
	pub fn threshold(&self) -> u64 { self.threshold }

	/// Get the total weight.
	pub fn total_weight(&self) -> u64 {
		self.voters.iter().map(|&(_, weight)| weight).sum()
	}
}

impl<Id: Hash + Eq + Clone + Ord> std::iter::FromIterator<(Id, u64)> for VoterSet<Id> {
	fn from_iter<I: IntoIterator<Item = (Id, u64)>>(iterable: I) -> Self {
		let iter = iterable.into_iter();
		let (lower, _) = iter.size_hint();

		let mut voters = Vec::with_capacity(lower);
		let mut weights = HashMap::with_capacity(lower);

		let mut total_weight = 0;
		for (id, weight) in iter {
			voters.push((id.clone(), weight));
			total_weight += weight;
		}

		voters.sort_unstable();

		for (idx, (id, weight)) in voters.iter().enumerate() {
			weights.insert(id.clone(), VoterInfo { canon_idx: idx, weight: weight.clone() });
		}

		let threshold = threshold(total_weight);
		VoterSet { weights, voters, threshold }
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoterInfo {
	canon_idx: usize,
	weight: u64,
}

impl VoterInfo {
	/// Get the canonical index of the voter.
	pub fn canon_idx(&self) -> usize { self.canon_idx }

	/// Get the weight of the voter.
	pub fn weight(&self) -> u64 { self.weight }
}


#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn voters_are_sorted() {
		let v1: VoterSet<usize> = [
			(1, 5),
			(4, 1),
			(3, 9),
			(5, 7),
			(9, 9),
			(2, 7),
		].iter().cloned().collect();

		let v2: VoterSet<usize> = [
			(1, 5),
			(2, 7),
			(3, 9),
			(4, 1),
			(5, 7),
			(9, 9),
		].iter().cloned().collect();

		assert_eq!(v1, v2);
	}

	#[test]
	fn voter_by_index_works() {
		let v: VoterSet<usize> = [
			(1, 5),
			(4, 1),
			(3, 9),
			(5, 7),
			(9, 9),
			(2, 7),
		].iter().cloned().collect();

		assert_eq!(v.len(), 6);
		assert_eq!(v.total_weight(), 38);

		assert_eq!(v.voter_by_index(0), &(1, 5));
		assert_eq!(v.voter_by_index(1), &(2, 7));
		assert_eq!(v.voter_by_index(2), &(3, 9));
		assert_eq!(v.voter_by_index(3), &(4, 1));
		assert_eq!(v.voter_by_index(4), &(5, 7));
		assert_eq!(v.voter_by_index(5), &(9, 9));
	}
}
