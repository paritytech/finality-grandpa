// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-afg.

// finality-afg is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// finality-afg is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-afg. If not, see <http://www.gnu.org/licenses/>.

//! Logic for a single round of AfG.

use vote_graph::VoteGraph;

use std::collections::hash_map::{HashMap, Entry};
use std::hash::Hash;
use std::ops::AddAssign;

use super::{Equivocation, Prevote, Precommit};

#[derive(Hash, Eq, PartialEq)]
struct Address;

#[derive(Default, Debug, Clone)]
struct VoteCount {
	prevote: usize,
	precommit: usize,
}

impl AddAssign for VoteCount {
	fn add_assign(&mut self, rhs: VoteCount) {
		self.prevote += rhs.prevote;
		self.precommit += rhs.precommit;
	}
}

struct VoteTracker<Id: Hash + Eq, Vote, Signature> {
	votes: HashMap<Id, (Vote, Signature)>,
	current_weight: usize,
}

impl<Id: Hash + Eq + Clone, Vote: Clone + Eq, Signature: Clone> VoteTracker<Id, Vote, Signature> {
	fn new() -> Self {
		VoteTracker {
			votes: HashMap::new(),
			current_weight: 0,
		}
	}

	// track a vote. if the vote is an equivocation, returns a proof-of-equivocation and
	// otherwise notes the current amount of weight on the tracked vote-set.
	//
	// since this struct doesn't track the round number of votes, that must be set
	// by the caller.
	fn add_vote(&mut self, id: Id, vote: Vote, signature: Signature, weight: usize)
		-> Result<(), Equivocation<Id, Vote, Signature>>
	{
		match self.votes.entry(id.clone()) {
			Entry::Vacant(mut vacant) => {
				vacant.insert((vote, signature));
			}
			Entry::Occupied(mut occupied) => {
				if occupied.get().0 != vote {
					return Err(Equivocation {
						round_number: 0,
						identity: id,
						first: occupied.get().clone(),
						second: (vote, signature),
					})
				}
			}
		}

		Ok(())
	}
}

/// Parameters for starting a round.
pub struct RoundParams<H> {
	/// The round number for votes.
	pub round_number: usize,
	/// The amount of byzantine-faulty voting weight that exists (assumed) in the
	/// system.
	pub faulty_weight: usize,
	/// The amount of total voting weight in the system
	pub total_weight: usize,
	/// The base block to build on.
	pub base: (H, usize),
}

/// Stores data for a round.
pub struct Round<Id: Hash + Eq, H: Hash + Eq, Signature> {
	graph: VoteGraph<H, VoteCount>,
	prevote: VoteTracker<Id, Prevote<H>, Signature>,
	precommit: VoteTracker<Id, Precommit<H>, Signature>,
	round_number: usize,
	faulty_weight: usize,
	total_weight: usize,
}

impl<Id: Hash + Clone + Eq, H: Hash + Clone + Eq + Ord, Signature: Eq + Clone> Round<Id, H, Signature> {
	/// Create a new round accumulator for given round number and with given weight.
	/// Not guaranteed to work correctly unless total_weight more than 3x larger than faulty_weight
	pub fn new(round_params: RoundParams<H>) -> Self {
		let (base_hash, base_number) = round_params.base;
		Round {
			round_number: round_params.round_number,
			faulty_weight: round_params.faulty_weight,
			total_weight: round_params.total_weight,
			graph: VoteGraph::new(base_hash, base_number),
			prevote: VoteTracker::new(),
			precommit: VoteTracker::new(),
		}
	}
}
