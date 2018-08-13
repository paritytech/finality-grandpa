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

use super::Equivocation;

#[derive(Hash, Eq, PartialEq)]
struct Address;

#[derive(Debug, Clone)]
struct VoteCount {
	prevote: usize,
	precommit: usize,
}

impl AddAssign for VoteCount {
	fn add_assign(&mut self, rhs: VoteCount) {
		*self.prevote += rhs.prevote;
		*self.precommit += rhs.precommit;
	}
}

struct VoteTracker<Id: Hash + Eq, Vote, Signature> {
	votes: HashMap<Id, (Vote, Signature)>,
	current_weight: usize,
}

impl<Id: Hash + Eq, Vote: Clone, Signature: Eq> VoteTracker<Id, Vote, Signature> {
	fn new() -> Self {
		VoteTracker {
			votes: HashMap::new();
			current_weight: 0,
		}
	}

	// track a vote. if the vote is an equivocation, returns a proof-of-equivocation and
	// otherwise notes the current amount of weight on the tracked vote-set
	fn add_vote(&mut self, id: Id, vote: Vote, signature: Signature, weight: usize)
		-> Result<(), Equivocation<Id, Vote, Signature>>
	{
		match self.votes.entry(id) {
			Entry::Vacant(mut vacant) => {
				vacant.insert((vote, signature));
			}
		}
	}
}

/// Stores data for a round.
pub struct Round<H: Hash + Eq> {
	graph: VoteGraph<H, VoteCount>,
	threshold_weight: usize,
	total_weight: usize,
	current_weight: usize,
}
