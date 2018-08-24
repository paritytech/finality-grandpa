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

use super::{Equivocation, Prevote, Precommit, Chain};

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
		match self.votes.entry(id) {
			Entry::Vacant(mut vacant) => {
				vacant.insert((vote, signature));
			}
			Entry::Occupied(occupied) => {
				if occupied.get().0 != vote {
					return Err(Equivocation {
						round_number: 0,
						identity: occupied.key().clone(),
						first: occupied.get().clone(),
						second: (vote, signature),
					})
				}
			}
		}

		self.current_weight += weight;

		Ok(())
	}
}

/// Parameters for starting a round.
pub struct RoundParams<Id: Hash + Eq, H> {
	/// The round number for votes.
	pub round_number: u64,
	/// Actors and weights in the round.
	pub voters: HashMap<Id, usize>,
	/// The base block to build on.
	pub base: (H, usize),
}

#[derive(Debug)]
pub enum Error<Id, H, S> {
	PrevoteEquivocation(Equivocation<Id, Prevote<H>, S>),
	PrecommitEquivocation(Equivocation<Id, Precommit<H>, S>),
	Chain(::Error),
}

/// Stores data for a round.
pub struct Round<Id: Hash + Eq, H: Hash + Eq, Signature> {
	graph: VoteGraph<H, VoteCount>, // DAG of blocks which have been voted on.
	prevote: VoteTracker<Id, Prevote<H>, Signature>, // tracks prevotes that have been counted
	precommit: VoteTracker<Id, Precommit<H>, Signature>, // tracks precommits
	round_number: u64,
	voters: HashMap<Id, usize>,
	faulty_weight: usize,
	total_weight: usize,
	prevote_ghost: Option<(H, usize)>, // current memoized prevote-GHOST block
	finalized: Option<(H, usize)>, // best finalized block in this round.
	estimate: Option<(H, usize)>, // current memoized round-estimate
	completable: bool, // whether the round is completable
}

impl<Id, H, Signature> Round<Id, H, Signature> where
	Id: Hash + Clone + Eq,
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	Signature: Eq + Clone,
{
	/// Create a new round accumulator for given round number and with given weight.
	/// Not guaranteed to work correctly unless total_weight more than 3x larger than faulty_weight
	pub fn new(round_params: RoundParams<Id, H>) -> Self {
		let (base_hash, base_number) = round_params.base;
		let total_weight: usize = round_params.voters.values().cloned().sum();
		let faulty_weight = total_weight.saturating_sub(1) / 3;

		Round {
			round_number: round_params.round_number,
			faulty_weight: faulty_weight,
			total_weight: total_weight,
			voters: round_params.voters,
			graph: VoteGraph::new(base_hash, base_number),
			prevote: VoteTracker::new(),
			precommit: VoteTracker::new(),
			prevote_ghost: None,
			finalized: None,
			estimate: None,
			completable: false,
		}
	}

	/// Import a prevote. Has no effect on internal state if an equivocation or if the
	/// signer is not a known voter.
	pub fn import_prevote<C: Chain<H>>(
		&mut self,
		chain: &C,
		vote: Prevote<H>,
		signer: Id,
		signature: Signature,
	) -> Result<(), Error<Id, H, Signature>> {
		let weight = match self.voters.get(&signer) {
			Some(weight) => *weight,
			None => return Ok(()),
		};

		self.prevote.add_vote(signer, vote.clone(), signature, weight)
			.map_err(|mut e| { e.round_number = self.round_number; e })
			.map_err(Error::PrevoteEquivocation)?;

		let vc = VoteCount {
			prevote: weight,
			precommit: 0,
		};

		self.graph.insert(vote.target_hash, vote.target_number as usize, vc, chain)
			.map_err(Error::Chain)?;

		// update prevote-GHOST
		let threshold = self.threshold();
		if self.prevote.current_weight >= threshold {
			self.prevote_ghost = self.graph.find_ghost(self.prevote_ghost.take(), |v| v.prevote >= threshold);
		}

		self.update_estimate();
		Ok(())
	}

	/// Import a prevote. Has no effect on internal state if an equivocation or if
	/// the signer is not a known voter.
	pub fn import_precommit<C: Chain<H>>(
		&mut self,
		chain: &C,
		vote: Precommit<H>,
		signer: Id,
		signature: Signature,
	) -> Result<(), Error<Id, H, Signature>> {
		let weight = match self.voters.get(&signer) {
			Some(weight) => *weight,
			None => return Ok(()),
		};

		self.precommit.add_vote(signer, vote.clone(), signature, weight)
			.map_err(|mut e| { e.round_number = self.round_number; e })
			.map_err(Error::PrecommitEquivocation)?;

		let vc = VoteCount {
			prevote: 0,
			precommit: weight,
		};

		self.graph.insert(vote.target_hash, vote.target_number as usize, vc, chain)
			.map_err(Error::Chain)?;

		// anything finalized?
		let threshold = self.threshold();
		if self.precommit.current_weight >= threshold {
			self.finalized = self.graph.find_ghost(self.finalized.take(), |v| v.precommit >= threshold);
		}

		self.update_estimate();
		Ok(())
	}

	// update the round-estimate and whether the round is completable.
	fn update_estimate(&mut self) {
		let threshold = self.threshold();
		if self.prevote.current_weight < threshold { return }

		let remaining_commit_votes = self.total_weight - self.precommit.current_weight;
		let (g_hash, g_num) = match self.prevote_ghost.clone() {
			None => return,
			Some(x) => x,
		};

		self.estimate = self.graph.find_ancestor(
			g_hash.clone(),
			g_num,
			|vote| vote.precommit + remaining_commit_votes >= threshold,
		);

		self.completable = self.estimate.clone().map_or(false, |(b_hash, b_num)| {
			b_hash != g_hash || {
				// round-estimate is the same as the prevote-ghost.
				// this round is still completable if no further blocks
				// could have commit-supermajority.
				let remaining_commit_votes = self.total_weight - self.precommit.current_weight;
				let threshold = self.threshold();

				// when the remaining votes are at least the threshold,
				// we can always have commit-supermajority.
				//
				// once it's below that level, we only need to consider already
				// blocks referenced in the graph, because no new leaf nodes
				// could ever have enough commits.
				remaining_commit_votes < threshold &&
					self.graph.find_ghost(Some((b_hash, b_num)), |count|
						count.precommit + remaining_commit_votes >= threshold
					).map_or(true, |x| x == (g_hash, g_num))
			}
		})
	}

	/// Fetch the "round-estimate": the best block which might have been finalized
	/// in this round.
	///
	/// Returns `None` when new new blocks could have been finalized in this round,
	/// according to our estimate.
	pub fn estimate(&self) -> Option<&(H, usize)> {
		self.estimate.as_ref()
	}

	/// Fetch the most recently finalized block.
	pub fn finalized(&self) -> Option<&(H, usize)> {
		self.finalized.as_ref()
	}

	/// Returns `true` when the round is completable.
	///
	/// This is the case when the round-estimate is an ancestor of the prevote-ghost head,
	/// or when they are the same block _and_ none of its children could possibly have
	/// enough precommits.
	pub fn completable(&self) -> bool {
		self.completable
	}

	// Threshold number of weight for supermajority.
	pub fn threshold(&self) -> usize {
		threshold(self.total_weight, self.faulty_weight)
	}
}

fn threshold(total_weight: usize, faulty_weight: usize) -> usize {
	let mut double_supermajority = total_weight + faulty_weight + 1;
	double_supermajority += double_supermajority & 1;
	double_supermajority / 2
}

#[cfg(test)]
mod tests {
	use super::*;
	use testing::{GENESIS_HASH, DummyChain};

	fn voters() -> HashMap<&'static str, usize> {
		[
			("Alice", 5),
			("Bob", 7),
			("Eve", 3),
		].iter().cloned().collect()
	}

	#[derive(PartialEq, Eq, Hash, Clone, Debug)]
	struct Signature(&'static str);

	#[test]
	fn threshold_is_right() {
		assert_eq!(threshold(10, 3), 7);
		assert_eq!(threshold(100, 33), 67);
		assert_eq!(threshold(101, 33), 68);
		assert_eq!(threshold(102, 33), 68);
	}

	#[test]
	fn estimate_is_valid() {
		let mut chain = DummyChain::new();
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E", "F"]);
		chain.push_blocks("E", &["EA", "EB", "EC", "ED"]);
		chain.push_blocks("F", &["FA", "FB", "FC"]);

		let mut round = Round::new(RoundParams {
			round_number: 1,
			voters: voters(),
			base: ("C", 4),
		});

		round.import_prevote(
			&chain,
			Prevote::new("FC", 10),
			"Alice",
			Signature("Alice"),
		).unwrap();

		round.import_prevote(
			&chain,
			Prevote::new("ED", 10),
			"Bob",
			Signature("Bob"),
		).unwrap();

		assert_eq!(round.prevote_ghost, Some(("E", 6)));
		assert_eq!(round.estimate(), Some(&("E", 6)));
		assert!(!round.completable());

		round.import_prevote(
			&chain,
			Prevote::new("F", 7),
			"Eve",
			Signature("Eve"),
		).unwrap();

		assert_eq!(round.prevote_ghost, Some(("E", 6)));
		assert_eq!(round.estimate(), Some(&("E", 6)));
	}

	#[test]
	fn finalization() {
		let mut chain = DummyChain::new();
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E", "F"]);
		chain.push_blocks("E", &["EA", "EB", "EC", "ED"]);
		chain.push_blocks("F", &["FA", "FB", "FC"]);

		let mut round = Round::new(RoundParams {
			round_number: 1,
			voters: voters(),
			base: ("C", 4),
		});

		round.import_precommit(
			&chain,
			Precommit::new("FC", 10),
			"Alice",
			Signature("Alice"),
		).unwrap();

		round.import_precommit(
			&chain,
			Precommit::new("ED", 10),
			"Bob",
			Signature("Bob"),
		).unwrap();

		assert_eq!(round.finalized, Some(("E", 6)));

		round.import_precommit(
			&chain,
			Precommit::new("EA", 7),
			"Eve",
			Signature("Eve"),
		).unwrap();

		assert_eq!(round.finalized, Some(("EA", 7)));
	}
}
