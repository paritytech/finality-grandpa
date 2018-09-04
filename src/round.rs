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

use bitfield::{Bitfield, Shared as BitfieldContext, LiveBitfield};

use super::{Equivocation, Prevote, Precommit, Chain};

#[derive(Hash, Eq, PartialEq)]
struct Address;

#[derive(Debug, Clone)]
struct VoteWeight<Id> {
	prevote: usize,
	precommit: usize,
	bitfield: Bitfield<Id>,
}

impl<Id> Default for VoteWeight<Id> {
	fn default() -> Self {
		VoteWeight {
			prevote: 0,
			precommit: 0,
			bitfield: Bitfield::Blank,
		}
	}
}

impl<Id: Eq> AddAssign for VoteWeight<Id> {
	fn add_assign(&mut self, rhs: VoteWeight<Id>) {
		self.prevote += rhs.prevote;
		self.precommit += rhs.precommit;

		// if any votes are counted in both weights, undo the double-counting.
		let (o_v, o_c) = self.bitfield.overlap(&rhs.bitfield)
			.expect("vote-weights with different contexts are never compared in this module; qed");

		self.prevote -= o_v;
		self.precommit -= o_c;

		self.bitfield = self.bitfield.merge(&rhs.bitfield)
			.expect("vote-weights with different contexts are never compared in this module; qed");
	}
}

// votes from a single validator
enum VoteMultiplicity<Vote, Signature> {
	// validator voted once.
	Single(Vote, Signature),
	// validator equivocated once.
	Equivocated((Vote, Signature), (Vote, Signature)),
	// validator equivocated many times.
	EquivocatedMany(Vec<(Vote, Signature)>),
}

impl<Vote, Signature> VoteMultiplicity<Vote, Signature> {
	fn update_graph<Id, F, G>(
		&self,
		weight: usize,
		mut import: F,
		make_bitfield: G,
	) -> Result<(), ::Error> where
		F: FnMut(&Vote, usize, Bitfield<Id>) -> Result<(), ::Error>,
		G: Fn() -> Bitfield<Id>,
	{
		match *self {
			VoteMultiplicity::Single(ref v, _) => {
				import(v, weight, Bitfield::Blank)
			}
			VoteMultiplicity::Equivocated((ref v_a, _), (ref v_b, _)) => {
				let bitfield = make_bitfield();

				// import the second vote. some of the weight may have been double-counted.
				import(v_b, weight, bitfield.clone())?;

				// re-import the first vote (but with zero weight) to undo double-counting
				// and initialize bitfields on non-overlapping sections of the path.
				import(v_a, 0, bitfield)
					.expect("all vote-nodes present in graph already; no chain lookup necessary; qed");

				Ok(())
			}
			VoteMultiplicity::EquivocatedMany(ref votes) => {
				let v = votes.last().expect("many equivocations means there is always a last vote; qed");
				import(&v.0, weight, make_bitfield())
			}
		}
	}

	fn equivocation(&self) -> Option<(&(Vote, Signature), &(Vote, Signature))> {
		match *self {
			VoteMultiplicity::Single(_, _) => None,
			VoteMultiplicity::Equivocated(ref a, ref b) => Some((a, b)),
			VoteMultiplicity::EquivocatedMany(ref v) => {
				assert!(v.len() >= 2, "Multi-equivocations variant always has at least two distinct members; qed");

				match (v.first(), v.last()) {
					(Some(ref a), Some(ref b)) => Some((a, b)),
					_ => panic!("length checked above; qed"),
				}
			}
		}
	}
}

struct VoteTracker<Id: Hash + Eq, Vote, Signature> {
	votes: HashMap<Id, VoteMultiplicity<Vote, Signature>>,
	current_weight: usize,
}

impl<Id: Hash + Eq + Clone, Vote: Clone + Eq, Signature: Clone> VoteTracker<Id, Vote, Signature> {
	fn new() -> Self {
		VoteTracker {
			votes: HashMap::new(),
			current_weight: 0,
		}
	}

	// track a vote, returning a value containing the multiplicity of all votes from this ID.
	// if the vote is an equivocation, returns a value indicating
	// it as such (the new vote is always the last in the multiplicity).
	//
	// since this struct doesn't track the round-number of votes, that must be set
	// by the caller.
	fn add_vote(&mut self, id: Id, vote: Vote, signature: Signature, weight: usize)
		-> &VoteMultiplicity<Vote, Signature>
	{
		match self.votes.entry(id) {
			Entry::Vacant(vacant) => {
				self.current_weight += weight;
				&*vacant.insert(VoteMultiplicity::Single(vote, signature))
			}
			Entry::Occupied(mut occupied) => {
				let new_val = match *occupied.get_mut() {
					VoteMultiplicity::Single(ref v, ref s) =>
						Some(VoteMultiplicity::Equivocated((v.clone(), s.clone()), (vote, signature))),
					VoteMultiplicity::Equivocated(ref a, ref b) =>
						Some(VoteMultiplicity::EquivocatedMany(vec![a.clone(), b.clone(), (vote, signature)])),
					VoteMultiplicity::EquivocatedMany(ref mut v) => {
						v.push((vote, signature));
						None
					}
				};

				if let Some(new_val) = new_val {
					*occupied.get_mut() = new_val;
				}

				&*occupied.into_mut()
			}
		}
	}
}

/// The result of something being imported.
/// Contains an optional equivocation proof and potential updates to blocks.
pub struct Imported<Id, V, H, Signature> {
	/// Equivocation by a validator.
	pub equivocation: Option<Equivocation<Id, V, Signature>>,
	/// Block trackers that were updated.
	pub trackers: Trackers<H>,
}

/// Updated trackers.
pub struct Trackers<H> {
	/// The prevote-GHOST block.
	pub prevote_ghost: Option<(H, usize)>,
	/// The finalized block.
	pub finalized: Option<(H, usize)>,
	/// The new round-estimate.
	pub estimate: Option<(H, usize)>,
	/// Whether the round is completable.
	pub completable: bool,
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

/// Stores data for a round.
pub struct Round<Id: Hash + Eq, H: Hash + Eq, Signature> {
	graph: VoteGraph<H, VoteWeight<Id>>, // DAG of blocks which have been voted on.
	prevote: VoteTracker<Id, Prevote<H>, Signature>, // tracks prevotes that have been counted
	precommit: VoteTracker<Id, Precommit<H>, Signature>, // tracks precommits
	round_number: u64,
	voters: HashMap<Id, usize>,
	faulty_weight: usize,
	total_weight: usize,
	bitfield_context: BitfieldContext<Id>,
	prevote_ghost: Option<(H, usize)>, // current memoized prevote-GHOST block
	finalized: Option<(H, usize)>, // best finalized block in this round.
	estimate: Option<(H, usize)>, // current memoized round-estimate
	completable: bool, // whether the round is completable
}

impl<Id, H, Signature> Round<Id, H, Signature> where
	Id: Hash + Clone + Eq + ::std::fmt::Debug,
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	Signature: Eq + Clone,
{
	/// Create a new round accumulator for given round number and with given weight.
	/// Not guaranteed to work correctly unless total_weight more than 3x larger than faulty_weight
	pub fn new(round_params: RoundParams<Id, H>) -> Self {
		let (base_hash, base_number) = round_params.base;
		let total_weight: usize = round_params.voters.values().cloned().sum();
		let faulty_weight = total_weight.saturating_sub(1) / 3;
		let n_validators = round_params.voters.len();

		Round {
			round_number: round_params.round_number,
			faulty_weight: faulty_weight,
			total_weight: total_weight,
			voters: round_params.voters,
			graph: VoteGraph::new(base_hash, base_number),
			prevote: VoteTracker::new(),
			precommit: VoteTracker::new(),
			bitfield_context: BitfieldContext::new(n_validators),
			prevote_ghost: None,
			finalized: None,
			estimate: None,
			completable: false,
		}
	}

	/// Import a prevote. Returns an equivocation proof if the vote is an equivocation.
	///
	/// Should not import the same prevote more than once.
	pub fn import_prevote<C: Chain<H>>(
		&mut self,
		chain: &C,
		vote: Prevote<H>,
		signer: Id,
		signature: Signature,
	) -> Result<Imported<Id, Prevote<H>, H, Signature>, ::Error> {
		let weight = match self.voters.get(&signer) {
			Some(weight) => *weight,
			None => return Ok(Imported { equivocation: None, trackers: self.current_trackers() }),
		};

		let equivocation = {
			let graph = &mut self.graph;
			let bitfield_context = &self.bitfield_context;
			let multiplicity = self.prevote.add_vote(signer.clone(), vote, signature, weight);
			let round_number = self.round_number;

			multiplicity.update_graph(
				weight,
				move |vote, weight, bitfield| {
					let vote_weight = VoteWeight {
						prevote: weight,
						precommit: 0,
						bitfield,
					};

					graph.insert(
						vote.target_hash.clone(),
						vote.target_number as usize,
						vote_weight,
						chain
					)
				},
				|| {
					let mut live = LiveBitfield::new(bitfield_context.clone());
					live.equivocated_prevote(signer.clone(), weight)
						.expect("no unrecognized voters will be added as equivocators; qed");
					Bitfield::Live(live)
				}
			)?;

			multiplicity.equivocation().map(|(first, second)| Equivocation {
				round_number,
				identity: signer,
				first: first.clone(),
				second: second.clone(),
			})
		};

		// update prevote-GHOST
		let threshold = self.threshold();
		if self.prevote.current_weight >= threshold {
			self.prevote_ghost = self.graph.find_ghost(self.prevote_ghost.take(), |v| v.prevote >= threshold);
		}

		self.update_trackers();
		Ok(Imported { equivocation, trackers: self.current_trackers() })
	}

	/// Import a precommit. Returns an equivocation proof if the vote is an
	/// equivocation.
	///
	/// Should not import the same precommit more than once.
	pub fn import_precommit<C: Chain<H>>(
		&mut self,
		chain: &C,
		vote: Precommit<H>,
		signer: Id,
		signature: Signature,
	) -> Result<Imported<Id, Precommit<H>, H, Signature>, ::Error> {
		let weight = match self.voters.get(&signer) {
			Some(weight) => *weight,
			None => return Ok(Imported { equivocation: None, trackers: self.current_trackers() }),
		};

		let equivocation = {
			let graph = &mut self.graph;
			let bitfield_context = &self.bitfield_context;
			let multiplicity = self.precommit.add_vote(signer.clone(), vote, signature, weight);
			let round_number = self.round_number;

			multiplicity.update_graph(
				weight,
				move |vote, weight, bitfield| {
					let vote_weight = VoteWeight {
						prevote: 0,
						precommit: weight,
						bitfield,
					};

					graph.insert(
						vote.target_hash.clone(),
						vote.target_number as usize,
						vote_weight,
						chain
					)
				},
				|| {
					let mut live = LiveBitfield::new(bitfield_context.clone());
					live.equivocated_precommit(signer.clone(), weight)
						.expect("no unrecognized voters will be added as equivocators; qed");
					Bitfield::Live(live)
				}
			)?;

			multiplicity.equivocation().map(|(first, second)| Equivocation {
				round_number,
				identity: signer,
				first: first.clone(),
				second: second.clone(),
			})
		};

		self.update_trackers();
		Ok(Imported { equivocation, trackers: self.current_trackers() })
	}

	fn current_trackers(&self) -> Trackers<H> {
		Trackers {
			prevote_ghost: self.prevote_ghost.clone(),
			finalized: self.finalized.clone(),
			estimate: self.finalized.clone(),
			completable: self.completable,
		}
	}

	// update the round-estimate and whether the round is completable.
	fn update_trackers(&mut self) {
		let threshold = self.threshold();
		if self.prevote.current_weight < threshold { return }

		let remaining_commit_votes = self.total_weight - self.precommit.current_weight;
		let (g_hash, g_num) = match self.prevote_ghost.clone() {
			None => return,
			Some(x) => x,
		};

		// anything new finalized? finalized blocks are those which have both
		// 2/3+ prevote and prevote weight.
		let threshold = self.threshold();
		if self.precommit.current_weight >= threshold {
			self.finalized = self.graph.find_ancestor(
				g_hash.clone(),
				g_num,
				|v| v.precommit >= threshold
			);
		};

		// figuring out whether a block can still be committed for is
		// not straightforward because we have to account for all possible future
		// equivocations and thus cannot discount weight from validators who
		// have already voted.
		let tolerated_equivocations = self.total_weight - threshold;
		let possible_to_precommit = |weight: &VoteWeight<_>| {
			// find how many more equivocations we could still get on this
			// block.
			let (_, precommit_equivocations) = weight.bitfield.total_weight();
			let additional_equivocation_weight = tolerated_equivocations
				.saturating_sub(precommit_equivocations);

			// all the votes already applied on this block,
			// and assuming all remaining actors commit to this block,
			// and assuming all possible equivocations end up on this block.
			let full_possible_weight = weight.precommit
				.saturating_add(remaining_commit_votes)
				.saturating_add(additional_equivocation_weight);

			full_possible_weight >= threshold
		};

		// until we have threshold precommits, any new block could get supermajority
		// precommits because there are at least f + 1 precommits remaining and then
		// f equivocations.
		//
		// once it's at least that level, we only need to consider blocks
		// already referenced in the graph, because no new leaf nodes
		// could ever have enough precommits.
		//
		// the round-estimate is the highest block in the chain with head
		// `prevote_ghost` that could have supermajority-commits.
		if self.precommit.current_weight >= threshold {
			self.estimate = self.graph.find_ancestor(
				g_hash.clone(),
				g_num,
				possible_to_precommit,
			);
		} else {
			self.estimate = Some((g_hash, g_num));
			return;
		}

		self.completable = self.estimate.clone().map_or(false, |(b_hash, b_num)| {
			b_hash != g_hash || {
				// round-estimate is the same as the prevote-ghost.
				// this round is still completable if no further blocks
				// could have commit-supermajority.
				self.graph.find_ghost(Some((b_hash, b_num)), possible_to_precommit)
					.map_or(true, |x| x == (g_hash, g_num))
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

		assert_eq!(round.finalized, None);

		// import some prevotes.
		{
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

			round.import_prevote(
				&chain,
				Prevote::new("EA", 7),
				"Eve",
				Signature("Eve"),
			).unwrap();

			assert_eq!(round.finalized, Some(("E", 6)));
		}

		round.import_precommit(
			&chain,
			Precommit::new("EA", 7),
			"Eve",
			Signature("Eve"),
		).unwrap();

		assert_eq!(round.finalized, Some(("EA", 7)));
	}

	#[test]
	fn equivocate_does_not_double_count() {
		let mut chain = DummyChain::new();
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E", "F"]);
		chain.push_blocks("E", &["EA", "EB", "EC", "ED"]);
		chain.push_blocks("F", &["FA", "FB", "FC"]);

		let mut round = Round::new(RoundParams {
			round_number: 1,
			voters: voters(),
			base: ("C", 4),
		});

		assert!(round.import_prevote(
			&chain,
			Prevote::new("FC", 10),
			"Eve",
			Signature("Eve-1"),
		).unwrap().equivocation.is_none());


		assert!(round.prevote_ghost.is_none());

		assert!(round.import_prevote(
			&chain,
			Prevote::new("ED", 10),
			"Eve",
			Signature("Eve-2"),
		).unwrap().equivocation.is_some());

		assert!(round.import_prevote(
			&chain,
			Prevote::new("F", 7),
			"Eve",
			Signature("Eve-2"),
		).unwrap().equivocation.is_some());

		// three eves together would be enough.

		assert!(round.prevote_ghost.is_none());

		assert!(round.import_prevote(
			&chain,
			Prevote::new("FA", 8),
			"Bob",
			Signature("Bob-1"),
		).unwrap().equivocation.is_none());

		assert_eq!(round.prevote_ghost, Some(("FA", 8)));
	}
}
