// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-grandpa.

// finality-grandpa is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// finality-grandpa is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-grandpa. If not, see <http://www.gnu.org/licenses/>.

//! Logic for a single round of GRANDPA.

use crate::vote_graph::VoteGraph;

use std::collections::hash_map::{HashMap, Entry};
use std::hash::Hash;
use std::ops::AddAssign;

use crate::bitfield::{Shared as BitfieldContext, Bitfield};

use super::{Equivocation, Prevote, Precommit, Chain, BlockNumberOps, VoterSet};

#[derive(Hash, Eq, PartialEq)]
struct Address;

#[derive(Debug, PartialEq, Eq)]
struct TotalWeight {
	prevote: u64,
	precommit: u64,
}

#[derive(Debug, Clone)]
struct VoteWeight {
	bitfield: Bitfield,
}

impl Default for VoteWeight {
	fn default() -> Self {
		VoteWeight {
			bitfield: Bitfield::Blank,
		}
	}
}

impl VoteWeight {
	// compute the total weight of all votes on this node.
	// equivocators are counted as voting for everything, and must be provided.
	fn total_weight<Id: Hash + Eq>(
		&self,
		equivocators: &Bitfield,
		voter_set: &VoterSet<Id>,
	) -> TotalWeight {
		let with_equivocators = self.bitfield.merge(equivocators)
			.expect("this function is never invoked with \
				equivocators of different canonicality; qed");

		// the unwrap-or is defensive only: there should be registered weights for
		// all known indices.
		let (prevote, precommit) = with_equivocators
			.total_weight(|idx| voter_set.weight_by_index(idx).unwrap_or(0));

		TotalWeight { prevote, precommit }
	}
}

impl AddAssign for VoteWeight {
	fn add_assign(&mut self, rhs: VoteWeight) {
		self.bitfield = self.bitfield.merge(&rhs.bitfield)
			.expect("both bitfields set to same length; qed");
	}
}

// votes from a single validator
enum VoteMultiplicity<Vote, Signature> {
	// validator voted once.
	Single(Vote, Signature),
 	// validator equivocated at least once.
	Equivocated((Vote, Signature), (Vote, Signature)),
}

impl<Vote: Eq, Signature: Eq> VoteMultiplicity<Vote, Signature> {
	fn contains(&self, vote: &Vote, signature: &Signature) -> bool {
		match self {
			VoteMultiplicity::Single(v, s) =>
				v == vote && s == signature,
			VoteMultiplicity::Equivocated((v1, s1), (v2, s2)) => {
				v1 == vote && s1 == signature ||
					v2 == vote && s2 == signature
			},
		}
	}
}

struct VoteTracker<Id: Hash + Eq, Vote, Signature> {
	votes: HashMap<Id, VoteMultiplicity<Vote, Signature>>,
	current_weight: u64,
}

impl<Id: Hash + Eq + Clone, Vote: Clone + Eq, Signature: Clone + Eq> VoteTracker<Id, Vote, Signature> {
	fn new() -> Self {
		VoteTracker {
			votes: HashMap::new(),
			current_weight: 0,
		}
	}

	// track a vote, returning a value containing the multiplicity of all votes from this ID.
	// if the vote is the first equivocation, returns a value indicating
	// it as such (the new vote is always the last in the multiplicity).
	//
	// if the vote is a further equivocation, it is ignored and there is nothing
	// to do.
	//
	// since this struct doesn't track the round-number of votes, that must be set
	// by the caller.
	fn add_vote(&mut self, id: Id, vote: Vote, signature: Signature, weight: u64)
		-> Option<&VoteMultiplicity<Vote, Signature>>
	{
		match self.votes.entry(id) {
			Entry::Vacant(vacant) => {
				self.current_weight += weight;
				return Some(&*vacant.insert(VoteMultiplicity::Single(vote, signature)));
			}
			Entry::Occupied(mut occupied) => {
				if occupied.get().contains(&vote, &signature) {
					return None;
				}

				// import, but ignore further equivocations.
				let new_val = match *occupied.get_mut() {
					VoteMultiplicity::Single(ref v, ref s) =>
						Some(VoteMultiplicity::Equivocated((v.clone(), s.clone()), (vote, signature))),
					VoteMultiplicity::Equivocated(_, _) => return None,
				};

				if let Some(new_val) = new_val {
					*occupied.get_mut() = new_val;
				}

				Some(&*occupied.into_mut())
			}
		}
	}

	// Returns all imported votes.
	fn votes(&self) -> Vec<(Id, Vote, Signature)> {
		let mut votes = Vec::new();

		for (id, vote) in self.votes.iter() {
			match vote {
				VoteMultiplicity::Single(v, s) => {
					votes.push((id.clone(), v.clone(), s.clone()))
				},
				VoteMultiplicity::Equivocated((v1, s1), (v2, s2)) => {
					votes.push((id.clone(), v1.clone(), s1.clone()));
					votes.push((id.clone(), v2.clone(), s2.clone()));
				},
			}
		}

		votes
	}
}

/// State of the round.
#[derive(PartialEq, Clone, Debug)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct State<H, N> {
	/// The prevote-GHOST block.
	pub prevote_ghost: Option<(H, N)>,
	/// The finalized block.
	pub finalized: Option<(H, N)>,
	/// The new round-estimate.
	pub estimate: Option<(H, N)>,
	/// Whether the round is completable.
	pub completable: bool,
}

impl<H: Clone, N: Clone> State<H, N> {
	// Genesis state.
	pub fn genesis(genesis: (H, N)) -> Self {
		State {
			prevote_ghost: Some(genesis.clone()),
			finalized: Some(genesis.clone()),
			estimate: Some(genesis),
			completable: true,
		}
	}
}

/// Parameters for starting a round.
pub struct RoundParams<Id: Hash + Eq, H, N> {
	/// The round number for votes.
	pub round_number: u64,
	/// Actors and weights in the round.
	pub voters: VoterSet<Id>,
	/// The base block to build on.
	pub base: (H, N),
}

/// Stores data for a round.
pub struct Round<Id: Hash + Eq, H: Hash + Eq, N, Signature> {
	graph: VoteGraph<H, N, VoteWeight>, // DAG of blocks which have been voted on.
	prevote: VoteTracker<Id, Prevote<H, N>, Signature>, // tracks prevotes that have been counted
	precommit: VoteTracker<Id, Precommit<H, N>, Signature>, // tracks precommits
	round_number: u64,
	voters: VoterSet<Id>,
	total_weight: u64,
	bitfield_context: BitfieldContext,
	prevote_ghost: Option<(H, N)>, // current memoized prevote-GHOST block
	precommit_ghost: Option<(H, N)>, // current memoized precommit-GHOST block
	finalized: Option<(H, N)>, // best finalized block in this round.
	estimate: Option<(H, N)>, // current memoized round-estimate
	completable: bool, // whether the round is completable
}

impl<Id, H, N, Signature> Round<Id, H, N, Signature> where
	Id: Hash + Clone + Eq + ::std::fmt::Debug,
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + ::std::fmt::Debug + BlockNumberOps,
	Signature: Eq + Clone,
{
	/// Create a new round accumulator for given round number and with given weight.
	/// Not guaranteed to work correctly unless total_weight more than 3x larger than faulty_weight
	pub fn new(round_params: RoundParams<Id, H, N>) -> Self {
		let (base_hash, base_number) = round_params.base;
		let total_weight = round_params.voters.total_weight();
		let n_validators = round_params.voters.len();

		Round {
			round_number: round_params.round_number,
			total_weight,
			voters: round_params.voters,
			graph: VoteGraph::new(base_hash, base_number),
			prevote: VoteTracker::new(),
			precommit: VoteTracker::new(),
			bitfield_context: BitfieldContext::new(n_validators),
			prevote_ghost: None,
			precommit_ghost: None,
			finalized: None,
			estimate: None,
			completable: false,
		}
	}

	/// Return the round number.
	pub fn number(&self) -> u64 {
		self.round_number
	}

	/// Import a prevote. Returns an equivocation proof if the vote is an equivocation.
	///
	/// Ignores duplicate prevotes (not equivocations).
	pub fn import_prevote<C: Chain<H, N>>(
		&mut self,
		chain: &C,
		vote: Prevote<H, N>,
		signer: Id,
		signature: Signature,
	) -> Result<Option<Equivocation<Id, Prevote<H, N>, Signature>>, crate::Error> {
		let info = match self.voters.info(&signer) {
			Some(info) => info,
			None => return Ok(None),
		};
		let weight = info.weight();

		let equivocation = {
			let multiplicity = match self.prevote.add_vote(signer.clone(), vote, signature, weight) {
				Some(m) => m,
				None => return Ok(None),
			};
			let round_number = self.round_number;

			match multiplicity {
				VoteMultiplicity::Single(ref vote, _) => {
					let vote_weight = VoteWeight {
						bitfield: self.bitfield_context.prevote_bitfield(info)
							.expect("info is instantiated from same voter set as context; qed"),
					};

					self.graph.insert(
						vote.target_hash.clone(),
						vote.target_number,
						vote_weight,
						chain,
					)?;

					None
				}
				VoteMultiplicity::Equivocated(ref first, ref second) => {
					// mark the equivocator as such. no need to "undo" the first vote.
					self.bitfield_context.equivocated_prevote(info)
						.expect("info is instantiated from same voter set as bitfield; qed");

					Some(Equivocation {
						round_number,
						identity: signer,
						first: first.clone(),
						second: second.clone(),
					})
				}
			}
		};

		// update prevote-GHOST
		let threshold = self.threshold();
		if self.prevote.current_weight >= threshold {
			let equivocators = self.bitfield_context.equivocators().read();

			self.prevote_ghost = self.graph.find_ghost(
				self.prevote_ghost.take(),
				|v| v.total_weight(&equivocators, &self.voters).prevote >= threshold,
			);
		}

		self.update();
		Ok(equivocation)
	}

	/// Import a precommit. Returns an equivocation proof if the vote is an
	/// equivocation.
	///
	/// Ignores duplicate precommits (not equivocations).
	pub fn import_precommit<C: Chain<H, N>>(
		&mut self,
		chain: &C,
		vote: Precommit<H, N>,
		signer: Id,
		signature: Signature,
	) -> Result<Option<Equivocation<Id, Precommit<H, N>, Signature>>, crate::Error> {
		let info = match self.voters.info(&signer) {
			Some(info) => info,
			None => return Ok(None),
		};
		let weight = info.weight();

		let equivocation = {
			let multiplicity = match self.precommit.add_vote(signer.clone(), vote, signature, weight) {
				Some(m) => m,
				None => return Ok(None),
			};
			let round_number = self.round_number;

			match multiplicity {
				VoteMultiplicity::Single(ref vote, _) => {
					let vote_weight = VoteWeight {
						bitfield: self.bitfield_context.precommit_bitfield(info)
							.expect("info is instantiated from same voter set as context; qed"),
					};

					self.graph.insert(
						vote.target_hash.clone(),
						vote.target_number,
						vote_weight,
						chain,
					)?;

					None
				}
				VoteMultiplicity::Equivocated(ref first, ref second) => {
					// mark the equivocator as such. no need to "undo" the first vote.
					self.bitfield_context.equivocated_precommit(info)
						.expect("info is instantiated from same voter set as bitfield; qed");

					Some(Equivocation {
						round_number,
						identity: signer,
						first: first.clone(),
						second: second.clone(),
					})
				}
			}
		};

		self.update();
		Ok(equivocation)
	}

	// Get current
	pub fn state(&self) -> State<H, N> {
		State {
			prevote_ghost: self.prevote_ghost.clone(),
			finalized: self.finalized.clone(),
			estimate: self.estimate.clone(),
			completable: self.completable,
		}
	}

	/// Compute and cache the precommit-GHOST.
	pub fn precommit_ghost(&mut self) -> Option<(H, N)> {
		// update precommit-GHOST
		let threshold = self.threshold();
		if self.precommit.current_weight >= threshold {
			let equivocators = self.bitfield_context.equivocators().read();

			self.precommit_ghost = self.graph.find_ghost(
				self.precommit_ghost.take(),
				|v| v.total_weight(&equivocators, &self.voters).precommit >= threshold,
			);
		}

		self.precommit_ghost.clone()
	}

	/// Returns an iterator of all precommits targeting the finalized hash.
	///
	/// Only returns `None` if no block has been finalized in this round.
	pub fn finalizing_precommits<'a, C: 'a + Chain<H, N>>(&'a mut self, chain: &'a C)
		-> Option<impl Iterator<Item=crate::SignedPrecommit<H, N, Signature, Id>> + 'a>
	{
		struct YieldVotes<'b, V: 'b, S: 'b> {
			yielded: usize,
			multiplicity: &'b VoteMultiplicity<V, S>,
		}

		impl<'b, V: 'b + Clone, S: 'b + Clone> Iterator for YieldVotes<'b, V, S> {
			type Item = (V, S);

			fn next(&mut self) -> Option<(V, S)> {
				match *self.multiplicity {
					VoteMultiplicity::Single(ref v, ref s) => {
						if self.yielded == 0 {
							self.yielded += 1;
							Some((v.clone(), s.clone()))
						} else {
							None
						}
					}
					VoteMultiplicity::Equivocated(ref a, ref b) => {
						let res = match self.yielded {
							0 => Some(a.clone()),
							1 => Some(b.clone()),
							_ => None,
						};

						self.yielded += 1;
						res
					}
				}
			}
		}

		let (f_hash, _f_num) = self.finalized.clone()?;
		let find_valid_precommits = self.precommit.votes.iter()
			.filter(move |&(_id, ref multiplicity)| {
				if let VoteMultiplicity::Single(ref v, _) = *multiplicity {
					// if there is a single vote from this voter, we only include it
					// if it branches off of the target.
					chain.is_equal_or_descendent_of(f_hash.clone(), v.target_hash.clone())
				} else {
					// equivocations count for everything, so we always include them.
					true
				}
			})
			.flat_map(|(id, multiplicity)| {
				let yield_votes = YieldVotes { yielded: 0, multiplicity };

				yield_votes.map(move |(v, s)| crate::SignedPrecommit {
					precommit: v,
					signature: s,
					id: id.clone(),
				})
			});

		Some(find_valid_precommits)
	}

	// update the round-estimate and whether the round is completable.
	fn update(&mut self) {
		let threshold = self.threshold();
		if self.prevote.current_weight < threshold { return }

		let remaining_commit_votes = self.total_weight - self.precommit.current_weight;
		let equivocators = self.bitfield_context.equivocators().read();
		let equivocators = &*equivocators;

		let voters = &self.voters;


		let (g_hash, g_num) = match self.prevote_ghost.clone() {
			None => return,
			Some(x) => x,
		};

		// anything new finalized? finalized blocks are those which have both
		// 2/3+ prevote and precommit weight.
		let threshold = self.threshold();
		let current_precommits = self.precommit.current_weight;
		if current_precommits >= threshold {
			self.finalized = self.graph.find_ancestor(
				g_hash.clone(),
				g_num,
				|v| v.total_weight(&equivocators, voters).precommit >= threshold,
			);
		};

		// figuring out whether a block can still be committed for is
		// not straightforward because we have to account for all possible future
		// equivocations and thus cannot discount weight from validators who
		// have already voted.
		let possible_to_precommit = {
			let tolerated_equivocations = self.total_weight - threshold;

			// find how many more equivocations we could still get.
			//
			// it is only important to consider the voters whose votes
			// we have already seen, because we are assuming any votes we
			// haven't seen will target this block.
			let current_equivocations = equivocators
				.total_weight(|idx| self.voters.weight_by_index(idx).unwrap_or(0))
				.1;

			let additional_equiv = tolerated_equivocations.saturating_sub(current_equivocations);

			move |weight: &VoteWeight| {
				// total precommits for this block, including equivocations.
				let precommitted_for = weight.total_weight(&equivocators, voters)
					.precommit;

				// equivocations we could still get are out of those who
				// have already voted, but not on this block.
				let possible_equivocations = std::cmp::min(
					current_precommits.saturating_sub(precommitted_for),
					additional_equiv,
				);

				// all the votes already applied on this block,
				// assuming all remaining actors commit to this block,
				// and that we get further equivocations
				let full_possible_weight = precommitted_for
					.saturating_add(remaining_commit_votes)
					.saturating_add(possible_equivocations);

				full_possible_weight >= threshold
			}
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
	pub fn estimate(&self) -> Option<&(H, N)> {
		self.estimate.as_ref()
	}

	/// Fetch the most recently finalized block.
	pub fn finalized(&self) -> Option<&(H, N)> {
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
	pub fn threshold(&self) -> u64 {
		self.voters.threshold()
	}

	/// Return the round base.
	pub fn base(&self) -> (H, N) {
		self.graph.base()
	}

	/// Return the round voters and weights.
	pub fn voters(&self) -> &VoterSet<Id> {
		&self.voters
	}

	/// Return all imported precommits.
	pub fn precommits(&self) -> Vec<(Id, Precommit<H, N>, Signature)> {
		self.precommit.votes()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::testing::{GENESIS_HASH, DummyChain};

	fn voters() -> VoterSet<&'static str> {
		[
			("Alice", 4),
			("Bob", 7),
			("Eve", 3),
		].iter().cloned().collect()
	}

	#[derive(PartialEq, Eq, Hash, Clone, Debug)]
	struct Signature(&'static str);

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

		// first prevote by eve
		assert!(round.import_prevote(
			&chain,
			Prevote::new("FC", 10),
			"Eve", // 3 on F, E
			Signature("Eve-1"),
		).unwrap().is_none());


		assert!(round.prevote_ghost.is_none());

		// second prevote by eve: comes with equivocation proof
		assert!(round.import_prevote(
			&chain,
			Prevote::new("ED", 10),
			"Eve", // still 3 on E
			Signature("Eve-2"),
		).unwrap().is_some());

		// third prevote: returns nothing.
		assert!(round.import_prevote(
			&chain,
			Prevote::new("F", 7),
			"Eve", // still 3 on F and E
			Signature("Eve-2"),
		).unwrap().is_none());

		// three eves together would be enough.

		assert!(round.prevote_ghost.is_none());

		assert!(round.import_prevote(
			&chain,
			Prevote::new("FA", 8),
			"Bob", // add 7 to FA and you get FA.
			Signature("Bob-1"),
		).unwrap().is_none());

		assert_eq!(round.prevote_ghost, Some(("FA", 8)));
	}

	#[test]
	fn vote_weight_discounts_equivocators() {
		let v: VoterSet<_> = [
			(1, 1),
			(2, 2),
			(3, 3),
			(4, 4),
			(5, 5),
		].iter().cloned().collect();

		let ctx = BitfieldContext::new(5);

		let equivocators = {
			let equiv_a = ctx.prevote_bitfield(v.info(&1).unwrap()).unwrap();
			let equiv_b = ctx.prevote_bitfield(v.info(&5).unwrap()).unwrap();

			equiv_a.merge(&equiv_b).unwrap()
		};

		let votes = {
			let vote_a = ctx.prevote_bitfield(v.info(&1).unwrap()).unwrap();
			let vote_b = ctx.prevote_bitfield(v.info(&2).unwrap()).unwrap();
			let vote_c = ctx.prevote_bitfield(v.info(&3).unwrap()).unwrap();

			vote_a.merge(&vote_b).unwrap().merge(&vote_c).unwrap()
		};

		let weight = VoteWeight { bitfield: votes };
		let vote_weight = weight.total_weight(&equivocators, &v);

		// counts the prevotes from 2, 3, and the equivocations from 1, 5 without
		// double-counting 1
		assert_eq!(vote_weight, TotalWeight { prevote: 1 + 5 + 2 + 3, precommit: 0 });

		let votes = weight.bitfield.merge(&ctx.prevote_bitfield(v.info(&5).unwrap()).unwrap()).unwrap();

		let weight = VoteWeight { bitfield: votes };
		let vote_weight = weight.total_weight(&equivocators, &v);


		// adding an extra vote by 5 doesn't increase the count.
		assert_eq!(vote_weight, TotalWeight { prevote: 1 + 5 + 2 + 3, precommit: 0 });
	}
}
