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

use vote_graph::VoteGraph;

use std::collections::hash_map::{HashMap, Entry};
use std::hash::Hash;
use std::ops::AddAssign;

use bitfield::{Bitfield, Shared as BitfieldContext, LiveBitfield};

use super::{Equivocation, Prevote, Precommit, Chain, BlockNumberOps};

#[derive(Hash, Eq, PartialEq)]
struct Address;

#[derive(Debug, Clone)]
struct VoteWeight<Id> {
	prevote: u64,
	precommit: u64,
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

impl<Vote: Eq, Signature: Eq> VoteMultiplicity<Vote, Signature> {
	fn update_graph<Id, F, G>(
		&self,
		weight: u64,
		mut import: F,
		make_bitfield: G,
	) -> Result<(), ::Error> where
		F: FnMut(&Vote, u64, Bitfield<Id>) -> Result<(), ::Error>,
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

	fn contains(&self, vote: &Vote, signature: &Signature) -> bool {
		match *self {
			VoteMultiplicity::Single(ref v, ref s) =>
				v == vote && s == signature,
			VoteMultiplicity::Equivocated((ref v1, ref s1), (ref v2, ref s2)) => {
				v1 == vote && s1 == signature ||
					v2 == vote && s2 == signature
			},
			VoteMultiplicity::EquivocatedMany(ref v) => {
				v.iter().any(|(ref v, ref s)| v == vote && s == signature)
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
	// if the vote is an equivocation, returns a value indicating
	// it as such (the new vote is always the last in the multiplicity).
	//
	// since this struct doesn't track the round-number of votes, that must be set
	// by the caller.
	fn add_vote(&mut self, id: Id, vote: Vote, signature: Signature, weight: u64)
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

	fn maybe_add_vote(&mut self, id: Id, vote: Vote, signature: Signature, weight: u64)
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

				Some(&*occupied.into_mut())
			}
		}
	}

	fn valid_votes<'a>(&'a self) -> impl Iterator<Item=(Id, Vote, Signature)> + 'a {
		self.votes.iter().filter_map(|(id, vote)| {
			match vote {
				VoteMultiplicity::Single(v, s) => Some((id.clone(), v.clone(), s.clone())),
				_ => None,
			}
		})
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
	pub voters: HashMap<Id, u64>,
	/// The base block to build on.
	pub base: (H, N),
}

/// Stores data for a round.
pub struct Round<Id: Hash + Eq, H: Hash + Eq, N, Signature> {
	graph: VoteGraph<H, N, VoteWeight<Id>>, // DAG of blocks which have been voted on.
	prevote: VoteTracker<Id, Prevote<H, N>, Signature>, // tracks prevotes that have been counted
	precommit: VoteTracker<Id, Precommit<H, N>, Signature>, // tracks precommits
	round_number: u64,
	voters: HashMap<Id, u64>,
	threshold_weight: u64,
	total_weight: u64,
	bitfield_context: BitfieldContext<Id>,
	prevote_ghost: Option<(H, N)>, // current memoized prevote-GHOST block
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
		let total_weight: u64 = round_params.voters.values().cloned().sum();
		let threshold_weight = threshold(total_weight);
		let n_validators = round_params.voters.len();

		Round {
			round_number: round_params.round_number,
			threshold_weight,
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

	/// Return the round number.
	pub fn number(&self) -> u64 {
		self.round_number
	}

	/// Import a prevote. Returns an equivocation proof if the vote is an equivocation.
	///
	/// Should not import the same prevote more than once.
	pub fn import_prevote<C: Chain<H, N>>(
		&mut self,
		chain: &C,
		vote: Prevote<H, N>,
		signer: Id,
		signature: Signature,
	) -> Result<Option<Equivocation<Id, Prevote<H, N>, Signature>>, ::Error> {
		let weight = match self.voters.get(&signer) {
			Some(weight) => *weight,
			None => return Ok(None),
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
						vote.target_number,
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

		self.update();
		Ok(equivocation)
	}

	/// Import a precommit. Returns an equivocation proof if the vote is an
	/// equivocation.
	///
	/// Should not import the same precommit more than once.
	pub fn import_precommit<C: Chain<H, N>>(
		&mut self,
		chain: &C,
		vote: Precommit<H, N>,
		signer: Id,
		signature: Signature,
		maybe_duplicate: bool,
	) -> Result<Option<Equivocation<Id, Precommit<H, N>, Signature>>, ::Error> {
		let weight = match self.voters.get(&signer) {
			Some(weight) => *weight,
			None => return Ok(None),
		};

		let equivocation = {
			let graph = &mut self.graph;
			let bitfield_context = &self.bitfield_context;
			let multiplicity = if maybe_duplicate {
				match self.precommit.maybe_add_vote(signer.clone(), vote, signature, weight) {
					Some(m) => m,
					_ => return Ok(None),
				}
			} else {
				self.precommit.add_vote(signer.clone(), vote, signature, weight)
			};
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
						vote.target_number,
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

	// update the round-estimate and whether the round is completable.
	fn update(&mut self) {
		let threshold = self.threshold();
		if self.prevote.current_weight < threshold { return }

		let remaining_commit_votes = self.total_weight - self.precommit.current_weight;
		let (g_hash, g_num) = match self.prevote_ghost.clone() {
			None => return,
			Some(x) => x,
		};

		// anything new finalized? finalized blocks are those which have both
		// 2/3+ prevote and precommit weight.
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
		self.threshold_weight
	}

	/// Return the round base.
	pub fn base(&self) -> (H, N) {
		self.graph.base()
	}

	pub fn weight(&self, signer: &Id) -> Option<u64> {
		self.voters.get(signer).cloned()
	}

	pub fn valid_precommits<'a>(&'a self) -> impl Iterator<Item=(Id, Precommit<H, N>, Signature)> + 'a {
		self.precommit.valid_votes()
	}
}

fn threshold(total_weight: u64) -> u64 {
	let faulty = total_weight.saturating_sub(1) / 3;
	total_weight - faulty
}

#[cfg(test)]
mod tests {
	use super::*;
	use testing::{GENESIS_HASH, DummyChain};

	fn voters() -> HashMap<&'static str, u64> {
		[
			("Alice", 4),
			("Bob", 7),
			("Eve", 3),
		].iter().cloned().collect()
	}

	#[derive(PartialEq, Eq, Hash, Clone, Debug)]
	struct Signature(&'static str);

	#[test]
	fn threshold_is_right() {
		assert_eq!(threshold(3), 3);
		assert_eq!(threshold(4), 3);
		assert_eq!(threshold(5), 4);
		assert_eq!(threshold(6), 5);
		assert_eq!(threshold(7), 5);
		assert_eq!(threshold(10), 7);
		assert_eq!(threshold(100), 67);
		assert_eq!(threshold(101), 68);
		assert_eq!(threshold(102), 69);
		assert_eq!(threshold(103), 69);
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
			"Eve", // 3 on F, E
			Signature("Eve-1"),
		).unwrap().is_none());


		assert!(round.prevote_ghost.is_none());

		assert!(round.import_prevote(
			&chain,
			Prevote::new("ED", 10),
			"Eve", // still 3 on E
			Signature("Eve-2"),
		).unwrap().is_some());

		assert!(round.import_prevote(
			&chain,
			Prevote::new("F", 7),
			"Eve", // still 3 on F and E
			Signature("Eve-2"),
		).unwrap().is_some());

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
}
