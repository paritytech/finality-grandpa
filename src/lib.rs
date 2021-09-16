// Copyright 2018-2019 Parity Technologies (UK) Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Finality gadget for blockchains.
//!
//! <https://github.com/w3f/consensus/blob/master/pdf/grandpa.pdf>
//!
//! Consensus proceeds in rounds. Each round, voters will cast a prevote
//! and precommit message.
//!
//! Votes on blocks are then applied to the blockchain, being recursively applied to
//! blocks before them. A DAG is superimposed over the blockchain with the `vote_graph` logic.
//!
//! Equivocation detection and vote-set management is done in the `round` module.
//! The work for actually casting votes is done in the `voter` module.

#![warn(missing_docs)]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub mod round;
pub mod vote_graph;
#[cfg(feature = "std")]
pub mod voter;
pub mod voter_set;

mod bitfield;
#[cfg(feature = "std")]
mod bridge_state;
#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod fuzz_helpers;
#[cfg(any(test))]
mod testing;
mod weights;
#[cfg(not(feature = "std"))]
mod std {
	pub use core::{cmp, hash, iter, mem, num, ops};

	pub mod vec {
		pub use alloc::vec::Vec;
	}

	pub mod collections {
		pub use alloc::collections::{
			btree_map::{self, BTreeMap},
			btree_set::{self, BTreeSet},
		};
	}

	pub mod fmt {
		pub use core::fmt::{Display, Formatter, Result};

		pub trait Debug {}
		impl<T> Debug for T {}
	}
}

use crate::{std::vec::Vec, voter_set::VoterSet};
#[cfg(feature = "derive-codec")]
use parity_scale_codec::{Decode, Encode};
use round::ImportResult;
#[cfg(feature = "derive-codec")]
use scale_info::TypeInfo;

/// A prevote for a block and its ancestors.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct Prevote<H, N> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number.
	pub target_number: N,
}

impl<H, N> Prevote<H, N> {
	/// Create a new prevote for the given block (hash and number).
	pub fn new(target_hash: H, target_number: N) -> Self {
		Prevote { target_hash, target_number }
	}
}

/// A precommit for a block and its ancestors.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct Precommit<H, N> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number
	pub target_number: N,
}

impl<H, N> Precommit<H, N> {
	/// Create a new precommit for the given block (hash and number).
	pub fn new(target_hash: H, target_number: N) -> Self {
		Precommit { target_hash, target_number }
	}
}

/// A primary proposed block, this is a broadcast of the last round's estimate.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct PrimaryPropose<H, N> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number
	pub target_number: N,
}

impl<H, N> PrimaryPropose<H, N> {
	/// Create a new primary proposal for the given block (hash and number).
	pub fn new(target_hash: H, target_number: N) -> Self {
		PrimaryPropose { target_hash, target_number }
	}
}

/// Top-level error type used by this crate.
#[derive(Clone, PartialEq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
pub enum Error {
	/// The block is not a descendent of the given base block.
	NotDescendent,
}

#[cfg(feature = "std")]
impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match *self {
			Error::NotDescendent => write!(f, "Block not descendent of base"),
		}
	}
}

#[cfg(feature = "std")]
impl std::error::Error for Error {
	fn description(&self) -> &str {
		match *self {
			Error::NotDescendent => "Block not descendent of base",
		}
	}
}

/// Arithmetic necessary for a block number.
pub trait BlockNumberOps:
	std::fmt::Debug
	+ std::cmp::Ord
	+ std::ops::Add<Output = Self>
	+ std::ops::Sub<Output = Self>
	+ num::One
	+ num::Zero
	+ num::AsPrimitive<usize>
{
}

impl<T> BlockNumberOps for T
where
	T: std::fmt::Debug,
	T: std::cmp::Ord,
	T: std::ops::Add<Output = Self>,
	T: std::ops::Sub<Output = Self>,
	T: num::One,
	T: num::Zero,
	T: num::AsPrimitive<usize>,
{
}

/// Chain context necessary for implementation of the finality gadget.
pub trait Chain<H: Eq, N: Copy + BlockNumberOps> {
	/// Get the ancestry of a block up to but not including the base hash.
	/// Should be in reverse order from `block`'s parent.
	///
	/// If the block is not a descendent of `base`, returns an error.
	fn ancestry(&self, base: H, block: H) -> Result<Vec<H>, Error>;

	/// Returns true if `block` is a descendent of or equal to the given `base`.
	fn is_equal_or_descendent_of(&self, base: H, block: H) -> bool {
		if base == block {
			return true
		}

		// TODO: currently this function always succeeds since the only error
		// variant is `Error::NotDescendent`, this may change in the future as
		// other errors (e.g. IO) are not being exposed.
		match self.ancestry(base, block) {
			Ok(_) => true,
			Err(Error::NotDescendent) => false,
		}
	}
}

/// An equivocation (double-vote) in a given round.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct Equivocation<Id, V, S> {
	/// The round number equivocated in.
	pub round_number: u64,
	/// The identity of the equivocator.
	pub identity: Id,
	/// The first vote in the equivocation.
	pub first: (V, S),
	/// The second vote in the equivocation.
	pub second: (V, S),
}

/// A protocol message or vote.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub enum Message<H, N> {
	/// A prevote message.
	#[cfg_attr(feature = "derive-codec", codec(index = 0))]
	Prevote(Prevote<H, N>),
	/// A precommit message.
	#[cfg_attr(feature = "derive-codec", codec(index = 1))]
	Precommit(Precommit<H, N>),
	/// A primary proposal message.
	#[cfg_attr(feature = "derive-codec", codec(index = 2))]
	PrimaryPropose(PrimaryPropose<H, N>),
}

impl<H, N: Copy> Message<H, N> {
	/// Get the target block of the vote.
	pub fn target(&self) -> (&H, N) {
		match *self {
			Message::Prevote(ref v) => (&v.target_hash, v.target_number),
			Message::Precommit(ref v) => (&v.target_hash, v.target_number),
			Message::PrimaryPropose(ref v) => (&v.target_hash, v.target_number),
		}
	}
}

/// A signed message.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct SignedMessage<H, N, S, Id> {
	/// The internal message which has been signed.
	pub message: Message<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer
	pub id: Id,
}

impl<H, N, S, Id> Unpin for SignedMessage<H, N, S, Id> {}

impl<H, N: Copy, S, Id> SignedMessage<H, N, S, Id> {
	/// Get the target block of the vote.
	pub fn target(&self) -> (&H, N) {
		self.message.target()
	}
}

/// A commit message which is an aggregate of precommits.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct Commit<H, N, S, Id> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number.
	pub target_number: N,
	/// Precommits for target block or any block after it that justify this commit.
	pub precommits: Vec<SignedPrecommit<H, N, S, Id>>,
}

/// A signed prevote message.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct SignedPrevote<H, N, S, Id> {
	/// The prevote message which has been signed.
	pub prevote: Prevote<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer.
	pub id: Id,
}

/// A signed precommit message.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct SignedPrecommit<H, N, S, Id> {
	/// The precommit message which has been signed.
	pub precommit: Precommit<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer.
	pub id: Id,
}

/// A commit message with compact representation of authentication data.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct CompactCommit<H, N, S, Id> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number.
	pub target_number: N,
	/// Precommits for target block or any block after it that justify this commit.
	pub precommits: Vec<Precommit<H, N>>,
	/// Authentication data for the commit.
	pub auth_data: MultiAuthData<S, Id>,
}

/// A catch-up message, which is an aggregate of prevotes and precommits necessary
/// to complete a round.
///
/// This message contains a "base", which is a block all of the vote-targets are
/// a descendent of.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct CatchUp<H, N, S, Id> {
	/// Round number.
	pub round_number: u64,
	/// Prevotes for target block or any block after it that justify this catch-up.
	pub prevotes: Vec<SignedPrevote<H, N, S, Id>>,
	/// Precommits for target block or any block after it that justify this catch-up.
	pub precommits: Vec<SignedPrecommit<H, N, S, Id>>,
	/// The base hash. See struct docs.
	pub base_hash: H,
	/// The base number. See struct docs.
	pub base_number: N,
}

/// Authentication data for a set of many messages, currently a set of precommit signatures but
/// in the future could be optimized with BLS signature aggregation.
pub type MultiAuthData<S, Id> = Vec<(S, Id)>;

impl<H, N, S, Id> From<CompactCommit<H, N, S, Id>> for Commit<H, N, S, Id> {
	fn from(commit: CompactCommit<H, N, S, Id>) -> Commit<H, N, S, Id> {
		Commit {
			target_hash: commit.target_hash,
			target_number: commit.target_number,
			precommits: commit
				.precommits
				.into_iter()
				.zip(commit.auth_data.into_iter())
				.map(|(precommit, (signature, id))| SignedPrecommit { precommit, signature, id })
				.collect(),
		}
	}
}

impl<H: Clone, N: Clone, S, Id> From<Commit<H, N, S, Id>> for CompactCommit<H, N, S, Id> {
	fn from(commit: Commit<H, N, S, Id>) -> CompactCommit<H, N, S, Id> {
		CompactCommit {
			target_hash: commit.target_hash,
			target_number: commit.target_number,
			precommits: commit.precommits.iter().map(|signed| signed.precommit.clone()).collect(),
			auth_data: commit
				.precommits
				.into_iter()
				.map(|signed| (signed.signature, signed.id))
				.collect(),
		}
	}
}

/// Struct returned from `validate_commit` function with information
/// about the validation result.
pub struct CommitValidationResult<H, N> {
	ghost: Option<(H, N)>,
	num_precommits: usize,
	num_duplicated_precommits: usize,
	num_equivocations: usize,
	num_invalid_voters: usize,
}

impl<H, N> CommitValidationResult<H, N> {
	/// Returns the commit GHOST i.e. the block with highest number for which
	/// the cumulative votes of descendents and itself reach finalization
	/// threshold.
	pub fn ghost(&self) -> Option<&(H, N)> {
		self.ghost.as_ref()
	}

	/// Returns the number of precommits in the commit.
	pub fn num_precommits(&self) -> usize {
		self.num_precommits
	}

	/// Returns the number of duplicate precommits in the commit.
	pub fn num_duplicated_precommits(&self) -> usize {
		self.num_duplicated_precommits
	}

	/// Returns the number of equivocated precommits in the commit.
	pub fn num_equivocations(&self) -> usize {
		self.num_equivocations
	}

	/// Returns the number of invalid voters in the commit.
	pub fn num_invalid_voters(&self) -> usize {
		self.num_invalid_voters
	}
}

impl<H, N> Default for CommitValidationResult<H, N> {
	fn default() -> Self {
		CommitValidationResult {
			ghost: None,
			num_precommits: 0,
			num_duplicated_precommits: 0,
			num_equivocations: 0,
			num_invalid_voters: 0,
		}
	}
}

/// Validates a GRANDPA commit message and returns the ghost calculated using
/// the precommits in the commit message and using the commit target as a
/// base.
///
/// Signatures on precommits are assumed to have been checked.
///
/// Duplicate votes or votes from voters not in the voter-set will be ignored, but it is recommended
/// for the caller of this function to remove those at signature-verification time.
pub fn validate_commit<H, N, S, I, C: Chain<H, N>>(
	commit: &Commit<H, N, S, I>,
	voters: &VoterSet<I>,
	chain: &C,
) -> Result<CommitValidationResult<H, N>, crate::Error>
where
	H: Clone + Eq + Ord + std::fmt::Debug,
	N: Copy + BlockNumberOps + std::fmt::Debug,
	I: Clone + Ord + Eq + std::fmt::Debug,
	S: Clone + Eq,
{
	let mut validation_result =
		CommitValidationResult { num_precommits: commit.precommits.len(), ..Default::default() };

	// check that all precommits are for blocks higher than the target
	// commit block, and that they're its descendents
	let all_precommits_higher_than_target = commit.precommits.iter().all(|signed| {
		signed.precommit.target_number >= commit.target_number &&
			chain.is_equal_or_descendent_of(
				commit.target_hash.clone(),
				signed.precommit.target_hash.clone(),
			)
	});

	if !all_precommits_higher_than_target {
		return Ok(validation_result)
	}

	let mut equivocated = std::collections::BTreeSet::new();

	// Add all precommits to the round with correct counting logic
	// using the commit target as a base.
	let mut round = round::Round::new(round::RoundParams {
		round_number: 0, // doesn't matter here.
		voters: voters.clone(),
		base: (commit.target_hash.clone(), commit.target_number),
	});

	for SignedPrecommit { precommit, id, signature } in &commit.precommits {
		match round.import_precommit(chain, precommit.clone(), id.clone(), signature.clone())? {
			ImportResult { equivocation: Some(_), .. } => {
				validation_result.num_equivocations += 1;
				// allow only one equivocation per voter, as extras are redundant.
				if !equivocated.insert(id) {
					return Ok(validation_result)
				}
			},
			ImportResult { duplicated, valid_voter, .. } => {
				if duplicated {
					validation_result.num_duplicated_precommits += 1;
				}
				if !valid_voter {
					validation_result.num_invalid_voters += 1;
				}
			},
		}
	}

	// if a ghost is found then it must be equal or higher than the commit
	// target, otherwise the commit is invalid
	validation_result.ghost = round.precommit_ghost();
	Ok(validation_result)
}

/// Runs the callback with the appropriate `CommitProcessingOutcome` based on
/// the given `CommitValidationResult`. Outcome is bad if ghost is undefined,
/// good otherwise.
#[cfg(feature = "std")]
pub fn process_commit_validation_result<H, N>(
	validation_result: CommitValidationResult<H, N>,
	mut callback: voter::Callback<voter::CommitProcessingOutcome>,
) {
	if validation_result.ghost.is_some() {
		callback.run(voter::CommitProcessingOutcome::Good(voter::GoodCommit::new()))
	} else {
		callback.run(voter::CommitProcessingOutcome::Bad(voter::BadCommit::from(validation_result)))
	}
}

/// Historical votes seen in a round.
#[derive(Default, Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "std", test), derive(Debug))]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode, TypeInfo))]
pub struct HistoricalVotes<H, N, S, Id> {
	seen: Vec<SignedMessage<H, N, S, Id>>,
	prevote_idx: Option<u64>,
	precommit_idx: Option<u64>,
}

impl<H, N, S, Id> HistoricalVotes<H, N, S, Id> {
	/// Create a new HistoricalVotes.
	pub fn new() -> Self {
		HistoricalVotes { seen: Vec::new(), prevote_idx: None, precommit_idx: None }
	}

	/// Create a new HistoricalVotes initialized from the parameters.
	pub fn new_with(
		seen: Vec<SignedMessage<H, N, S, Id>>,
		prevote_idx: Option<u64>,
		precommit_idx: Option<u64>,
	) -> Self {
		HistoricalVotes { seen, prevote_idx, precommit_idx }
	}

	/// Push a vote into the list. The value of `self` before this call
	/// is considered to be a prefix of the value post-call.
	pub fn push_vote(&mut self, msg: SignedMessage<H, N, S, Id>) {
		self.seen.push(msg)
	}

	/// Return the messages seen so far.
	pub fn seen(&self) -> &[SignedMessage<H, N, S, Id>] {
		&self.seen
	}

	/// Return the number of messages seen before prevoting.
	/// None in case we didn't prevote yet.
	pub fn prevote_idx(&self) -> Option<u64> {
		self.prevote_idx
	}

	/// Return the number of messages seen before precommiting.
	/// None in case we didn't precommit yet.
	pub fn precommit_idx(&self) -> Option<u64> {
		self.precommit_idx
	}

	/// Set the number of messages seen before prevoting.
	pub fn set_prevoted_idx(&mut self) {
		self.prevote_idx = Some(self.seen.len() as u64)
	}

	/// Set the number of messages seen before precommiting.
	pub fn set_precommitted_idx(&mut self) {
		self.precommit_idx = Some(self.seen.len() as u64)
	}
}

#[cfg(test)]
mod tests {

	#[cfg(feature = "derive-codec")]
	#[test]
	fn codec_was_derived() {
		use parity_scale_codec::{Decode, Encode};

		let signed = crate::SignedMessage {
			message: crate::Message::Prevote(crate::Prevote {
				target_hash: b"Hello".to_vec(),
				target_number: 5,
			}),
			signature: b"Signature".to_vec(),
			id: 5000,
		};

		let encoded = signed.encode();
		let signed2 = crate::SignedMessage::decode(&mut &encoded[..]).unwrap();
		assert_eq!(signed, signed2);
	}
}
