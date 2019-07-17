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
//! https://github.com/w3f/consensus/blob/master/pdf/grandpa.pdf
//!
//! Consensus proceeds in rounds. Each round, voters will cast a prevote
//! and precommit message.
//!
//! Votes on blocks are then applied to the blockchain, being recursively applied to
//! blocks before them. A DAG is superimposed over the blockchain with the `vote_graph` logic.
//!
//! Equivocation detection and vote-set management is done in the `round` module.
//! The work for actually casting votes is done in the `voter` module.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(not(feature = "std"), feature(alloc))]

#[cfg(not(feature = "std"))]
extern crate core as std;

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

pub mod bitfield;
pub mod round;
pub mod vote_graph;
pub mod voter_set;

#[cfg(feature = "std")]
pub mod voter;

#[cfg(feature = "std")]
mod bridge_state;

#[cfg(test)]
mod testing;

use collections::Vec;
use std::fmt;
use crate::voter_set::VoterSet;
use round::ImportResult;
#[cfg(feature = "derive-codec")]
use parity_codec::{Encode, Decode};

pub use primitives::{
	Prevote, Precommit, Equivocation, Message, PrimaryPropose,
	Error as GrandpaError, CommitValidationResult
};

#[cfg(not(feature = "std"))]
mod collections {
	pub use alloc::collections::*;
	pub use alloc::vec::Vec;
	pub use hashmap_core::{map as hash_map, HashMap, HashSet};
}

#[cfg(feature = "std")]
mod collections {
	pub use std::collections::*;
	pub use std::vec::Vec;
}

/// Get the threshold weight given the total voting weight.
pub fn threshold(total_weight: u64) -> u64 {
	let faulty = total_weight.saturating_sub(1) / 3;
	total_weight - faulty
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	NotDescendent,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
	std::fmt::Debug +
	std::cmp::Ord +
	std::ops::Add<Output=Self> +
	std::ops::Sub<Output=Self> +
	num::One +
	num::Zero +
	num::AsPrimitive<usize>
{}

impl<T> BlockNumberOps for T where
	T: std::fmt::Debug,
	T: std::cmp::Ord,
	T: std::ops::Add<Output=Self>,
	T: std::ops::Sub<Output=Self>,
	T: num::One,
	T: num::Zero,
	T: num::AsPrimitive<usize>,
{}

/// Chain context necessary for implementation of the finality gadget.
pub trait Chain<H: Eq, N: Copy + BlockNumberOps> {
	/// Get the ancestry of a block up to but not including the base hash.
	/// Should be in reverse order from `block`'s parent.
	///
	/// If the block is not a descendent of `base`, returns an error.
	fn ancestry(&self, base: H, block: H) -> Result<Vec<H>, Error>;

	/// Return the hash of the best block whose chain contains the given block hash,
	/// even if that block is `base` itself.
	///
	/// If `base` is unknown, return `None`.
	fn best_chain_containing(&self, base: H) -> Option<(H, N)>;

	/// Returns true if `block` is a descendent of or equal to the given `base`.
	fn is_equal_or_descendent_of(&self, base: H, block: H) -> bool {
		if base == block { return true; }

		// TODO: currently this function always succeeds since the only error
		// variant is `Error::NotDescendent`, this may change in the future as
		// other errors (e.g. IO) are not being exposed.
		match self.ancestry(base, block) {
			Ok(_) => true,
			Err(Error::NotDescendent) => false,
		}
	}
}

/// A signed message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct SignedMessage<H, N, S, Id> {
	/// The internal message which has been signed.
	pub message: Message<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer
	pub id: Id,
}

impl<H, N: Copy, S, Id> SignedMessage<H, N, S, Id> {
	/// Get the target block of the vote.
	pub fn target(&self) -> (&H, N) {
		self.message.target()
	}
}

/// A commit message which is an aggregate of precommits.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct Commit<H, N, S, Id> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number.
	pub target_number: N,
	/// Precommits for target block or any block after it that justify this commit.
	pub precommits: Vec<SignedPrecommit<H, N, S, Id>>,
}

/// A signed prevote message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct SignedPrevote<H, N, S, Id> {
	/// The prevote message which has been signed.
	pub prevote: Prevote<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer.
	pub id: Id,
}

/// A signed precommit message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct SignedPrecommit<H, N, S, Id> {
	/// The precommit message which has been signed.
	pub precommit: Precommit<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer.
	pub id: Id,
}

/// A commit message with compact representation of authentication data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
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
			precommits: commit.precommits.into_iter()
				.zip(commit.auth_data.into_iter())
				.map(|(precommit, (signature, id))| SignedPrecommit { precommit, signature, id })
				.collect()
		}
	}
}

impl<H: Clone, N: Clone, S, Id> From<Commit<H, N, S, Id>> for CompactCommit<H, N, S, Id> {
	fn from(commit: Commit<H, N, S, Id>) -> CompactCommit<H, N, S, Id> {
		CompactCommit {
			target_hash: commit.target_hash,
			target_number: commit.target_number,
			precommits: commit.precommits.iter().map(|signed| signed.precommit.clone()).collect(),
			auth_data: commit.precommits.into_iter().map(|signed| (signed.signature, signed.id)).collect(),
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
	H: std::hash::Hash + Clone + Eq + Ord + std::fmt::Debug,
	N: Copy + BlockNumberOps + std::fmt::Debug,
	I: Clone + std::hash::Hash + Eq + std::fmt::Debug,
	S: Eq,
{
	let mut validation_result = CommitValidationResult::default();
	validation_result.num_precommits = commit.precommits.len();

	// check that all precommits are for blocks higher than the target
	// commit block, and that they're its descendents
	if !commit.precommits.iter().all(|signed| {
		signed.precommit.target_number >= commit.target_number &&
			chain.is_equal_or_descendent_of(
				commit.target_hash.clone(),
				signed.precommit.target_hash.clone(),
			)
	}) {
		return Ok(validation_result);
	}

	let mut equivocated = crate::collections::HashSet::new();

	// Add all precommits to the round with correct counting logic
	// using the commit target as a base.
	let mut round = round::Round::new(round::RoundParams {
		round_number: 0, // doesn't matter here.
		voters: voters.clone(),
		base: (commit.target_hash.clone(), commit.target_number),
	});

	for SignedPrecommit { precommit, id, signature } in commit.precommits.iter() {
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
			}
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
pub fn process_commit_validation_result<H, N>(
	validation_result: CommitValidationResult<H, N>,
	mut callback: voter::Callback<voter::CommitProcessingOutcome>,
) {
	if let Some(_) = validation_result.ghost {
		callback.run(
			voter::CommitProcessingOutcome::Good(voter::GoodCommit::new())
		)
	} else {
		callback.run(
			voter::CommitProcessingOutcome::Bad(voter::BadCommit::from(validation_result))
		)
	}
}

/// Historical votes seen in a round.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct HistoricalVotes<H, N, S, Id> {
	seen: Vec<SignedMessage<H, N, S, Id>>,
	prevote_index: Option<u32>,
	precommit_index: Option<u32>,
}

impl<H, N, S, Id> HistoricalVotes<H, N, S, Id> {
	/// Create a new HistoricalVotes.
	pub fn new() -> Self {
		HistoricalVotes {
			seen: Vec::new(),
			prevote_index: None,
			precommit_index: None,
		}
	}

	/// Create a new HistoricalVotes initialized from the parameters.
	pub fn new_with(
		seen: Vec<SignedMessage<H, N, S, Id>>,
		prevote_index: Option<u32>,
		precommit_index: Option<u32>
	) -> Self {
		HistoricalVotes {
			seen,
			prevote_index,
			precommit_index,
		}
	}

	/// Push a message into the list.
	pub fn push_vote(&mut self, msg: SignedMessage<H, N, S, Id>) {
		self.seen.push(msg)
	}

	/// Return the messages seen so far.
	pub fn seen(&self) -> &Vec<SignedMessage<H, N, S, Id>> {
		&self.seen
	}

	/// Return the number of messages seen before prevoting.
	/// None in case we didn't prevote yet.
	pub fn prevote_index(&self) -> Option<u32> {
		self.prevote_index
	}

	/// Return the number of messages seen before precommiting.
	/// None in case we didn't precommit yet.
	pub fn precommit_index(&self) -> Option<u32> {
		self.precommit_index
	}

	/// Set the number of messages seen before prevoting.
	pub fn set_prevoted_index(&mut self) {
		self.prevote_index = Some(self.seen.len() as u32)
	}

	/// Set the number of messages seen before precommiting.
	pub fn set_precommited_index(&mut self) {
		self.precommit_index = Some(self.seen.len() as u32)
	}
}

pub trait AccountableSafety {
	type Messages;

	fn prevotes_seen(&self, round: u64) -> Self::Messages;
	fn precommits_seen(&self, round: u64) -> Self::Messages;
}

#[cfg(test)]
mod tests {
	use super::threshold;

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

	#[cfg(feature = "derive-codec")]
	#[test]
	fn codec_was_derived() {
		use parity_codec::{Encode, Decode};

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
