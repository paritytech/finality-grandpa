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

extern crate parking_lot;
extern crate num_traits as num;

#[cfg_attr(feature = "std", macro_use)]
extern crate futures;
#[cfg_attr(feature = "std", macro_use)]
extern crate log;

#[cfg(test)]
extern crate exit_future;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate tokio;

#[cfg(feature = "derive-codec")]
#[macro_use]
extern crate parity_codec_derive;

#[cfg(feature = "derive-codec")]
extern crate parity_codec;

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

pub use primitives::{
	Prevote, Precommit, Equivocation, Message, PrimaryPropose, Error as GrandpaError
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

// #[cfg(feature = "std")]
// impl std::error::Error for Error {
// 	fn description(&self) -> &str {
// 		match *self {
// 			Error::NotDescendent => "Block not descendent of base",
// 		}
// 	}
// }

/// Arithmetic necessary for a block number.
pub trait BlockNumberOps:
	std::fmt::Debug +
	std::cmp::Ord +
	std::ops::Add<Output=Self> +
	std::ops::Sub<Output=Self> +
	crate::num::One +
	crate::num::Zero +
	crate::num::AsPrimitive<usize>
{}

impl<T> BlockNumberOps for T where
	T: std::fmt::Debug,
	T: std::cmp::Ord,
	T: std::ops::Add<Output=Self>,
	T: std::ops::Sub<Output=Self>,
	T: crate::num::One,
	T: crate::num::Zero,
	T: crate::num::AsPrimitive<usize>,
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
	pub auth_data: CommitAuthData<S, Id>,
}

/// Authentication data for a commit, currently a set of precommit signatures but
/// in the future could be optimized with BLS signature aggregation.
pub type CommitAuthData<S, Id> = Vec<(S, Id)>;

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
	mut callback: voter::Callback,
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
