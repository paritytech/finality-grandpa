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

//! Primitives.

#![cfg_attr(not(feature = "std"), no_std)]


extern crate alloc;
extern crate num_traits;

mod voter_set;
mod bitfield;
mod vote_graph;
mod round;

#[cfg(feature = "std")]
use serde::Serialize;
use parity_codec::{Encode, Decode};
use core::fmt;
use alloc::vec::Vec;
use num_traits as num;
pub use voter_set::VoterSet;
use round::ImportResult;

/// A commit message which is an aggregate of precommits.
#[cfg_attr(feature = "std", derive(Serialize))]
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Commit<H, N, S, Id> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number.
	pub target_number: N,
	/// Precommits for target block or any block after it that justify this commit.
	pub precommits: Vec<SignedPrecommit<H, N, S, Id>>,
}

/// Struct returned from `validate_commit` function with information
/// about the validation result.
pub struct CommitValidationResult<H, N> {
	pub ghost: Option<(H, N)>,
	pub num_precommits: usize,
	pub num_duplicated_precommits: usize,
	pub num_equivocations: usize,
	pub num_invalid_voters: usize,
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
	H: core::hash::Hash + Clone + Eq + Ord,
	N: Copy + BlockNumberOps,
	I: Clone + core::hash::Hash + Eq + Ord,
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

	let mut equivocated = alloc::collections::BTreeSet::new();

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


/// A signed precommit message.
#[cfg_attr(feature = "std", derive(Serialize))]
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SignedPrecommit<H, N, S, Id> {
	/// The precommit message which has been signed.
	pub precommit: Precommit<H, N>,
	/// The signature on the message.
	pub signature: S,
	/// The Id of the signer.
	pub id: Id,
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


/// Arithmetic necessary for a block number.
pub trait BlockNumberOps:
	core::cmp::Ord +
	core::ops::Add<Output=Self> +
	core::ops::Sub<Output=Self> +
	crate::num::One +
	crate::num::Zero +
	crate::num::AsPrimitive<usize>
{}

impl<T> BlockNumberOps for T where
	T: core::cmp::Ord,
	T: core::ops::Add<Output=Self>,
	T: core::ops::Sub<Output=Self>,
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

/// A prevote for a block and its ancestors.
#[cfg_attr(feature = "std", derive(Serialize))]
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Prevote<H, N> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number.
	pub target_number: N,
}

impl<H, N> Prevote<H, N> {
	pub fn new(target_hash: H, target_number: N) -> Self {
		Prevote { target_hash, target_number }
	}
}

/// A precommit for a block and its ancestors.
#[cfg_attr(feature = "std", derive(Serialize))]
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Precommit<H, N> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number
	pub target_number: N,
}

impl<H, N> Precommit<H, N> {
	pub fn new(target_hash: H, target_number: N) -> Self {
		Precommit { target_hash, target_number }
	}
}

/// A primary proposed block, this is a broadcast of the last round's estimate.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PrimaryPropose<H, N> {
	/// The target block's hash.
	pub target_hash: H,
	/// The target block's number
	pub target_number: N,
}

impl<H, N> PrimaryPropose<H, N> {
	pub fn new(target_hash: H, target_number: N) -> Self {
		PrimaryPropose { target_hash, target_number }
	}
}

/// An equivocation (double-vote) in a given round.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Equivocation<Id, V, S> {
	/// The round number equivocated in.
	pub round_number: u64,
	/// The identity of the equivocator.
	pub identity: Id,
	/// The first vote in the equivocation.
	pub	first: (V, S),
	/// The second vote in the equivocation.
	pub second: (V, S),
}


/// A protocol message or vote.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Message<H, N> {
	/// A prevote message.
	Prevote(Prevote<H, N>),
	/// A precommit message.
	Precommit(Precommit<H, N>),
	// Primary proposed block.
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

	/// Return true if the message is a prevote.
	pub fn is_prevote(&self) -> bool {
		match *self {
			Message::Prevote(_) => true,
			_ => false,
		}
	}
}
