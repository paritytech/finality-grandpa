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

use collections::{Vec};
use std::fmt;
use crate::voter_set::VoterSet;

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

/// A prevote for a block and its ancestors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
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

/// An equivocation (double-vote) in a given round.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub enum Message<H, N> {
	/// A prevote message.
	#[cfg_attr(feature = "derive-codec", codec(index = "0"))]
	Prevote(Prevote<H, N>),
	/// A precommit message.
	#[cfg_attr(feature = "derive-codec", codec(index = "1"))]
	Precommit(Precommit<H, N>),
	// Primary proposed block.
	#[cfg_attr(feature = "derive-codec", codec(index = "2"))]
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
) -> Result<Option<(H, N)>, crate::Error>
	where
	H: std::hash::Hash + Clone + Eq + Ord + std::fmt::Debug,
	N: Copy + BlockNumberOps + std::fmt::Debug,
	I: Clone + std::hash::Hash + Eq + std::fmt::Debug,
	S: Eq,
{
	// check that all precommits are for blocks higher than the target
	// commit block, and that they're its descendents
	if !commit.precommits.iter().all(|signed| {
		signed.precommit.target_number >= commit.target_number &&
			chain.is_equal_or_descendent_of(
				commit.target_hash.clone(),
				signed.precommit.target_hash.clone(),
			)
	}) {
		return Ok(None);
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
		if let Some(_) = round.import_precommit(chain, precommit.clone(), id.clone(), signature.clone())? {
			// allow only one equivocation per voter, as extras are redundant.
			if !equivocated.insert(id) { return Ok(None) }
		}
	}

	// if a ghost is found then it must be equal or higher than the commit
	// target, otherwise the commit is invalid
	Ok(round.precommit_ghost())
}

/// Get the threshold weight given the total voting weight.
pub fn threshold(total_weight: u64) -> u64 {
	let faulty = total_weight.saturating_sub(1) / 3;
	total_weight - faulty
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
