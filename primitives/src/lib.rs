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

// #![cfg_attr(not(feature = "std"), no_std)]

use parity_codec::{Encode, Decode};

/// A prevote for a block and its ancestors.
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	NotDescendent,
}

/// Chain context necessary for implementation of the finality gadget.
pub trait Chain<H: Eq, N: Copy> {
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
