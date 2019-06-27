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

mod voter_set;
mod bitfield;

use parity_codec::{Encode, Decode};
use core::fmt;

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
