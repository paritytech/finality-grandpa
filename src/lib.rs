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

extern crate parking_lot;

#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;

#[cfg(test)]
extern crate tokio;
#[cfg(test)]
extern crate exit_future;

#[cfg(feature = "derive-codec")]
#[macro_use]
extern crate parity_codec_derive;

#[cfg(feature = "derive-codec")]
extern crate parity_codec;

pub mod bitfield;
pub mod round;
pub mod vote_graph;
pub mod voter;

mod bridge_state;

#[cfg(test)]
mod testing;

use std::fmt;
/// A prevote for a block and its ancestors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct Prevote<H> {
	target_hash: H,
	target_number: u32,
}

impl<H> Prevote<H> {
	pub fn new(target_hash: H, target_number: u32) -> Self {
		Prevote { target_hash, target_number }
	}
}

/// A precommit for a block and its ancestors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct Precommit<H> {
	target_hash: H,
	target_number: u32,
}

impl<H> Precommit<H> {
	pub fn new(target_hash: H, target_number: u32) -> Self {
		Precommit { target_hash, target_number }
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

impl ::std::error::Error for Error {
	fn description(&self) -> &str {
		match *self {
			Error::NotDescendent => "Block not descendent of base",
		}
	}
}

/// Chain context necessary for implementation of the finality gadget.
pub trait Chain<H> {
	/// Get the ancestry of a block up to but not including the base hash.
	/// Should be in reverse order from `block`'s parent.
	///
	/// If the block is not a descendent of `base`, returns an error.
	fn ancestry(&self, base: H, block: H) -> Result<Vec<H>, Error>;

	/// Return the hash of the best block whose chain contains the given block hash,
	/// even if that block is `base` itself.
	///
	/// If `base` is unknown, return `None`.
	fn best_chain_containing(&self, base: H) -> Option<(H, u32)>;
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
pub enum Message<H> {
	/// A prevote message.
	Prevote(Prevote<H>),
	/// A precommit message.
	Precommit(Precommit<H>),
	// TODO: liveness-propose and commit messages.
}

/// A signed message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct SignedMessage<H, S, Id> {
	pub message: Message<H>,
	pub signature: S,
	pub id: Id,
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "derive-codec")]
	#[test]
	fn codec_was_derived() {
		use parity_codec::{Encode, Decode};

		let signed = ::SignedMessage {
			message: ::Message::Prevote(::Prevote {
				target_hash: b"Hello".to_vec(),
				target_number: 5,
			}),
			signature: b"Signature".to_vec(),
			id: 5000,
		};

		let encoded = signed.encode();
		let signed2 = ::SignedMessage::decode(&mut &encoded[..]).unwrap();
		assert_eq!(signed, signed2);
	}
}
