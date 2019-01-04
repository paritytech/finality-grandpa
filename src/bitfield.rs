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

//! Bitfields and tools for handling equivocations.
//!
//! This is primarily a bitfield for tracking equivocating voters.
//! It is necessary because there is a need to track vote-weight of equivocation
//! on the vote-graph but to avoid double-counting.
//!
//! We count equivocating voters as voting for everything. This makes any
//! further equivocations redundant with the first.
//!
//! Bitfields are either blank or live, with two bits per equivocator.
//! The first is for equivocations in prevote messages and the second
//! for those in precommits.
//!
//! Bitfields on regular vote-nodes will tend to be live, but the equivocating
//! bitfield will be mostly empty.

use std::fmt;
use std::sync::Arc;
use parking_lot::RwLock;

use crate::VoterInfo;

/// Errors that can occur when using the equivocation weighting tools.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
	/// Attempted to index bitfield past its length.
	IndexOutOfBounds(usize, usize),
	/// Mismatch in bitfield length when merging bitfields.
	LengthMismatch(usize, usize),
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Error::IndexOutOfBounds(ref idx, ref n)
				=> write!(f, "Attempted to set voter {}. Maximum specified was {}", idx, n),
			Error::LengthMismatch(ref idx1, ref idx2)
				=> write!(f, "Attempted to merge bitfields with different lengths: {} vs {}", idx1, idx2),
		}
	}
}

impl ::std::error::Error for Error {}

/// Bitfield for tracking voters who have equivocated.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Bitfield {
	/// Blank bitfield,
	Blank,
	/// Live bitfield,
	Live(LiveBitfield),
}

impl Default for Bitfield {
	fn default() -> Self {
		Bitfield::Blank
	}
}

impl Bitfield {
	/// Combine two bitfields. Fails if they have conflicting shared data
	/// (i.e. they come from different contexts).
	pub fn merge(&self, other: &Self) -> Result<Self, Error> {
		match (self, other) {
			(&Bitfield::Blank, &Bitfield::Blank) => Ok(Bitfield::Blank),
			(&Bitfield::Live(ref live), &Bitfield::Blank) | (&Bitfield::Blank, &Bitfield::Live(ref live))
				=> Ok(Bitfield::Live(live.clone())),
			(&Bitfield::Live(ref a), &Bitfield::Live(ref b)) => {
				if a.bits.len() != b.bits.len() {
					// we can't merge two bitfields with different lengths.
					Err(Error::LengthMismatch(a.bits.len(), b.bits.len()))
				} else {
					let bits = a.bits.iter().zip(&b.bits).map(|(a, b)| a | b).collect();
					Ok(Bitfield::Live(LiveBitfield { bits }))
				}
			}
		}
	}

	/// Find overlap weight (prevote, precommit) between this bitfield and another.
	pub fn overlap(&self, other: &Self) -> Result<Self, Error> {
		match (self, other) {
			(&Bitfield::Live(ref a), &Bitfield::Live(ref b)) => {
				if a.bits.len() != b.bits.len() {
					// we can't find overlap of two bitfields with different lengths.
					Err(Error::LengthMismatch(a.bits.len(), b.bits.len()))
				} else {
					Ok(Bitfield::Live(LiveBitfield {
						bits: a.bits.iter().zip(&b.bits).map(|(a, b)| a & b).collect(),
					}))
				}
			}
			_ => Ok(Bitfield::Blank)
		}
	}

	/// Find total equivocating weight (prevote, precommit).
	/// Provide a function for looking up voter weight.
	pub fn total_weight<F: Fn(usize) -> u64>(&self, lookup: F) -> (u64, u64) {
		match *self {
			Bitfield::Blank => (0, 0),
			Bitfield::Live(ref live) => total_weight(live.bits.iter().cloned(), lookup),
		}
	}

	/// Set a bit in the bitfield.
	fn set_bit(&mut self, bit: usize, n_voters: usize) -> Result<(), Error> {
		let mut live = match ::std::mem::replace(self, Bitfield::Blank) {
			Bitfield::Blank => LiveBitfield::with_voters(n_voters),
			Bitfield::Live(live) => live,
		};

		live.set_bit(bit, n_voters)?;
		*self = Bitfield::Live(live);
		Ok(())
	}
}

/// Live bitfield instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveBitfield {
	bits: Vec<u64>,
}

impl LiveBitfield {
	fn with_voters(n_voters: usize) -> Self {
		let n_bits = n_voters * 2;
		let n_words = (n_bits + 63) / 64;

		LiveBitfield { bits: vec![0; n_words] }
	}

	fn set_bit(&mut self, bit_idx: usize, n_voters: usize) -> Result<(), Error> {
		let word_off = bit_idx / 64;
		let bit_off = bit_idx % 64;

		// If this isn't `Some`, something has gone really wrong.
		if let Some(word) = self.bits.get_mut(word_off) {
			// set bit starting from left.
			*word |= 1 << (63 - bit_off);
			Ok(())
		} else {
			Err(Error::IndexOutOfBounds(bit_idx / 2, n_voters))
		}
	}
}

// find total weight of the given iterable of bits. assumes that there are enough
// voters in the given context to correspond to all bits.
fn total_weight<Iter, Lookup>(iterable: Iter, lookup: Lookup) -> (u64, u64) where
	Iter: IntoIterator<Item=u64>,
	Lookup: Fn(usize) -> u64,
{
	struct State {
		val_idx: usize,
		prevote: u64,
		precommit: u64,
	};

	let state = State {
		val_idx: 0,
		prevote: 0,
		precommit: 0,
	};

	let state = iterable.into_iter().fold(state, |mut state, mut word| {
		for i in 0..32 {
			if word == 0 { break }

			// prevote bit is set
			if word & (1 << 63) == (1 << 63) {
				state.prevote += lookup(state.val_idx + i);
			}

			// precommit bit is set
			if word & (1 << 62) == (1 << 62) {
				state.precommit += lookup(state.val_idx + i);
			}

			word <<= 2;
		}

		state.val_idx += 32;
		state
	});

	(state.prevote, state.precommit)
}

/// Shared data among all live bitfield instances.
#[derive(Debug)]
pub struct Shared {
	n_voters: usize,
	equivocators: Arc<RwLock<Bitfield>>,
}

impl Clone for Shared {
	fn clone(&self) -> Self {
		Shared {
			n_voters: self.n_voters,
			equivocators: self.equivocators.clone(),
		}
	}
}

impl Shared {
	/// Create new shared equivocation detection data. Provide the number of voters.
	pub fn new(n_voters: usize) -> Self {
		Shared {
			n_voters,
			equivocators: Arc::new(RwLock::new(Bitfield::Blank)),
		}
	}

	/// Construct a new bitfield for a specific voter prevoting.
	pub fn prevote_bitfield(&self, info: &VoterInfo) -> Result<Bitfield, Error> {
		let mut bitfield = LiveBitfield::with_voters(self.n_voters);
		bitfield.set_bit(info.canon_idx() * 2, self.n_voters)?;

		Ok(Bitfield::Live(bitfield))
	}

	/// Construct a new bitfield for a specific voter prevoting.
	pub fn precommit_bitfield(&self, info: &VoterInfo) -> Result<Bitfield, Error> {
		let mut bitfield = LiveBitfield::with_voters(self.n_voters);
		bitfield.set_bit(info.canon_idx() * 2 + 1, self.n_voters)?;

		Ok(Bitfield::Live(bitfield))
	}

	/// Get the equivocators bitfield.
	pub fn equivocators(&self) -> &Arc<RwLock<Bitfield>> {
		&self.equivocators
	}

	/// Note a voter's equivocation in prevote.
	pub fn equivocated_prevote(&self, info: &VoterInfo) -> Result<(), Error> {
		self.equivocators.write().set_bit(info.canon_idx() * 2, self.n_voters)?;
		Ok(())
	}

	/// Note a voter's equivocation in precommit.
	pub fn equivocated_precommit(&self, info: &VoterInfo) -> Result<(), Error> {
		self.equivocators.write().set_bit(info.canon_idx() * 2 + 1, self.n_voters)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::VoterSet;

	#[test]
	fn merge_live() {
		let mut a = Bitfield::Live(LiveBitfield::with_voters(10));
		let mut b = Bitfield::Live(LiveBitfield::with_voters(10));

		let v: VoterSet<usize> = [
			(1, 5),
			(4, 1),
			(3, 9),
			(5, 7),
			(9, 9),
			(2, 7),
		].iter().cloned().collect();

		a.set_bit(0, 10).unwrap(); // prevote 1
		a.set_bit(11, 10).unwrap(); // precommit 2

		b.set_bit(4, 10).unwrap(); // prevote 3
		b.set_bit(5, 10).unwrap(); // precommit 3

		let c = a.merge(&b).unwrap();
		assert_eq!(c.total_weight(|i| v.weight_by_index(i).unwrap()), (14, 16));
	}

	#[test]
	fn set_first_and_last_bits() {
		let v: VoterSet<usize> = (0..32).map(|i| (i, (i + 1) as u64)).collect();

		let mut live_bitfield = Bitfield::Live(LiveBitfield::with_voters(32));

		live_bitfield.set_bit(0, 32).unwrap();
		live_bitfield.set_bit(63, 32).unwrap();

		assert_eq!(live_bitfield.total_weight(|i| v.weight_by_index(i).unwrap()), (1, 32));
	}

	#[test]
	fn weight_overlap() {
		let mut a = Bitfield::Live(LiveBitfield::with_voters(10));
		let mut b = Bitfield::Live(LiveBitfield::with_voters(10));

		let v: VoterSet<usize> = [
			(1, 5),
			(4, 1),
			(3, 9),
			(5, 7),
			(9, 9),
			(2, 7),
		].iter().cloned().collect();

		a.set_bit(0, 10).unwrap(); // prevote 1
		a.set_bit(11, 10).unwrap(); // precommit 2
		a.set_bit(4, 10).unwrap(); // prevote 3

		b.set_bit(0, 10).unwrap(); // prevote 1
		b.set_bit(11, 10).unwrap(); // precommit 2
		b.set_bit(5, 10).unwrap(); // precommit 3

		assert_eq!(a.total_weight(|i| v.weight_by_index(i).unwrap()), (14, 7));
		assert_eq!(b.total_weight(|i| v.weight_by_index(i).unwrap()), (5, 16));

		let mut c = Bitfield::Live(LiveBitfield::with_voters(10));

		c.set_bit(0, 10).unwrap(); // prevote 1
		c.set_bit(11, 10).unwrap(); // precommit 2

		assert_eq!(a.overlap(&b).unwrap(), c);
	}
}
