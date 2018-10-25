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

//! Bitfield for handling equivocations.
//!
//! This is primarily a bitfield for tracking equivocating validators.
//! It is necessary because there is a need to track vote-weight of equivocation
//! on the vote-graph but to avoid double-counting.
//!
//! Bitfields are either blank (in the general case) or live, in the case of
//! equivocations, with two bits per equivocator. The first is for equivocations
//! in prevote messages and the second for those in precommits.
//!
//! Each live bitfield carries a reference to a shared object that
//! provides lookups from bit indices to validator weight. Bitfields can be
//! merged, and queried for total weight in commits and precommits.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::RwLock;

// global used to ensure that conflicting shared objects are not used.
static SHARED_IDX: AtomicUsize = AtomicUsize::new(0);

/// Errors that can occur when using the equivocation weighting tools.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
	/// Too many equivocating validators registered. (shared data IDX, n_validators).
	TooManyEquivocators(usize, usize),
	/// Mismatch in shared data IDX when merging bitfields.
	ContextMismatch(usize, usize),
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Error::TooManyEquivocators(ref idx, ref n)
				=> write!(f, "Registered too many equivocators for shared data with ID {}. Maximum specified was {}", idx, n),
			Error::ContextMismatch(ref idx1, ref idx2)
				=> write!(f, "Attempted to merge bitfields with different contexts: {} vs {}", idx1, idx2),
		}
	}
}

impl ::std::error::Error for Error {}

/// Bitfield for equivocating validators.
///
/// See module docs for more details.
#[derive(Debug)]
pub enum Bitfield<Id> {
	/// Blank bitfield,
	Blank,
	/// Live bitfield,
	Live(LiveBitfield<Id>),
}

impl<Id> Default for Bitfield<Id> {
	fn default() -> Self {
		Bitfield::Blank
	}
}

impl<Id> Clone for Bitfield<Id> {
	fn clone(&self) -> Self {
		match *self {
			Bitfield::Blank => Bitfield::Blank,
			Bitfield::Live(ref l) => Bitfield::Live(l.clone()),
		}
	}
}

impl<Id: Eq> Bitfield<Id> {
	/// Find total equivocating weight (prevote, precommit).
	pub fn total_weight(&self) -> (u64, u64) {
		match *self {
			Bitfield::Blank => (0, 0),
			Bitfield::Live(ref live) => live.total_weight(),
		}
	}

	/// Combine two bitfields. Fails if they have conflicting shared data
	/// (i.e. they come from different contexts).
	pub fn merge(&self, other: &Self) -> Result<Self, Error> {
		match (self, other) {
			(&Bitfield::Blank, &Bitfield::Blank) => Ok(Bitfield::Blank),
			(&Bitfield::Live(ref live), &Bitfield::Blank) | (&Bitfield::Blank, &Bitfield::Live(ref live))
				=> Ok(Bitfield::Live(live.clone())),
			(&Bitfield::Live(ref a), &Bitfield::Live(ref b)) => {
				if a.shared.idx != b.shared.idx {
					// we can't merge two bitfields from different contexts.
					Err(Error::ContextMismatch(a.shared.idx, b.shared.idx))
				} else {
					let bits = a.bits.iter().zip(&b.bits).map(|(a, b)| a | b).collect();
					Ok(Bitfield::Live(LiveBitfield { bits, shared: a.shared.clone() }))
				}
			}
		}
	}

	/// Find overlap weight (prevote, precommit) between this bitfield and another.
	pub fn overlap(&self, other: &Self) -> Result<(u64, u64), Error> {
		match (self, other) {
			(&Bitfield::Live(ref a), &Bitfield::Live(ref b)) => {
				if a.shared.idx != b.shared.idx {
					// we can't merge two bitfields from different contexts.
					Err(Error::ContextMismatch(a.shared.idx, b.shared.idx))
				} else {
					Ok(total_weight(
						a.bits.iter().zip(&b.bits).map(|(a, b)| a & b),
						a.shared.validators.read().as_slice(),
					))
				}
			}
			_ => Ok((0, 0))
		}
	}
}

/// Live bitfield instance.
#[derive(Debug)]
pub struct LiveBitfield<Id> {
	bits: Vec<u64>,
	shared: Shared<Id>,
}

impl<Id> Clone for LiveBitfield<Id> {
	fn clone(&self) -> Self {
		LiveBitfield {
			bits: self.bits.clone(),
			shared: self.shared.clone(),
		}
	}
}

impl<Id: Eq> LiveBitfield<Id> {
	/// Create a new live bitfield.
	pub fn new(shared: Shared<Id>) -> Self {
		LiveBitfield { bits: shared.blank_bitfield(), shared }
	}

	/// Note a validator's equivocation in prevote.
	/// Fails if more equivocators than the number of validators have
	/// been registered.
	pub fn equivocated_prevote(&mut self, id: Id, weight: u64) -> Result<(), Error> {
		let val_off = self.shared.get_or_register_equivocator(id, weight)?;
		self.set_bit(val_off * 2);
		Ok(())
	}

	/// Note a validator's equivocation in precommit.
	/// Fails if more equivocators than the number of validators have
	/// been registered.
	pub fn equivocated_precommit(&mut self, id: Id, weight: u64) -> Result<(), Error> {
		let val_off = self.shared.get_or_register_equivocator(id, weight)?;
		self.set_bit(val_off * 2 + 1);
		Ok(())
	}

	fn set_bit(&mut self, bit_idx: usize) {
		let word_off = bit_idx / 64;
		let bit_off = bit_idx % 64;

		// If this isn't `Some`, something has gone really wrong.
		if let Some(word) = self.bits.get_mut(word_off) {
			// set bit starting from left.
			*word |= 1 << (63 - bit_off)
		} else {
			warn!(target: "afg", "Could not set bit {}. Bitfield was meant to have 2 bits for each of {} validators.",
				bit_idx, self.shared.n_validators);
		}
	}

	// find total weight of this bitfield (prevote, precommit).
	fn total_weight(&self) -> (u64, u64) {
		total_weight(self.bits.iter().cloned(), self.shared.validators.read().as_slice())
	}
}

// find total weight of the given iterable of bits. assumes that there are enough
// validators in the given context to correspond to all bits.
fn total_weight<Iter, Id>(iterable: Iter, validators: &[ValidatorEntry<Id>]) -> (u64, u64)
	where Iter: IntoIterator<Item=u64>
{
		struct State {
			word_idx: usize,
			prevote: u64,
			precommit: u64,
		};

		let state = State {
			word_idx: 0,
			prevote: 0,
			precommit: 0,
		};

		let state = iterable.into_iter().fold(state, |mut state, mut word| {
			for i in 0..32 {
				if word == 0 { break }

				// prevote bit is set
				if word & (1 << 63) == (1 << 63) {
					state.prevote += validators[state.word_idx * 32 + i].weight;
				}

				// precommit bit is set
				if word & (1 << 62) == (1 << 62) {
					state.precommit += validators[state.word_idx * 32 + i].weight;
				}

				word <<= 2;
			}

			state.word_idx += 1;
			state
		});

		(state.prevote, state.precommit)
	}

/// Shared data among all live bitfield instances.
#[derive(Debug)]
pub struct Shared<Id> {
	idx: usize,
	n_validators: usize,
	validators: Arc<RwLock<Vec<ValidatorEntry<Id>>>>
}

impl<Id> Clone for Shared<Id> {
	fn clone(&self) -> Self {
		Shared {
			idx: self.idx,
			n_validators: self.n_validators,
			validators: self.validators.clone(),
		}
	}
}

impl<Id: Eq> Shared<Id> {
	/// Create new shared equivocation detection data. Provide the number
	/// of validators.
	pub fn new(n_validators: usize) -> Self {
		let idx = SHARED_IDX.fetch_add(1, Ordering::SeqCst);
		Shared {
			idx,
			n_validators,
			validators: Arc::new(RwLock::new(Vec::new())),
		}
	}

	fn blank_bitfield(&self) -> Vec<u64> {
		let n_bits = self.n_validators * 2;
		let n_words = (n_bits + 63) / 64;

		vec![0; n_words]
	}

	fn get_or_register_equivocator(&self, equivocator: Id, weight: u64) -> Result<usize, Error> {
		{
			// linear search is probably fast enough until we have thousands of
			// equivocators. finding the bit to set is slow but happens rarely.
			let validators = self.validators.read();
			let maybe_found = validators.iter()
				.enumerate()
				.find(|&(_, ref e)| e.id == equivocator);

			if let Some((idx, _)) = maybe_found {
				return Ok(idx);
			}
		}

		let mut validators = self.validators.write();
		if validators.len() == self.n_validators {
			return Err(Error::TooManyEquivocators(self.idx, self.n_validators) )
		}
		validators.push(ValidatorEntry { id: equivocator, weight });
		Ok(validators.len() - 1)
	}
}

#[derive(Debug)]
struct ValidatorEntry<Id> {
	id: Id,
	weight: u64,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn shared_fails_registering_too_many() {
		let shared = Shared::new(0);
		assert!(shared.get_or_register_equivocator(5, 1000).is_err());
	}

	#[test]
	fn shared_register_same_many_times() {
		let shared = Shared::new(1);
		assert_eq!(shared.get_or_register_equivocator(5, 1000), Ok(0));
		assert_eq!(shared.get_or_register_equivocator(5, 1000), Ok(0));
	}

	#[test]
	fn merge_live() {
		let shared = Shared::new(10);
		let mut live_a = LiveBitfield::new(shared.clone());
		let mut live_b = LiveBitfield::new(shared.clone());

		live_a.equivocated_prevote(1, 5).unwrap();
		live_a.equivocated_precommit(2, 7).unwrap();

		live_b.equivocated_prevote(3, 9).unwrap();
		live_b.equivocated_precommit(3, 9).unwrap();

		assert_eq!(live_a.total_weight(), (5, 7));
		assert_eq!(live_b.total_weight(), (9, 9));

		let (a, b) = (Bitfield::Live(live_a), Bitfield::Live(live_b));

		let c = a.merge(&b).unwrap();
		assert_eq!(c.total_weight(), (14, 16));
	}

	#[test]
	fn merge_with_different_shared_is_error() {
		let shared_a: Shared<usize> = Shared::new(1);
		let shared_b = Shared::new(1);

		let bitfield_a = Bitfield::Live(LiveBitfield::new(shared_a));
		let bitfield_b = Bitfield::Live(LiveBitfield::new(shared_b));

		assert!(bitfield_a.merge(&bitfield_b).is_err());
	}

	#[test]
	fn set_first_and_last_bits() {
		let shared = Shared::new(32);
		assert_eq!(shared.blank_bitfield().len(), 1);

		for i in 0..32 {
			shared.get_or_register_equivocator(i, i + 1).unwrap();
		}

		let mut live_bitfield = LiveBitfield::new(shared);
		live_bitfield.equivocated_prevote(0, 1).unwrap();
		live_bitfield.equivocated_precommit(31, 32).unwrap();

		assert_eq!(live_bitfield.total_weight(), (1, 32));
	}

	#[test]
	fn weight_overlap() {
		let shared = Shared::new(10);
		let mut live_a = LiveBitfield::new(shared.clone());
		let mut live_b = LiveBitfield::new(shared.clone());

		live_a.equivocated_prevote(1, 5).unwrap();
		live_a.equivocated_precommit(2, 7).unwrap();
		live_a.equivocated_prevote(3, 9).unwrap();

		live_b.equivocated_prevote(1, 5).unwrap();
		live_b.equivocated_precommit(2, 7).unwrap();
		live_b.equivocated_precommit(3, 9).unwrap();

		assert_eq!(live_a.total_weight(), (14, 7));
		assert_eq!(live_b.total_weight(), (5, 16));

		let (a, b) = (Bitfield::Live(live_a), Bitfield::Live(live_b));

		assert_eq!(a.overlap(&b).unwrap(), (5, 7));
	}
}
