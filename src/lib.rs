// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-afg.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-afg. If not, see <http://www.gnu.org/licenses/>.

//! Finality gadget for blockchains.
//!
//! https://hackmd.io/svMTltnGQsSR1GCjRKOPbw

mod vote_graph;

/// A prevote for a block and its ancestors.
pub struct Prevote<H> {
	round: u64,
	target: H,
	weight: usize,
}

/// A precommit for a block and its ancestors.
pub struct Precommit<H> {
	round: u64,
	target: H,
	weight: usize,
}

/// Chain context necessary for implementation of the finality gadget.
pub trait Chain<H> {
	/// Get the ancestry of a block up to but not including the base hash.
	/// Should be in reverse order from `block`'s parent.
	fn ancestry(&self, base: H, block: H) -> Option<Vec<H>>;
}
