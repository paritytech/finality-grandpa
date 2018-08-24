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

//! Helpers for testing

use std::collections::HashMap;
use super::{Chain, Error};

pub const GENESIS_HASH: &str = "genesis";
const NULL_HASH: &str = "NULL";

struct BlockRecord {
	number: usize,
	parent: &'static str,
}

pub struct DummyChain {
	inner: HashMap<&'static str, BlockRecord>,
}

impl DummyChain {
	pub fn new() -> Self {
		let mut inner = HashMap::new();
		inner.insert(GENESIS_HASH, BlockRecord { number: 1, parent: NULL_HASH });

		DummyChain { inner }
	}

	pub fn push_blocks(&mut self, mut parent: &'static str, blocks: &[&'static str]) {
		let base_number = self.inner.get(parent).unwrap().number + 1;
		for (i, descendent) in blocks.iter().enumerate() {
			self.inner.insert(descendent, BlockRecord {
				number: base_number + i,
				parent,
			});

			parent = descendent;
		}
	}

	pub fn number(&self, hash: &'static str) -> usize {
		self.inner.get(hash).unwrap().number
	}
}

impl Chain<&'static str> for DummyChain {
	fn ancestry(&self, base: &'static str, mut block: &'static str) -> Result<Vec<&'static str>, Error> {
		let mut ancestry = Vec::new();

		loop {
			match self.inner.get(block) {
				None => return Err(Error::BlockNotInSubtree),
				Some(record) => { block = record.parent; }
			}

			ancestry.push(block);

			if block == NULL_HASH { return Err(Error::BlockNotInSubtree) }
			if block == base { break }
		}

		Ok(ancestry)
	}
}
