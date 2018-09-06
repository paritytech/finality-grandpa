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
	leaves: Vec<&'static str>,
}

impl DummyChain {
	pub fn new() -> Self {
		let mut inner = HashMap::new();
		inner.insert(GENESIS_HASH, BlockRecord { number: 1, parent: NULL_HASH });

		DummyChain { inner, leaves: vec![GENESIS_HASH] }
	}

	pub fn push_blocks(&mut self, mut parent: &'static str, blocks: &[&'static str]) {
		use std::cmp::Ord;

		if blocks.is_empty() { return }

		let base_number = self.inner.get(parent).unwrap().number + 1;

		if let Some(pos) = self.leaves.iter().position(|x| x == &parent) {
			self.leaves.remove(pos);
		}

		for (i, descendent) in blocks.iter().enumerate() {
			self.inner.insert(descendent, BlockRecord {
				number: base_number + i,
				parent,
			});

			parent = descendent;
		}

		let new_leaf = blocks.last().unwrap();
		let new_leaf_number = self.inner.get(new_leaf).unwrap().number;

		let insertion_index = self.leaves.binary_search_by(
			|x| self.inner.get(x).unwrap().number.cmp(&new_leaf_number).reverse(),
		).unwrap_or_else(|i| i);

		self.leaves.insert(insertion_index, new_leaf);
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
				None => return Err(Error::NotDescendent),
				Some(record) => { block = record.parent; }
			}

			ancestry.push(block);

			if block == NULL_HASH { return Err(Error::NotDescendent) }
			if block == base { break }
		}

		Ok(ancestry)
	}

	fn best_chain_containing(&self, base: &'static str) -> Option<(&'static str, usize)> {
		let base_number = self.inner.get(base)?.number;

		for leaf in &self.leaves {
			// leaves are in descending order.
			let leaf_number = self.inner.get(leaf).unwrap().number;
			if leaf_number < base_number { break }

			if let Ok(_) = self.ancestry(base, leaf) {
				return Some((leaf, leaf_number));
			}
		}

		None
	}
}
