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

use finality_grandpa::round::{RoundParams, Round};

use finality_grandpa::{Chain, Error};

pub const GENESIS_HASH: u8 = 0;

type Voter = u8;
type Hash = u8;
type BlockNumber = u8;
type Signature = u8;

/// The fuzzing chain is made of 16 blocks, including the genesis.
/// The genesis is 0. each block can be distinguished by a 4-bit number.
///
/// 0 -> [1, 2, 3]
/// 1 -> [4, 5, 6]
/// 2 -> [7, 8, 9]
/// 4 -> [10, 11, 12]
/// 7 -> [13, 14, 15]

#[derive(Default, Clone, Copy)]
pub struct FuzzChain;

impl FuzzChain {
	fn number(hash: Hash) -> BlockNumber {
		match hash {
			0 => 0,

			1 | 2 | 3 => 1,

			4 | 5 | 6 => 2,
			7 | 8 | 9 => 2,

			10 | 11 | 12 => 3,
			13 | 14 | 15 => 3,

			_ => panic!("invalid block hash"),
		}
	}
}

impl Chain<Hash, BlockNumber> for FuzzChain {
	fn ancestry(&self, base: Hash, mut block: Hash) -> Result<Vec<Hash>, Error> {
		// filter out bad descendents.
		match (base, block) {
			(0, x) if x <= 15 => {},

			(1, 4) => {},
			(1, 5) => {},
			(1, 6) => {},
			(1, 10) => {},
			(1, 11) => {},
			(1, 12) => {},

			(2, 7) => {},
			(2, 8) => {},
			(2, 9) => {},
			(2, 13) => {},
			(2, 14) => {},
			(2, 15) => {},

			(4, 10) => {},
			(4, 11) => {},
			(4, 12) => {},

			(7, 13) => {},
			(7, 14) => {},
			(7, 15) => {},

			_ => return Err(Error::NotDescendent),
		}

		let full_ancestry: &[Hash] = match base {
			0 => &[],
			1 | 2 | 3 => &[0],
			4 | 5 | 6 => &[0, 1],
			7 | 8 | 9 => &[0, 2],
			10 | 11 | 12 => &[0, 1, 4],
			13 | 14 | 15 => &[0, 2, 7],
			_ => panic!("invalid block hash"),
		};

		Ok(full_ancestry.iter().rev().take_while(|x| **x != base).cloned().collect::<Vec<_>>())
	}

	fn best_chain_containing(&self, _base: Hash) -> Option<(Hash, BlockNumber)> {
		// should be unused.
		unimplemented!()
	}
}

struct RandomnessStream<'a> {
	inner: &'a [u8],
	pos: usize,
	half_nibble: bool,
}

impl<'a> RandomnessStream<'a> {
	fn read_nibble(&mut self) -> u8 {
		let active = self.inner[self.pos];
		if self.half_nibble {
			self.half_nibble = false;
			self.pos += 1;

			active & 0x0F
		} else {
			self.half_nibble = true;

			(active >> 4) & 0x0F
		}
	}

	fn read_byte(&mut self) -> u8 {
		if self.half_nibble {
			// just skip 4 bytes.
			self.half_nibble = false;
		}
		self.pos += 1;
		self.inner[self.pos]
	}
}

fn voters() -> [Voter; 10] {
	[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
}

const FACTORIAL: [u32; 11] = [
	1, // 0
	1, // 1
	2, // 2
	6, // 3
	24, // 4
	120, // 5
	720, // 6
	5040, // 7
	40320, // 8
	362880, // 9
	3628800, // 10
];

// how many combinations there are of r combinations. of n elements.
fn n_choose_r(n: u8, r: u8) -> u8 {
	assert!(r <= 7);
	assert!(n <= 10);
	assert!(n >= r);

	(FACTORIAL[n as usize] / (FACTORIAL[r as usize] * FACTORIAL[(n - r) as usize])) as u8
}

// returns the kth combination of 6 voters from the first 9.
// this is used to select 6 other voters from the 10 (bumping indices after our own)
// to import prevotes from.
fn kth_combination(k: u8, n: u8, r: u8) {
	unimplemented!()
}

#[test]
fn fuzz_vote() {

}

fn execute_fuzzed_vote(data: &[u8]) {
	let mut round: Round<Voter, Hash, BlockNumber, Signature> = Round::new(RoundParams {
		round_number: 0,
		voters: voters().iter().cloned().map(|v| (v, 1)).collect(),
		base: (0, 1),
	});

	let mut stream = RandomnessStream {
		inner: data,
		pos: 0,
		half_nibble: false,
	};

	let prevotes = voters().iter().cloned().map(|v| {
		let prevote_block = stream.read_nibble();

		// cast prevote and import into local round.
		let prevote = finality_grandpa::Prevote {
			target_hash: prevote_block,
			target_number: FuzzChain::number(prevote_block),
		};

		round.import_prevote(
			&FuzzChain,
			prevote.clone(),
			v,
			v,
		).unwrap();
	}).collect::<Vec<_>>();

	for voter in voters().iter() {

	}

	for voter in voters().iter() {
		// select 6 other voters to import prevotes from.
		// cast precommit.
	}

	for voter in voters().iter() {
		// start importing precommits from others in random order. ensure that
		// the estimate only ever moves backwards once completable.
	}
}
