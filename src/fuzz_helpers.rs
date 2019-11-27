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

use crate::round::{RoundParams, Round};

use crate::{Chain, Error};

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

	fn children(hash: Hash) -> &'static [Hash] {
		match hash {
			0 => &[1, 2, 3],
			1 => &[4, 5, 6],
			2 => &[7, 8, 9],
			4 => &[10, 11, 12],
			7 => &[13, 14, 15],
			_ => &[],
		}
	}

	fn all_descendents(hash: Hash) -> impl Iterator<Item=Hash> {
		let children = Self::children(hash);

		struct Descendents(Vec<Hash>);
		impl Iterator for Descendents {
			type Item = Hash;

			fn next(&mut self) -> Option<Hash> {
				let next = self.0.pop()?;
				self.0.extend(FuzzChain::children(next).iter().cloned());
				Some(next)
			}
		}

		Descendents(children.to_vec())
	}
}

impl Chain<Hash, BlockNumber> for FuzzChain {
	fn ancestry(&self, base: Hash, block: Hash) -> Result<Vec<Hash>, Error> {
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

		let full_ancestry: &[Hash] = match block {
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
	fn read_nibble(&mut self) -> Option<u8> {
		let active = *self.inner.get(self.pos)?;
		if self.half_nibble {
			self.half_nibble = false;
			self.pos += 1;

			Some(active & 0x0F)
		} else {
			self.half_nibble = true;

			Some((active >> 4) & 0x0F)
		}
	}

	fn read_byte(&mut self) -> Option<u8> {
		if self.half_nibble {
			// just skip 4 bytes.
			self.half_nibble = false;
		}
		self.pos += 1;
		self.inner.get(self.pos).map(|&b| b)
	}
}

fn voters() -> [Voter; 10] {
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
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

// returns the kth combination of r numbers from the first n.
//
// only works for values of n and r up to 10.
// this is used to select 6 other voters from the 10 (bumping indices after our own)
// to import prevotes from.
fn kth_combination(k: u8, n: u8, r: u8) -> Vec<u8> {
	let mut v = Vec::with_capacity(r as usize);
	fn r_helper(k: u8, n: u8, r: u8, off: u8, v: &mut Vec<u8>) {
		if r == 0 { return }

		// the "tail" of the list we have here is all the elements from the offset
		// to the total number of elements.
		if n == 0 {
			v.extend((0..r).map(|x| x + off));
			return
		}

		// how many choices there are of the remaining.
		let i = n_choose_r(n - 1, r - 1);
		if k < i {
			// first item of the list and then the k'th choice of the remainder
			v.push(off);
			r_helper(k, n - 1, r - 1, off + 1, v);
		} else {
			// choose k - i of items not including the first.
			r_helper(k - i, n - 1, r, off + 1, v);
		}
	}

	r_helper(k, n, r, 0, &mut v);

	v
}

/// Execute a fuzzed voting process.
pub fn execute_fuzzed_vote(data: &[u8]) {
	let mut stream = RandomnessStream {
		inner: data,
		pos: 0,
		half_nibble: false,
	};

	// every voter gets a round.
	let mut rounds: Vec<Round<Voter, Hash, BlockNumber, Signature>>
		= voters().iter().map(|_| Round::new(RoundParams {
			round_number: 0,
			voters: voters().iter().cloned().map(|v| (v, 1)).collect(),
			base: (0, 0),
		})).collect();

	let prevotes = voters().iter().filter_map(|_| {
		let prevote_block = stream.read_nibble()?;

		Some(crate::Prevote {
			target_hash: prevote_block,
			target_number: FuzzChain::number(prevote_block),
		})
	}).collect::<Vec<_>>();

	if prevotes.len() != voters().len() {
		// fuzzer needs to get us more data.
		return
	}

	let mut precommits = Vec::with_capacity(voters().len());

	let n_combinations = n_choose_r(9, 6);
	for (i, &voter) in voters().iter().enumerate() {
		// select 6 other voters to import prevotes from.
		// cast precommit.
		let k = match stream.read_byte() {
			Some(b) => b % n_combinations,
			None => return,
		};
		let combination = kth_combination(k, 9, 6);

		let round = &mut rounds[i];

		// import our own prevote.
		round.import_prevote(
			&FuzzChain,
			prevotes[i].clone(),
			voter,
			voter,
		).unwrap();

		// import others' prevotes.
		for other in combination {
			// we selected out of 9, assuming that our index was omitted.
			let other = if other >= voter {
				other + 1
			} else {
				other
			};

			round.import_prevote(
				&FuzzChain,
				prevotes[other as usize].clone(),
				other,
				other,
			).unwrap();
		}

		let (target_hash, target_number) = round.state().prevote_ghost
			.expect("after importing threshold votes, ghost exists");

		let precommit = crate::Precommit { target_hash, target_number };
		precommits.push(precommit.clone());

		round.import_precommit(
			&FuzzChain,
			precommit,
			voter,
			voter,
		).unwrap();
	}

	for (i, &voter) in voters().iter().enumerate() {
		let k = match stream.read_byte() {
			Some(b) => b % n_combinations,
			None => return,
		};

		let mut omit = kth_combination(k, 9, 3);
		{
			for x in &mut omit {
				if *x >= voter { *x += 1 }
			}
		}

		// import random precommits, omitting those listed.
		let round = &mut rounds[i as usize];

		for &j in voters().iter().filter(|j| !omit.contains(j)) {
			round.import_precommit(
				&FuzzChain,
				precommits[j as usize].clone(),
				j,
				j,
			).unwrap();
		}

		let mut completable = round.state().completable;
		let mut last_estimate = round.state().estimate.clone();

		// import the remaining precommits.
		for j in omit {
			round.import_precommit(
				&FuzzChain,
				precommits[j as usize].clone(),
				j,
				j,
			).unwrap();

			let new_state = round.state();
			if completable {
				assert!(new_state.completable);

				let new_estimate = new_state.estimate.expect("is completable");
				let old_estimate = last_estimate.expect("is completable");

				// check that estimate only moves backwards.
				assert!(
					new_estimate == old_estimate
					|| FuzzChain.ancestry(new_estimate.0, old_estimate.0).is_ok()
				);
				last_estimate = Some(new_estimate);
			} else {
				completable = new_state.completable;
				last_estimate = new_state.estimate;
			}
		}
	}
}

pub fn execute_fuzzed_graph(data: &[u8]) {
	// 100 voters, all voting on random blocks.
	const N: u8 = 100;
	const T: u8 = 67;

	#[derive(Default, Clone, Debug)]
	struct Vote(u8, u8);
	impl std::ops::AddAssign for Vote {
		fn add_assign(&mut self, other: Vote) {
			self.0 += other.0;
			self.1 += other.1;
		}
	}

	let mut stream = RandomnessStream {
		inner: data,
		pos: 0,
		half_nibble: false,
	};

	let mut graph = crate::vote_graph::VoteGraph::new(0, 0);

	let mut prevote_ghost = None;
	for _ in 0..N {
		let target_hash = match stream.read_nibble() {
			None => return,
			Some(h) => h,
		};
		let target_number = FuzzChain::number(target_hash);

		graph.insert(target_hash, target_number, Vote(1, 0), &FuzzChain).unwrap();

		let new_prevote_ghost = graph.find_ghost(prevote_ghost.clone(), |v| v.0 >= T);
		if let Some((o_h, o_n)) = prevote_ghost {
			let (n_h, n_n) = new_prevote_ghost.expect("ghost does not disappear with more votes.");

			// assert it always moves forwards.
			assert!(
				o_h == n_h
				|| (n_n > o_n && FuzzChain.ancestry(o_h, n_h).is_ok())
			);
		}

		if let Some((p_h, _p_n)) = new_prevote_ghost {
			// ensure that no children have enough votes.
			for descendent in FuzzChain::all_descendents(p_h) {
				let n = FuzzChain::number(descendent);
				assert!(graph.cumulative_vote(descendent, n).0 < T);
			}
		}

		prevote_ghost = new_prevote_ghost;
	}

	let prevote_ghost = prevote_ghost.expect("prevote ghost always some by this point.");

	let mut estimate: Option<(Hash, BlockNumber)> = None;
	let mut completable = false;
	for i in 0..N {
		let target_hash = match stream.read_nibble() {
			None => return,
			Some(h) => h,
		};

		let target_hash = match FuzzChain.ancestry(prevote_ghost.0, target_hash) {
			Ok(_) => target_hash,
			Err(_) => prevote_ghost.0,
		};

		let target_number = FuzzChain::number(target_hash);
		graph.insert(target_hash, target_number, Vote(0, 1), &FuzzChain).unwrap();

		let new_prevote_ghost = graph.find_ghost(Some(prevote_ghost.clone()), |v| v.0 >= T).unwrap();
		assert_eq!(new_prevote_ghost, prevote_ghost, "should not change");

		if i < T { return }

		let remaining = N - i - 1;
		let possible_equivocations = 33;
		let possible_to_precommit = |v: &Vote| v.1 + remaining + possible_equivocations >= T;

		let new_estimate = graph.find_ancestor(
			prevote_ghost.0,
			prevote_ghost.1,
			possible_to_precommit,
		);

		let newly_completable = if let Some((e_h, e_n)) = new_estimate {
			let ancestor_of = e_h != prevote_ghost.0 && {
				// is ancestor of prevote ghost.
				assert!(FuzzChain.ancestry(e_h, prevote_ghost.0).is_ok());

				true
			};

			ancestor_of || {
				graph.find_ghost(
					Some((e_h, e_n)),
					possible_to_precommit,
				).map_or(true, |x| x == prevote_ghost)
			}
		} else {
			false
		};

		if completable {
			assert!(newly_completable);
			let new_estimate = new_estimate.expect("was completable before; estimate exists");
			let estimate = estimate.expect("was completable before");

			// estimate may only move monotonically backwards from this point.
			assert!(
				estimate == new_estimate
				|| FuzzChain.ancestry(new_estimate.0, estimate.0).is_ok()
			)
		}

		estimate = new_estimate.clone();
		completable = newly_completable;
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn be9e58ec5a0d4dce97bd1f07a3d1ffddd7d4b48b() {
		let data = include_bytes!("../fuzz_corpus/be9e58ec5a0d4dce97bd1f07a3d1ffddd7d4b48b");
		super::execute_fuzzed_vote(&data[..]);
	}

	#[test]
	fn a8898e66e34fee70c41c7aac26369c02e249dfe9() {
		let data = include_bytes!("../fuzz_corpus/a8898e66e34fee70c41c7aac26369c02e249dfe9");
		super::execute_fuzzed_vote(&data[..]);
	}
}
