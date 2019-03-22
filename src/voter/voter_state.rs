// Copyright 2019 Parity Technologies (UK) Ltd.
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

use std::clone::Clone;
use std::hash::Hash;
use std::sync::Arc;
use parking_lot::Mutex;

use crate::{
	Commit, Message, Prevote, Precommit, PrimaryPropose, SignedMessage,
	SignedPrecommit, BlockNumberOps, validate_commit
};
use super::{Environment, Buffered, RoundData};

pub struct VoterState<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	inner: Arc<Mutex<InnerVoterState<H, N, E>>>,
}

impl<H, N, E: Environment<H, N>> VoterState<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	pub fn new() -> Self {
		VoterState { 
			inner: Arc::new(Mutex::new(
				InnerVoterState {
					primary_voter: None,
				}
			) 
		}
	}

	pub fn set_primary_voter(&mut self, primary_voter: E::Id) {
		self.inner.lock().primary_voter = Some(primary_voter);
	}

	pub fn get_primary_voter(&self) -> Option<E::Id> {
		self.inner.lock().primary_voter.clone()
	}
}

impl<H, N, E: Environment<H, N>> Clone for VoterState<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	fn clone(&self) -> Self {
		VoterState {
			inner: self.inner.clone(),
		}
	}
}

#[derive(Clone)]
struct InnerVoterState<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	primary_voter: Option<E::Id>,
}