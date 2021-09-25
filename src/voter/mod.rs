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

//! A voter in GRANDPA. This transitions between rounds and casts votes.
//!
//! Voters rely on some external context to function:
//!   - setting timers to cast votes.
//!   - incoming vote streams.
//!   - providing voter weights.
//!   - getting the local voter id.
//!
//!  The local voter id is used to check whether to cast votes for a given
//!  round. If no local id is defined or if it's not part of the voter set then
//!  votes will not be pushed to the sink. The protocol state machine still
//!  transitions state as if the votes had been pushed out.

use std::{fmt::Debug, pin::Pin};

use async_trait::async_trait;
use futures::{future::Fuse, pin_mut, prelude::*, select};
use log::debug;

use crate::{
	round::State as RoundState,
	voter::{background_round::BackgroundRound, voting_round::VotingRound},
	BlockNumberOps, Chain, Commit, Error, Message, SignedMessage, VoterSet,
};

mod background_round;
#[cfg(test)]
mod tests;
mod voting_round;

/// Data necessary to participate in a round.
pub struct RoundData<Id, Timer, Incoming, Outgoing> {
	/// Local voter id (if any).
	pub voter_id: Option<Id>,
	/// Timer before prevotes can be cast. This should be `start + 2T`,
	/// where T is the gossip time estimate.
	pub prevote_timer: Timer,
	/// Timer before precommits can be cast. This should be `start + 4T`,
	/// where T is the gossip time estimate.
	pub precommit_timer: Timer,
	/// Incoming messages.
	pub incoming: Incoming,
	/// Outgoing messages.
	pub outgoing: Outgoing,
}

#[async_trait]
pub trait Environment<Hash, Number>: Chain<Hash, Number> + Clone
where
	Hash: Eq,
{
	type Id: Clone + Debug + Ord;
	type Signature: Clone + Eq;
	type Error: From<Error>;
	type Timer: Future<Output = ()> + Unpin;
	type Incoming: Stream<Item = Result<SignedMessage<Hash, Number, Self::Signature, Self::Id>, Self::Error>>
		+ Unpin;
	type Outgoing: Sink<Message<Hash, Number>, Error = Self::Error> + Unpin;

	async fn best_chain_containing(
		&self,
		base: Hash,
	) -> Result<Option<(Hash, Number)>, Self::Error>;

	/// Called when a block should be finalized.
	async fn finalize_block(
		&self,
		hash: Hash,
		number: Number,
		round: u64,
		commit: Commit<Hash, Number, Self::Signature, Self::Id>,
	) -> Result<(), Self::Error>;

	async fn round_data(
		&self,
		round: u64,
	) -> RoundData<Self::Id, Self::Timer, Self::Incoming, Self::Outgoing>;

	/// Return a timer that will be used to delay the broadcast of a commit
	/// message. This delay should not be static to minimize the amount of
	/// commit messages that are sent (e.g. random value in [0, 1] seconds).
	/// NOTE: this function is not async as we are returning a named future.
	fn round_commit_timer(&self) -> Self::Timer;
}
