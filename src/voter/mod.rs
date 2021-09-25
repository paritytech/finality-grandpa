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

use self::Environment as EnvironmentT;

// type GlobalIn = Stream<Item = Result<CommunicationIn<H, N, E::Signature, E::Id>, E::Error>> + Unpin;
// type GlobalOut = Sink<CommunicationOut<H, N, E::Signature, E::Id>, Error = E::Error> + Unpin;

struct Voter<Hash, Number, Environment>
where
	Hash: Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	environment: Environment,
	voting_round: VotingRound<Hash, Number, Environment>,
	background_round: Option<BackgroundRound<Hash, Number, Environment>>,
}

impl<Hash, Number, Environment> Voter<Hash, Number, Environment>
where
	Hash: Clone + Debug + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	async fn new(
		environment: Environment,
		voters: VoterSet<Environment::Id>,
		// TODO: handle global communication
		// global_comms: (GlobalIn, GlobalOut),
		last_round_number: u64,
		last_round_votes: Vec<SignedMessage<Hash, Number, Environment::Signature, Environment::Id>>,
		last_round_base: (Hash, Number),
		best_finalized: (Hash, Number),
	) -> Voter<Hash, Number, Environment> {
		// TODO: create last round
		let last_round_state = RoundState::genesis(last_round_base.clone());

		let (_, previous_round_state_updates) = futures::channel::mpsc::channel(4);

		let voting_round = VotingRound::new(
			environment.clone(),
			last_round_number + 1,
			voters.clone(),
			// TODO: use finalized from previous round state?
			best_finalized,
			last_round_state,
			previous_round_state_updates,
		)
		.await;

		Voter { environment, voting_round, background_round: None }
	}

	pub async fn run(mut self) -> Result<(), Environment::Error> {
		let mut background_round = self
			.background_round
			.take()
			.map(|round| round.run().fuse())
			.unwrap_or(Fuse::terminated());

		let mut voting_round = self.voting_round.run().fuse();

		pin_mut!(background_round, voting_round);

		loop {
			// FIXME: this only needs to be recreated when creating a new background round
			let (commit_in, commit_out) = futures::channel::mpsc::channel(4);

			log::trace!("here1");
			select! {
				completable_round = voting_round => {
					log::trace!("here2");

					let completable_round = completable_round?;

					let (previous_round_state_updates_tx, previous_round_state_updates_rx) = futures::channel::mpsc::channel(4);

					let completable_round_number = completable_round.round.number();
					let completable_round_state = completable_round.round.state();
					// FIXME: deal with unwrap
					let completable_round_finalized = completable_round_state.finalized.clone().unwrap();
					let voters = completable_round.round.voters().clone();

					debug!("completed voting round, finalized: {:?}", completable_round_finalized);

					self.environment.finalize_block(
						completable_round_finalized.0.clone(),
						completable_round_finalized.1,
						completable_round_number,
						Commit {
							target_hash: completable_round_finalized.0.clone(),
							target_number: completable_round_finalized.1,
							precommits: completable_round.round.finalizing_precommits(&self.environment)
								.expect("always returns none if something was finalized; this is checked above; qed")
								.collect(),
						},
					).await?;

					let new_background_round = BackgroundRound::new(
						self.environment.clone(),
						completable_round.incoming,
						completable_round.round,
						previous_round_state_updates_tx,
						commit_out,
					).await;

					let new_voting_round = VotingRound::new(
						self.environment.clone(),
						completable_round_number + 1,
						voters.clone(),
						completable_round_finalized,
						completable_round_state,
						previous_round_state_updates_rx,
					).await;

					background_round.set(new_background_round.run().fuse());
					voting_round.set(new_voting_round.run().fuse());
				},
				_ = background_round => {
					// concluded
					debug!("concluded background round");
				},
			}
		}
	}
}
