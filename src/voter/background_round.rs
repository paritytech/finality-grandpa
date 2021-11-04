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

//! Rounds that are not the current best round are run in the background.
//!
//! This module provides utilities for managing those rounds and producing commit
//! messages from them. Any rounds that become irrelevant are dropped.
//!
//! Create a `PastRounds` struct, and drive it to completion while:
//!   - Informing it of any new finalized block heights
//!   - Passing it any validated commits (so backgrounded rounds don't produce conflicting ones)

use std::fmt::Debug;

use futures::{
	channel::{mpsc, oneshot},
	future, select, stream, FutureExt, SinkExt, StreamExt,
};
use log::{debug, trace};

use crate::{
	round::{Round, RoundParams, State as RoundState},
	validate_commit,
	voter::{Callback, CommitProcessingOutcome, Environment as EnvironmentT},
	BlockNumberOps, Commit, CommitValidationResult, Message, SignedMessage, SignedPrecommit,
	VoterSet,
};

pub struct BackgroundRound<Hash, Number, Environment>
where
	Hash: Ord,
	Environment: EnvironmentT<Hash, Number>,
{
	environment: Environment,
	round_incoming: stream::Fuse<Environment::Incoming>,
	round: Round<Environment::Id, Hash, Number, Environment::Signature>,
	round_state_updates: mpsc::Sender<RoundState<Hash, Number>>,
	commit_incoming: mpsc::Receiver<(
		Commit<Hash, Number, Environment::Signature, Environment::Id>,
		Callback<CommitProcessingOutcome>,
	)>,
	commit_outgoing: mpsc::Sender<Commit<Hash, Number, Environment::Signature, Environment::Id>>,
	commit_timer: future::Fuse<Environment::Timer>,
	best_commit: Option<Commit<Hash, Number, Environment::Signature, Environment::Id>>,
}

impl<Hash, Number, Environment> BackgroundRound<Hash, Number, Environment>
where
	Hash: Clone + Debug + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	pub async fn new(
		environment: Environment,
		round_incoming: stream::Fuse<Environment::Incoming>,
		round: Round<Environment::Id, Hash, Number, Environment::Signature>,
		round_state_updates: mpsc::Sender<RoundState<Hash, Number>>,
		commit_incoming: mpsc::Receiver<(
			Commit<Hash, Number, Environment::Signature, Environment::Id>,
			Callback<CommitProcessingOutcome>,
		)>,
		commit_outgoing: mpsc::Sender<
			Commit<Hash, Number, Environment::Signature, Environment::Id>,
		>,
	) -> BackgroundRound<Hash, Number, Environment> {
		let commit_timer = environment.round_commit_timer().fuse();

		BackgroundRound {
			environment,
			round_incoming,
			round,
			round_state_updates,
			commit_incoming,
			commit_outgoing,
			commit_timer,
			best_commit: None,
		}
	}

	pub async fn restore(
		environment: Environment,
		voters: VoterSet<Environment::Id>,
		round_number: u64,
		round_base: (Hash, Number),
		round_votes: Vec<SignedMessage<Hash, Number, Environment::Signature, Environment::Id>>,
		round_state_updates: mpsc::Sender<RoundState<Hash, Number>>,
		commit_incoming: mpsc::Receiver<(
			Commit<Hash, Number, Environment::Signature, Environment::Id>,
			Callback<CommitProcessingOutcome>,
		)>,
		commit_outgoing: mpsc::Sender<
			Commit<Hash, Number, Environment::Signature, Environment::Id>,
		>,
	) -> Option<BackgroundRound<Hash, Number, Environment>> {
		let round_data = environment.round_data(round_number).await;
		let round = Round::new(RoundParams { voters, base: round_base, round_number });

		let mut background_round = BackgroundRound::new(
			environment,
			round_data.incoming.fuse(),
			round,
			round_state_updates,
			commit_incoming,
			commit_outgoing,
		)
		.await;

		for vote in round_votes {
			// bail if any votes are bad.
			background_round.handle_incoming_round_message(vote).await.ok()?;
		}

		if background_round.round.state().completable {
			Some(background_round)
		} else {
			None
		}
	}

	async fn commit(&mut self) -> Result<(), Environment::Error> {
		let round_finalized = self.round.state().finalized.unwrap();

		let commit = Commit {
			target_hash: round_finalized.0.clone(),
			target_number: round_finalized.1,
			precommits: self
				.round
				.finalizing_precommits(&self.environment)
				.expect(
					"always returns none if something was finalized; this is checked above; qed",
				)
				.collect(),
		};

		debug!(target: "afg",
			"Committing: round_number = {}, target_number = {:?}, target_hash = {:?}",
			self.round.number(),
			commit.target_number,
			commit.target_hash,
		);

		// FIXME: deal with error due to dropped channel receiver
		let _ = self.commit_outgoing.send(commit.clone()).await;

		self.best_commit = Some(commit);

		Ok(())
	}

	async fn handle_incoming_round_message(
		&mut self,
		message: SignedMessage<Hash, Number, Environment::Signature, Environment::Id>,
	) -> Result<(), Environment::Error> {
		let SignedMessage { message, signature, id } = message;

		if !self
			.environment
			.is_equal_or_descendent_of(self.round.base().0, message.target().0.clone())
		{
			trace!(target: "afg",
				"Ignoring message targeting {:?} lower than round base {:?}",
				message.target(), self.round.base(),
			);

			return Ok(())
		}

		let initial_round_state = self.round.state();
		match message {
			Message::Prevote(prevote) => {
				let import_result =
					self.round.import_prevote(&self.environment, prevote, id, signature)?;

				if let Some(_equivocation) = import_result.equivocation {
					// TODO: handle equivocation
					// self.environment.prevote_equivocation(self.round.number(), equivocation);
				}
			},
			Message::Precommit(precommit) => {
				let import_result =
					self.round.import_precommit(&self.environment, precommit, id, signature)?;

				if let Some(_equivocation) = import_result.equivocation {
					// TODO: handle equivocation
					// self.environment.precommit_equivocation(self.round.number(), equivocation);
				}
			},
			Message::PrimaryPropose(_primary) => {
				debug!("ignoring primary proposal message for background round");
			},
		}

		let new_round_state = self.round.state();
		if new_round_state != initial_round_state {
			// TODO: create timer to deal with full round state updates channel
			let _ = self.round_state_updates.send(new_round_state).await;
		}

		Ok(())
	}

	async fn handle_incoming_commit_message(
		&mut self,
		commit: Commit<Hash, Number, Environment::Signature, Environment::Id>,
	) -> Result<CommitProcessingOutcome, Environment::Error> {
		// ignore commits for a block lower than we already finalized
		if commit.target_number < self.round.finalized().map_or_else(Number::zero, |(_, n)| *n) {
			return Ok(CommitProcessingOutcome::Good)
		}

		let commit_validation_result =
			validate_commit(&commit, self.round.voters(), &self.environment)?;

		if commit_validation_result.ghost.is_some() {
			for SignedPrecommit { precommit, signature, id } in commit.precommits.iter().cloned() {
				let _import_result =
					self.round.import_precommit(&self.environment, precommit, id, signature)?;

				// TODO: handle equivocations
				// if let ImportResult { equivocation: Some(e), .. } = import_result {
				// 	self.env.precommit_equivocation(self.round_number(), e);
				// }
			}

			self.best_commit = Some(commit);
		}

		Ok(commit_validation_result.into())
	}

	fn is_concluded(&self) -> bool {
		// we haven't committed or received any valid commit for this round yet
		if self.best_commit.is_none() {
			return false
		}

		// the round is only concluded when the round's estimate has been finalized.
		// NOTE: it's possible that the estimate could be finalized in the following
		// round, since that is a condition for the following round to be completable
		// we don't need to deal with that here, i.e. completing the following round
		// will forcefully conclude this one.
		if let RoundState {
			estimate: Some((_, previous_round_estimate)),
			finalized: Some((_, previous_round_finalized)),
			..
		} = self.round.state()
		{
			return previous_round_estimate <= previous_round_finalized
		}

		true
	}

	pub async fn run(mut self) -> Result<(), Environment::Error> {
		// FIXME: handle finality notifications
		loop {
			select! {
				round_message = self.round_incoming.select_next_some() => {
					self.handle_incoming_round_message(round_message?).await?;
				},
				(commit, mut callback) = self.commit_incoming.select_next_some() => {
					callback.run(self.handle_incoming_commit_message(commit).await?)
				},
				_ = &mut self.commit_timer => {
					self.commit().await?;
				}
			}

			if self.is_concluded() {
				break
			}
		}

		Ok(())
	}
}
