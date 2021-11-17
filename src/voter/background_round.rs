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

use futures::{
	channel::{mpsc, oneshot},
	future, select, stream, FutureExt, SinkExt, StreamExt,
};
use log::{debug, trace};

use crate::{
	round::{Round, RoundParams, State as RoundState},
	validate_commit,
	voter::{Callback, CommitProcessingOutcome, Environment as EnvironmentT},
	Commit, ImportResult, Message, SignedMessage, SignedPrecommit, VoterSet,
};

pub struct ConcludedRound<Environment>
where
	Environment: EnvironmentT,
{
	pub round:
		Round<Environment::Id, Environment::Hash, Environment::Number, Environment::Signature>,
}

pub struct BackgroundRoundCommit<Hash, Number, Id, Signature> {
	pub round_number: u64,
	pub commit: Commit<Hash, Number, Signature, Id>,
	pub broadcast: bool,
}

pub struct BackgroundRound<Environment>
where
	Environment: EnvironmentT,
{
	environment: Environment,
	round_incoming: stream::Fuse<Environment::Incoming>,
	round: Round<Environment::Id, Environment::Hash, Environment::Number, Environment::Signature>,
	round_state_updates: mpsc::Sender<RoundState<Environment::Hash, Environment::Number>>,
	commit_outgoing: mpsc::Sender<
		BackgroundRoundCommit<
			Environment::Hash,
			Environment::Number,
			Environment::Id,
			Environment::Signature,
		>,
	>,
	commit_timer: future::Fuse<Environment::Timer>,
	best_commit: Option<
		Commit<Environment::Hash, Environment::Number, Environment::Signature, Environment::Id>,
	>,
}

/// The background round is forcefully concluded when this handle is dropped.
pub struct BackgroundRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
	commits_sender: mpsc::Sender<(
		Commit<Environment::Hash, Environment::Number, Environment::Signature, Environment::Id>,
		Callback<CommitProcessingOutcome>,
	)>,
	force_conclude_sender: Option<oneshot::Sender<()>>,
}

impl<Environment> Drop for BackgroundRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
	fn drop(&mut self) {
		if let Some(sender) = self.force_conclude_sender.take() {
			let _ = sender.send(());
		}
	}
}

impl<Environment> BackgroundRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
	pub async fn send_commit(
		&mut self,
		commit: Commit<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
		callback: Callback<CommitProcessingOutcome>,
	) -> Result<(), Environment::Error> {
		// FIXME: deal with error due to dropped channel
		let _ = self.commits_sender.send((commit, callback)).await;
		Ok(())
	}
}

impl<Environment> BackgroundRound<Environment>
where
	Environment: EnvironmentT,
{
	pub async fn new(
		environment: Environment,
		round_incoming: stream::Fuse<Environment::Incoming>,
		round: Round<
			Environment::Id,
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
		>,
		round_state_updates: mpsc::Sender<RoundState<Environment::Hash, Environment::Number>>,
		commit_outgoing: mpsc::Sender<
			BackgroundRoundCommit<
				Environment::Hash,
				Environment::Number,
				Environment::Id,
				Environment::Signature,
			>,
		>,
	) -> BackgroundRound<Environment> {
		let commit_timer = environment.round_commit_timer().fuse();

		BackgroundRound {
			environment,
			round_incoming,
			round,
			round_state_updates,
			commit_outgoing,
			commit_timer,
			best_commit: None,
		}
	}

	pub async fn restore(
		environment: Environment,
		voters: VoterSet<Environment::Id>,
		round_number: u64,
		round_base: (Environment::Hash, Environment::Number),
		round_votes: Vec<
			SignedMessage<
				Environment::Hash,
				Environment::Number,
				Environment::Signature,
				Environment::Id,
			>,
		>,
		round_state_updates: mpsc::Sender<RoundState<Environment::Hash, Environment::Number>>,
		commit_outgoing: mpsc::Sender<
			BackgroundRoundCommit<
				Environment::Hash,
				Environment::Number,
				Environment::Id,
				Environment::Signature,
			>,
		>,
	) -> Option<BackgroundRound<Environment>> {
		let round_data = environment.round_data(round_number).await;
		let round = Round::new(RoundParams { voters, base: round_base, round_number });

		let mut background_round = BackgroundRound::new(
			environment,
			round_data.incoming.fuse(),
			round,
			round_state_updates,
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
		let _ = self
			.commit_outgoing
			.send(BackgroundRoundCommit {
				round_number: self.round.number(),
				commit: commit.clone(),
				broadcast: true,
			})
			.await;

		self.best_commit = Some(commit);

		Ok(())
	}

	async fn handle_incoming_round_message(
		&mut self,
		message: SignedMessage<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
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

				if let Some(equivocation) = import_result.equivocation {
					self.environment.prevote_equivocation(self.round.number(), equivocation).await;
				}
			},
			Message::Precommit(precommit) => {
				let import_result =
					self.round.import_precommit(&self.environment, precommit, id, signature)?;

				if let Some(equivocation) = import_result.equivocation {
					self.environment
						.precommit_equivocation(self.round.number(), equivocation)
						.await;
				}
			},
			Message::PrimaryPropose(_primary) => {
				debug!("ignoring primary proposal message for background round");
			},
		}

		let new_round_state = self.round.state();

		// FIXME: send updates while importing commits as well
		// move this to main loop
		if new_round_state != initial_round_state {
			// TODO: create timer to deal with full round state updates channel
			let _ = self.round_state_updates.send(new_round_state).await;
		}

		Ok(())
	}

	async fn handle_incoming_commit_message(
		&mut self,
		commit: Commit<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
	) -> Result<CommitProcessingOutcome, Environment::Error> {
		use num::Zero;

		// ignore commits for a block lower than we already finalized
		if commit.target_number <
			self.round.finalized().map_or_else(Environment::Number::zero, |(_, n)| *n)
		{
			return Ok(CommitProcessingOutcome::Good)
		}

		let commit_validation_result =
			validate_commit(&commit, self.round.voters(), &self.environment)?;

		if commit_validation_result.ghost.is_some() {
			for SignedPrecommit { precommit, signature, id } in commit.precommits.iter().cloned() {
				let import_result =
					self.round.import_precommit(&self.environment, precommit, id, signature)?;

				if let ImportResult { equivocation: Some(e), .. } = import_result {
					self.environment.precommit_equivocation(self.round.number(), e).await;
				}
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

	async fn run(
		&mut self,
		mut commits_receiver: mpsc::Receiver<(
			Commit<Environment::Hash, Environment::Number, Environment::Signature, Environment::Id>,
			Callback<CommitProcessingOutcome>,
		)>,
		mut force_conclude_receiver: oneshot::Receiver<()>,
	) -> Result<(), Environment::Error> {
		loop {
			let pre_finalized = self.round.state().finalized;

			select! {
				round_message = self.round_incoming.select_next_some() => {
					self.handle_incoming_round_message(round_message?).await?;
				},
				(commit, mut callback) = commits_receiver.select_next_some() => {
					callback.run(self.handle_incoming_commit_message(commit).await?)
				},
				_ = &mut self.commit_timer => {
					self.commit().await?;
				}
				_ = force_conclude_receiver => {
					break;
				}
			}

			let post_finalized = self.round.state().finalized;

			// FIXME: not being formatted
			if pre_finalized != post_finalized {
				if let Some(finalized) = post_finalized {
					// FIXME: deal with error
					let _ = self.commit_outgoing.send(BackgroundRoundCommit {
						round_number: self.round.number(),
						commit: Commit {
							target_hash: finalized.0.clone(),
							target_number: finalized.1,
							precommits: self.round.finalizing_precommits(&self.environment)
								.expect("always returns none if something was finalized; this is checked above; qed")
								.collect(),
						},
						broadcast: false,
					}).await;
				}
			}

			if self.is_concluded() {
				break
			}
		}

		debug!(target: "afg", "Concluded background round: {}", self.round.number());

		Ok(())
	}

	pub fn start(
		mut self,
	) -> (
		impl futures::Future<Output = Result<ConcludedRound<Environment>, Environment::Error>>,
		BackgroundRoundHandle<Environment>,
	) {
		let (force_conclude_sender, force_conclude_receiver) = oneshot::channel();
		let (commits_sender, commits_receiver) = mpsc::channel(4);

		let run = async {
			self.run(commits_receiver, force_conclude_receiver).await?;
			Ok(ConcludedRound { round: self.round })
		};

		let handle = BackgroundRoundHandle {
			commits_sender,
			force_conclude_sender: Some(force_conclude_sender),
		};

		(run, handle)
	}
}
