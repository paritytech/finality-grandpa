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

use std::sync::Arc;

use futures::{
	channel::{mpsc, oneshot},
	future, select, stream, FutureExt, SinkExt, StreamExt,
};
use log::{debug, trace};

use crate::{
	round::{Round, RoundParams, State as RoundState},
	validate_commit,
	voter::{Callback, CommitProcessingOutcome, Environment as EnvironmentT},
	Commit, ImportResult, Message, SignedMessage, SignedPrecommit, VoterSet, LOG_TARGET,
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
#[derive(Clone)]
pub struct BackgroundRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
	commits_sender: mpsc::Sender<(
		Commit<Environment::Hash, Environment::Number, Environment::Signature, Environment::Id>,
		Callback<CommitProcessingOutcome>,
	)>,
	report_round_state_sender:
		mpsc::Sender<oneshot::Sender<super::report::RoundState<Environment::Id>>>,
	force_conclude_sender: Arc<Option<oneshot::Sender<()>>>,
}

impl<Environment> Drop for BackgroundRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
	fn drop(&mut self) {
		// FIXME: add note about this
		if let Some(force_conclude_sender) = Arc::get_mut(&mut self.force_conclude_sender) {
			if let Some(sender) = force_conclude_sender.take() {
				let _ = sender.send(());
			}
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

	pub async fn report_round_state(
		&mut self,
	) -> Result<super::report::RoundState<Environment::Id>, Environment::Error> {
		let (sender, receiver) = oneshot::channel();

		// FIXME: Handle error
		let _ = self.report_round_state_sender.send(sender).await;
		let round_state = receiver.await.unwrap();

		Ok(round_state)
	}
}

pub type BackgroundRoundStateUpdatesReceiver<Environment> = mpsc::Receiver<
	RoundState<<Environment as crate::Chain>::Hash, <Environment as crate::Chain>::Number>,
>;

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

		let mut background_round =
			BackgroundRound::new(environment, round_data.incoming.fuse(), round, commit_outgoing)
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

		debug!(
			target: LOG_TARGET,
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
			trace!(
				target: LOG_TARGET,
				"Ignoring message targeting {:?} lower than round base {:?}",
				message.target(),
				self.round.base(),
			);

			return Ok(())
		}

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
				debug!(
					target: LOG_TARGET,
					"ignoring primary proposal message for background round"
				);
			},
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

		if commit_validation_result.is_valid() {
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

	async fn handle_report_round_state_request(
		&mut self,
		response_sender: oneshot::Sender<super::report::RoundState<Environment::Id>>,
	) -> Result<(), Environment::Error> {
		let round_state = super::report::RoundState {
			total_weight: self.round.voters().total_weight(),
			threshold_weight: self.round.threshold(),
			prevote_current_weight: self.round.prevote_participation().0,
			prevote_ids: self.round.prevotes().into_iter().map(|pv| pv.0).collect(),
			precommit_current_weight: self.round.precommit_participation().0,
			precommit_ids: self.round.precommits().into_iter().map(|pv| pv.0).collect(),
		};

		// FIXME: deal with error
		let _ = response_sender.send(round_state);

		Ok(())
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
		mut report_round_state_receiver: mpsc::Receiver<
			oneshot::Sender<super::report::RoundState<Environment::Id>>,
		>,
		mut force_conclude_receiver: oneshot::Receiver<()>,
		mut round_state_updates_sender: mpsc::Sender<
			RoundState<Environment::Hash, Environment::Number>,
		>,
	) -> Result<(), Environment::Error> {
		loop {
			let initial_round_state = self.round.state();

			select! {
				round_message = self.round_incoming.select_next_some() => {
					self.handle_incoming_round_message(round_message?).await?;
				},
				(commit, mut callback) = commits_receiver.select_next_some() => {
					callback.run(self.handle_incoming_commit_message(commit).await?)
				},
				response_sender = report_round_state_receiver.select_next_some() => {
					self.handle_report_round_state_request(response_sender).await?;
				},
				_ = &mut self.commit_timer => {
					self.commit().await?;
				}
				_ = force_conclude_receiver => {
					break;
				}
			}

			let new_round_state = self.round.state();

			if new_round_state != initial_round_state {
				if new_round_state.finalized != initial_round_state.finalized {
					if let Some(finalized) = new_round_state.finalized.as_ref() {
						let precommits = self
							.round
							.finalizing_precommits(&self.environment)
							.expect(
								"always returns none if something was finalized; \
								this is checked above; qed",
							)
							.collect();

						let commit = Commit {
							target_hash: finalized.0.clone(),
							target_number: finalized.1,
							precommits,
						};

						// FIXME: deal with error
						let _ = self
							.commit_outgoing
							.send(BackgroundRoundCommit {
								commit,
								round_number: self.round.number(),
								broadcast: false,
							})
							.await;
					}
				}

				// TODO: create timer to deal with full round state updates channel
				let _ = round_state_updates_sender.send(new_round_state).await;
			}

			if self.is_concluded() {
				break
			}
		}

		debug!(target: LOG_TARGET, "Concluded background round: {}", self.round.number());

		Ok(())
	}

	pub fn start(
		mut self,
	) -> (
		impl futures::Future<Output = Result<ConcludedRound<Environment>, Environment::Error>>,
		BackgroundRoundStateUpdatesReceiver<Environment>,
		BackgroundRoundHandle<Environment>,
	) {
		let (force_conclude_sender, force_conclude_receiver) = oneshot::channel();
		let (round_state_updates_sender, round_state_updates_receiver) = mpsc::channel(4);
		let (commits_sender, commits_receiver) = mpsc::channel(4);
		let (report_round_state_sender, report_round_state_receiver) = mpsc::channel(4);

		let run = async {
			self.run(
				commits_receiver,
				report_round_state_receiver,
				force_conclude_receiver,
				round_state_updates_sender,
			)
			.await?;
			Ok(ConcludedRound { round: self.round })
		};

		let handle = BackgroundRoundHandle {
			commits_sender,
			report_round_state_sender,
			force_conclude_sender: Arc::new(Some(force_conclude_sender)),
		};

		(run, round_state_updates_receiver, handle)
	}
}
