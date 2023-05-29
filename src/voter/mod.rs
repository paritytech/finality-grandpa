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

use std::fmt::Debug;

use async_trait::async_trait;
use futures::{
	channel::{mpsc, oneshot},
	future,
	future::BoxFuture,
	select_biased,
	stream::{self, FuturesUnordered},
	Future, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use log::{debug, trace};

use crate::{
	round::{Round, RoundParams, State as RoundState},
	validate_commit,
	voter::{
		background_round::{BackgroundRound, ConcludedRound},
		voting_round::{CompletableRound, VotingRound, VotingRoundHandle},
	},
	weights::VoteWeight,
	CatchUp, Chain, Commit, CommitValidationResult, CompactCommit, Equivocation, Error,
	HistoricalVotes, Message, Precommit, Prevote, PrimaryPropose, SignedMessage, SignedPrecommit,
	SignedPrevote, VoterSet, LOG_TARGET,
};

use self::{
	background_round::{BackgroundRoundCommit, BackgroundRoundHandle},
	Environment as EnvironmentT,
};

mod background_round;
#[cfg(test)]
mod tests;
mod voting_round;

/// Communication between nodes that is not round-localized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalCommunicationOutgoing<Hash, Number, Signature, Id> {
	/// A commit message.
	Commit(u64, Commit<Hash, Number, Signature, Id>),
}

/// The outcome of processing a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitProcessingOutcome {
	/// It was beneficial to process this commit.
	Good,
	/// It wasn't beneficial to process this commit. We wasted resources.
	Bad {
		num_precommits: usize,
		num_duplicated_precommits: usize,
		num_equivocations: usize,
		num_invalid_voters: usize,
	},
}

impl From<CommitValidationResult> for CommitProcessingOutcome {
	fn from(result: CommitValidationResult) -> Self {
		if result.is_valid() {
			CommitProcessingOutcome::Good
		} else {
			CommitProcessingOutcome::Bad {
				num_precommits: result.num_precommits,
				num_duplicated_precommits: result.num_duplicated_precommits,
				num_equivocations: result.num_equivocations,
				num_invalid_voters: result.num_invalid_voters,
			}
		}
	}
}

/// The outcome of processing a catch up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatchUpProcessingOutcome {
	/// It was beneficial to process this catch up.
	Good,
	/// It wasn't beneficial to process this catch up, it is invalid and we
	/// wasted resources.
	Bad,
	/// The catch up wasn't processed because it is useless, e.g. it is for a
	/// round lower than we're currently in.
	Useless,
}

/// Callback used to pass information about the outcome of importing a given
/// message (e.g. vote, commit, catch up). Useful to propagate data to the
/// network after making sure the import is successful.
pub enum Callback<O> {
	/// Default value.
	Blank,
	/// Callback to execute given a processing outcome.
	Work(Box<dyn FnMut(O) + Send>),
}

#[cfg(any(test, feature = "test-helpers"))]
impl<O> Clone for Callback<O> {
	fn clone(&self) -> Self {
		Callback::Blank
	}
}

impl<O> Callback<O> {
	/// Do the work associated with the callback, if any.
	pub fn run(&mut self, o: O) {
		match self {
			Callback::Blank => {},
			Callback::Work(cb) => cb(o),
		}
	}
}

#[cfg(any(test, feature = "test-helpers"))]
impl<O> std::fmt::Debug for Callback<O> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Callback::Blank => write!(f, "Callback::Blank"),
			Callback::Work(_) => write!(f, "Callback::Work"),
		}
	}
}

/// Communication between nodes that is not round-localized.
#[cfg_attr(any(test, feature = "test-helpers"), derive(Clone, Debug))]
pub enum GlobalCommunicationIncoming<Hash, Number, Signature, Id> {
	/// A commit message.
	/// TODO: replace usage of callback with oneshot sender of processing outcome
	Commit(u64, CompactCommit<Hash, Number, Signature, Id>, Callback<CommitProcessingOutcome>),
	/// A catch up message.
	CatchUp(CatchUp<Hash, Number, Signature, Id>, Callback<CatchUpProcessingOutcome>),
}

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
pub trait Environment: Chain + Clone {
	type Id: Clone + Debug + Ord;
	type Signature: Clone + Eq;
	type Error: From<Error>;
	// FIXME: is the unpin really needed?
	// FIXME: maybe it makes sense to make this a FusedFuture to avoid having to wrap it in Fuse<...>
	type Timer: Future<Output = ()> + Unpin;
	type Incoming: Stream<
			Item = Result<
				SignedMessage<Self::Hash, Self::Number, Self::Signature, Self::Id>,
				Self::Error,
			>,
		> + Unpin;
	type Outgoing: Sink<Message<Self::Hash, Self::Number>, Error = Self::Error> + Unpin;

	async fn best_chain_containing(
		&self,
		base: Self::Hash,
	) -> Result<Option<(Self::Hash, Self::Number)>, Self::Error>;

	async fn round_data(
		&self,
		round: u64,
	) -> RoundData<Self::Id, Self::Timer, Self::Incoming, Self::Outgoing>;

	/// Return a timer that will be used to delay the broadcast of a commit
	/// message. This delay should not be static to minimize the amount of
	/// commit messages that are sent (e.g. random value in [0, 1] seconds).
	/// NOTE: this function is not async as we are returning a named future.
	fn round_commit_timer(&self) -> Self::Timer;

	/// Note that we've done a primary proposal in the given round.
	async fn proposed(
		&self,
		round: u64,
		propose: PrimaryPropose<Self::Hash, Self::Number>,
	) -> Result<(), Self::Error>;

	/// Note that we have prevoted in the given round.
	async fn prevoted(
		&self,
		round: u64,
		prevote: Prevote<Self::Hash, Self::Number>,
	) -> Result<(), Self::Error>;

	/// Note that we have precommitted in the given round.
	async fn precommitted(
		&self,
		round: u64,
		precommit: Precommit<Self::Hash, Self::Number>,
	) -> Result<(), Self::Error>;

	/// Note that a round is completed. This is called when a round has been
	/// voted in and the next round can start. The round may continue to be run
	/// in the background until _concluded_.
	/// Should return an error when something fatal occurs.
	async fn completed(
		&self,
		round: u64,
		state: RoundState<Self::Hash, Self::Number>,
		base: (Self::Hash, Self::Number),
		votes: &HistoricalVotes<Self::Hash, Self::Number, Self::Signature, Self::Id>,
	) -> Result<(), Self::Error>;

	/// Note that a round has concluded. This is called when a round has been
	/// `completed` and additionally, the round's estimate has been finalized.
	///
	/// There may be more votes than when `completed`, and it is the responsibility
	/// of the `Environment` implementation to deduplicate. However, the caller guarantees
	/// that the votes passed to `completed` for this round are a prefix of the votes passed here.
	async fn concluded(
		&self,
		round: u64,
		state: RoundState<Self::Hash, Self::Number>,
		base: (Self::Hash, Self::Number),
		votes: &HistoricalVotes<Self::Hash, Self::Number, Self::Signature, Self::Id>,
	) -> Result<(), Self::Error>;

	/// Called when a block should be finalized.
	async fn finalize_block(
		&self,
		hash: Self::Hash,
		number: Self::Number,
		round: u64,
		commit: Commit<Self::Hash, Self::Number, Self::Signature, Self::Id>,
	) -> Result<(), Self::Error>;

	/// Note that an equivocation in prevotes has occurred.
	async fn prevote_equivocation(
		&self,
		round: u64,
		equivocation: Equivocation<Self::Id, Prevote<Self::Hash, Self::Number>, Self::Signature>,
	);

	/// Note that an equivocation in precommits has occurred.
	async fn precommit_equivocation(
		&self,
		round: u64,
		equivocation: Equivocation<Self::Id, Precommit<Self::Hash, Self::Number>, Self::Signature>,
	);
}

type VotingRoundFuture<Environment> =
	BoxFuture<'static, Result<CompletableRound<Environment>, <Environment as EnvironmentT>::Error>>;

type BackgroundRoundFuture<Environment> =
	BoxFuture<'static, Result<ConcludedRound<Environment>, <Environment as EnvironmentT>::Error>>;

pub struct Voter<Environment, GlobalIncoming, GlobalOutgoing>
where
	Environment: EnvironmentT,
{
	///
	voters: VoterSet<Environment::Id>,
	///
	environment: Environment,
	///
	global_incoming: stream::Fuse<GlobalIncoming>,
	global_outgoing: GlobalOutgoing,
	/// The best finalized block so far.
	best_finalized: (Environment::Hash, Environment::Number),
	/// The best finalized block so far that has been finalized through the normal round
	/// lifecycle (i.e. blocks finalized through global commits are not accounted here).
	best_finalized_in_rounds: (Environment::Hash, Environment::Number),
	/// The round number of the voting round we're currently processing.
	current_round_number: u64,
	/// The future representing the current voting round process.
	voting_round: future::Fuse<VotingRoundFuture<Environment>>,
	/// A handle to the latest voting round.
	voting_round_handle: VotingRoundHandle<Environment>,
	/// The future representing the background round process.
	background_rounds: FuturesUnordered<BackgroundRoundFuture<Environment>>,
	/// A handle to the background round.
	background_round_handle: Option<BackgroundRoundHandle<Environment>>,
	/// A channel for receiving new commits from the background round task.
	from_background_round_commits_receiver: mpsc::Receiver<
		BackgroundRoundCommit<
			Environment::Hash,
			Environment::Number,
			Environment::Id,
			Environment::Signature,
		>,
	>,
	/// A channel to be used by background rounds to send commits to the main voter task. We keep it
	/// here since we'll need to clone it and pass it on everytime we instantiate a new background
	/// round.
	from_background_round_commits_sender: mpsc::Sender<
		BackgroundRoundCommit<
			Environment::Hash,
			Environment::Number,
			Environment::Id,
			Environment::Signature,
		>,
	>,
	pending_report_voter_state_requests:
		FuturesUnordered<BoxFuture<'static, Result<(), Environment::Error>>>,
}

#[derive(Clone)]
pub struct VoterHandle<Environment>
where
	Environment: EnvironmentT,
{
	report_voter_state_sender: mpsc::Sender<oneshot::Sender<report::VoterState<Environment::Id>>>,
}

impl<Environment> VoterHandle<Environment>
where
	Environment: EnvironmentT,
{
	pub async fn report_voter_state(
		&mut self,
	) -> Result<report::VoterState<Environment::Id>, Environment::Error> {
		let (sender, receiver) = oneshot::channel();

		// FIXME: Handle error
		let _ = self.report_voter_state_sender.send(sender).await;
		let voter_state = receiver.await.unwrap();

		Ok(voter_state)
	}
}

// FIXME: add a way to do a clean shutdown this will make sure the background round is concluded and
// pending commits are sent out (currently there's probably an issue with commits not being set out
// on authority set changes, since the voter is abruptly stopped)

// FIXME: create Handle interface for VotingRound and BackgroundRound, this should hide a way all the
// channel stuff and just look like async method calls.
// it is also possible to merge multiple channels by sending Request enum messages instead for IO
// between actors
// use Mutex<Sender> ToBackground message types

impl<Environment, GlobalIncoming, GlobalOutgoing> Voter<Environment, GlobalIncoming, GlobalOutgoing>
where
	Environment: EnvironmentT + Send + Sync + 'static,
	Environment::Hash: Send + Sync,
	Environment::Number: Send + Sync,
	Environment::Error: Send,
	Environment::Id: Send + Sync,
	Environment::Signature: Send + Sync,
	Environment::Timer: Send + Sync,
	Environment::Incoming: Send + Sync,
	Environment::Outgoing: Send + Sync,
	GlobalIncoming: Stream<
			Item = Result<
				GlobalCommunicationIncoming<
					Environment::Hash,
					Environment::Number,
					Environment::Signature,
					Environment::Id,
				>,
				Environment::Error,
			>,
		> + Unpin,
	GlobalOutgoing: Sink<
			GlobalCommunicationOutgoing<
				Environment::Hash,
				Environment::Number,
				Environment::Signature,
				Environment::Id,
			>,
			Error = Environment::Error,
		> + Unpin,
{
	pub async fn new(
		environment: Environment,
		voters: VoterSet<Environment::Id>,
		global_communication: (GlobalIncoming, GlobalOutgoing),
		last_round_number: u64,
		last_round_votes: Vec<
			SignedMessage<
				Environment::Hash,
				Environment::Number,
				Environment::Signature,
				Environment::Id,
			>,
		>,
		last_round_base: (Environment::Hash, Environment::Number),
		best_finalized: (Environment::Hash, Environment::Number),
	) -> Voter<Environment, GlobalIncoming, GlobalOutgoing> {
		let last_round_state = RoundState::genesis(last_round_base.clone());
		let (global_incoming, global_outgoing) = global_communication;

		let (from_background_round_commits_sender, from_background_round_commits_receiver) =
			mpsc::channel(4);

		let background_rounds = FuturesUnordered::new();

		let background_round = BackgroundRound::restore(
			environment.clone(),
			voters.clone(),
			last_round_number,
			last_round_base,
			last_round_votes,
			from_background_round_commits_sender.clone(),
		)
		.await;

		let (background_round_state_updates_receiver, background_round_handle) =
			if let Some(background_round) = background_round {
				let (round, receiver, handle) = background_round.start();
				background_rounds.push(round.boxed());
				(receiver, Some(handle))
			} else {
				// FIXME: add comment about this edge case
				let (_, receiver) = mpsc::channel(0);
				(receiver, None)
			};

		let voting_round = VotingRound::new(
			environment.clone(),
			voters.clone(),
			last_round_number + 1,
			// TODO: use finalized from previous round state?
			best_finalized.clone(),
			last_round_state,
		)
		.await;

		let (voting_round, voting_round_handle) =
			voting_round.start(background_round_state_updates_receiver);

		Voter {
			voters,
			environment,
			global_incoming: global_incoming.fuse(),
			global_outgoing,
			best_finalized_in_rounds: best_finalized.clone(),
			best_finalized,
			current_round_number: last_round_number + 1,
			voting_round: voting_round.boxed().fuse(),
			voting_round_handle,
			background_rounds,
			background_round_handle,
			from_background_round_commits_receiver,
			from_background_round_commits_sender,
			pending_report_voter_state_requests: FuturesUnordered::new(),
		}
	}

	async fn handle_completable_round(
		&mut self,
		completable_round: CompletableRound<Environment>,
	) -> Result<(), Environment::Error> {
		let completable_round_number = completable_round.round.number();
		let completable_round_state = completable_round.round.state();
		// FIXME: deal with unwrap
		let completable_round_finalized = completable_round_state.finalized.clone().unwrap();

		debug!(
			target: LOG_TARGET,
			"completed voting round, finalized: {:?}", completable_round_finalized
		);

		if completable_round_finalized.1 > self.best_finalized.1 {
			self.environment
				.finalize_block(
					completable_round_finalized.0.clone(),
					completable_round_finalized.1,
					completable_round_number,
					Commit {
						target_hash: completable_round_finalized.0.clone(),
						target_number: completable_round_finalized.1,
						precommits: completable_round
							.round
							.finalizing_precommits(&self.environment)
							.expect(
								"always returns none if something was finalized; \
								 this is checked above; qed",
							)
							.collect(),
					},
				)
				.await?;

			self.best_finalized = completable_round_finalized.clone();
		}

		self.environment
			.completed(
				completable_round.round.number(),
				completable_round.round.state(),
				completable_round.round.base(),
				completable_round.round.historical_votes(),
			)
			.await?;

		let background_round = BackgroundRound::new(
			self.environment.clone(),
			completable_round.incoming,
			completable_round.round,
			self.from_background_round_commits_sender.clone(),
		)
		.await;

		let (background_round, background_round_state_updates_receiver, background_round_handle) =
			background_round.start();

		self.background_rounds.push(background_round.boxed());
		self.background_round_handle = Some(background_round_handle);

		let voting_round = VotingRound::new(
			self.environment.clone(),
			self.voters.clone(),
			completable_round_number + 1,
			completable_round_finalized,
			completable_round_state,
		)
		.await;

		let (voting_round, voting_round_handle) =
			voting_round.start(background_round_state_updates_receiver);

		self.voting_round = voting_round.boxed().fuse();
		self.voting_round_handle = voting_round_handle;

		self.current_round_number = completable_round_number + 1;

		Ok(())
	}

	async fn handle_concluded_round(
		&mut self,
		concluded_round: ConcludedRound<Environment>,
	) -> Result<(), Environment::Error> {
		self.environment
			.concluded(
				concluded_round.round.number(),
				concluded_round.round.state(),
				concluded_round.round.base(),
				concluded_round.round.historical_votes(),
			)
			.await?;

		Ok(())
	}

	async fn handle_background_round_commit(
		&mut self,
		background_round_commit: BackgroundRoundCommit<
			Environment::Hash,
			Environment::Number,
			Environment::Id,
			Environment::Signature,
		>,
	) -> Result<(), Environment::Error> {
		if background_round_commit.broadcast {
			// FIXME: deal with error
			let _ = self
				.global_outgoing
				.send(GlobalCommunicationOutgoing::Commit(
					background_round_commit.round_number,
					background_round_commit.commit.clone(),
				))
				.await;
		}

		if background_round_commit.commit.target_number > self.best_finalized.1 {
			let new_best_finalized = (
				background_round_commit.commit.target_hash.clone(),
				background_round_commit.commit.target_number,
			);

			self.environment
				.finalize_block(
					background_round_commit.commit.target_hash.clone(),
					background_round_commit.commit.target_number,
					background_round_commit.round_number,
					background_round_commit.commit,
				)
				.await?;

			// FIXME: also update self.best_finalized_in_rounds
			self.best_finalized = new_best_finalized;
		}

		Ok(())
	}

	async fn handle_incoming_global_message(
		&mut self,
		message: GlobalCommunicationIncoming<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
	) -> Result<(), Environment::Error> {
		match message {
			GlobalCommunicationIncoming::Commit(commit_round_number, commit, callback) =>
				self.handle_incoming_commit_message(commit_round_number, commit.into(), callback)
					.await,
			GlobalCommunicationIncoming::CatchUp(catch_up, callback) =>
				self.handle_incoming_catch_up_message(catch_up, callback).await,
		}
	}

	async fn handle_incoming_catch_up_message(
		&mut self,
		catch_up: CatchUp<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
		mut callback: Callback<CatchUpProcessingOutcome>,
	) -> Result<(), Environment::Error> {
		let round = if let Some(round) =
			validate_catch_up(catch_up, &self.environment, &self.voters, self.current_round_number)
		{
			round
		} else {
			callback.run(CatchUpProcessingOutcome::Bad);
			return Ok(())
		};

		let round_data = self.environment.round_data(round.number()).await;

		let round_number = round.number();
		let round_state = round.state();
		// FIXME deal with unwrap
		let round_state_finalized = round.state().finalized.unwrap();

		self.environment
			.completed(round.number(), round.state(), round.base(), round.historical_votes())
			.await?;

		// FIXME: clean this up
		if round_state_finalized.1 > self.best_finalized.1 {
			let precommits = round
				.finalizing_precommits(&self.environment)
				.expect(
					"always returns none if something was finalized; \
								this is checked above; qed",
				)
				.collect();

			let commit = Commit {
				target_hash: round_state_finalized.0.clone(),
				target_number: round_state_finalized.1,
				precommits,
			};

			// FIXME: finalize block
			self.environment
				.finalize_block(
					commit.target_hash.clone(),
					commit.target_number,
					round_number,
					commit,
				)
				.await?;

			self.best_finalized = round_state_finalized.clone();
		}

		let background_round = BackgroundRound::new(
			self.environment.clone(),
			round_data.incoming.fuse(),
			round,
			self.from_background_round_commits_sender.clone(),
		)
		.await;

		let (background_round, background_round_state_updates_receiver, background_round_handle) =
			background_round.start();

		self.background_rounds.push(background_round.boxed());
		self.background_round_handle = Some(background_round_handle);

		let voting_round = VotingRound::new(
			self.environment.clone(),
			self.voters.clone(),
			round_number + 1,
			// FIXME: use global value for finalized in rounds
			round_state_finalized,
			round_state,
		)
		.await;

		let (voting_round, voting_round_handle) =
			voting_round.start(background_round_state_updates_receiver);

		self.voting_round = voting_round.boxed().fuse();
		self.voting_round_handle = voting_round_handle;

		self.current_round_number = round_number + 1;

		callback.run(CatchUpProcessingOutcome::Good);

		Ok(())
	}

	async fn handle_incoming_commit_message(
		&mut self,
		commit_round_number: u64,
		commit: Commit<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
		mut callback: Callback<CommitProcessingOutcome>,
	) -> Result<(), Environment::Error> {
		match self.current_round_number.checked_sub(1) {
			Some(background_round_number) if background_round_number == commit_round_number => {
				if let Some(handle) = self.background_round_handle.as_mut() {
					handle.send_commit(commit, callback).await?;
				}

				return Ok(())
			},
			_ => {},
		}

		let commit_validation_result = validate_commit(&commit, &self.voters, &self.environment)?;

		if commit_validation_result.is_valid() {
			if commit.target_number > self.best_finalized.1 {
				self.environment
					.finalize_block(
						commit.target_hash.clone(),
						commit.target_number,
						commit_round_number,
						commit.clone(),
					)
					.await?;

				self.best_finalized = (commit.target_hash, commit.target_number);
			}
		}

		callback.run(commit_validation_result.into());

		Ok(())
	}

	async fn handle_report_voter_state_request(
		&mut self,
		response_sender: oneshot::Sender<report::VoterState<Environment::Id>>,
	) -> Result<(), Environment::Error> {
		let current_round_number = self.current_round_number;
		let mut voting_round_handle = self.voting_round_handle.clone();
		let mut background_round_handle = self.background_round_handle.clone();

		// NOTE: we need to do this out of the voter main loop, otherwise voting
		// round and background round won't be polled, which means the requests
		// below will go unanswered.
		let work = async move {
			let voting_round_state = voting_round_handle.report_round_state().await?;

			let mut background_rounds = std::collections::BTreeMap::new();
			if let Some(handle) = background_round_handle.as_mut() {
				let background_round_state = handle.report_round_state().await?;
				// FIXME: check underflow
				background_rounds.insert(current_round_number - 1, background_round_state);
			}

			let voter_state = report::VoterState {
				best_round: (current_round_number, voting_round_state),
				background_rounds,
			};

			// FIXME: deal with error
			let _ = response_sender.send(voter_state);

			Ok(())
		};

		self.pending_report_voter_state_requests.push(work.boxed());

		Ok(())
	}

	pub async fn run(
		&mut self,
		mut report_voter_state_receiver: mpsc::Receiver<
			oneshot::Sender<report::VoterState<Environment::Id>>,
		>,
	) -> Result<(), Environment::Error> {
		loop {
			select_biased! {
				completable_round = &mut self.voting_round => {
					self.handle_completable_round(completable_round?).await?;
				},
				concluded_round = self.background_rounds.select_next_some() => {
					self.handle_concluded_round(concluded_round?).await?;
				},
				background_round_commit = self.from_background_round_commits_receiver.select_next_some() => {
					self.handle_background_round_commit(background_round_commit).await?;
				},
				global_message = self.global_incoming.select_next_some() => {
					self.handle_incoming_global_message(global_message?).await?;
				},
				response_sender = report_voter_state_receiver.select_next_some() => {
					self.handle_report_voter_state_request(response_sender).await?;
				},
				_ = self.pending_report_voter_state_requests.select_next_some() => {
					// noop, we just need to keep polling this future to make
					// background work progress
				},
			}
		}
	}

	pub fn start(
		mut self,
	) -> (impl Future<Output = Result<(), Environment::Error>>, VoterHandle<Environment>) {
		let (report_voter_state_sender, report_voter_state_receiver) = mpsc::channel(4);

		(
			async move { self.run(report_voter_state_receiver).await },
			VoterHandle { report_voter_state_sender },
		)
	}
}

/// Contains a number of data transfer objects for reporting data to the outside world.
pub mod report {
	use std::collections::{BTreeMap, BTreeSet};

	use crate::weights::{VoteWeight, VoterWeight};

	/// Basic data struct for the state of a round.
	#[derive(PartialEq, Eq, Clone)]
	#[cfg_attr(test, derive(Debug))]
	pub struct RoundState<Id> {
		/// Total weight of all votes.
		pub total_weight: VoterWeight,
		/// The threshold voter weight.
		pub threshold_weight: VoterWeight,

		/// Current weight of the prevotes.
		pub prevote_current_weight: VoteWeight,
		/// The identities of nodes that have cast prevotes so far.
		pub prevote_ids: BTreeSet<Id>,

		/// Current weight of the precommits.
		pub precommit_current_weight: VoteWeight,
		/// The identities of nodes that have cast precommits so far.
		pub precommit_ids: BTreeSet<Id>,
	}

	/// Basic data struct for the current state of the voter in a form suitable
	/// for passing on to other systems.
	#[derive(PartialEq, Eq)]
	#[cfg_attr(test, derive(Debug))]
	pub struct VoterState<Id> {
		/// Voting rounds running in the background.
		pub background_rounds: BTreeMap<u64, RoundState<Id>>,
		/// The current best voting round.
		pub best_round: (u64, RoundState<Id>),
	}
}

fn validate_catch_up<Environment>(
	catch_up: CatchUp<
		Environment::Hash,
		Environment::Number,
		Environment::Signature,
		Environment::Id,
	>,
	env: &Environment,
	voters: &VoterSet<Environment::Id>,
	best_round_number: u64,
) -> Option<Round<Environment::Id, Environment::Hash, Environment::Number, Environment::Signature>>
where
	Environment: EnvironmentT,
{
	if catch_up.round_number <= best_round_number {
		trace!(target: LOG_TARGET, "Ignoring because best round number is {}", best_round_number);

		// FIXME: should be outcome::useless?
		return None
	}

	// check threshold support in prevotes and precommits.
	{
		let mut map = std::collections::BTreeMap::new();

		for prevote in &catch_up.prevotes {
			if !voters.contains(&prevote.id) {
				trace!(
					target: LOG_TARGET,
					"Ignoring invalid catch up, invalid voter: {:?}",
					prevote.id,
				);

				return None
			}

			map.entry(prevote.id.clone()).or_insert((false, false)).0 = true;
		}

		for precommit in &catch_up.precommits {
			if !voters.contains(&precommit.id) {
				trace!(
					target: LOG_TARGET,
					"Ignoring invalid catch up, invalid voter: {:?}",
					precommit.id,
				);

				return None
			}

			map.entry(precommit.id.clone()).or_insert((false, false)).1 = true;
		}

		let (pv, pc) = map.into_iter().fold(
			(VoteWeight(0), VoteWeight(0)),
			|(mut pv, mut pc), (id, (prevoted, precommitted))| {
				if let Some(v) = voters.get(&id) {
					if prevoted {
						pv = pv + v.weight();
					}

					if precommitted {
						pc = pc + v.weight();
					}
				}

				(pv, pc)
			},
		);

		let threshold = voters.threshold();
		if pv < threshold || pc < threshold {
			trace!(target: LOG_TARGET, "Ignoring invalid catch up, missing voter threshold");

			return None
		}
	}

	let mut round = Round::new(RoundParams {
		round_number: catch_up.round_number,
		voters: voters.clone(),
		base: (catch_up.base_hash.clone(), catch_up.base_number),
	});

	// import prevotes first.
	for SignedPrevote { prevote, id, signature } in catch_up.prevotes {
		match round.import_prevote(env, prevote, id, signature) {
			Ok(_) => {},
			Err(e) => {
				trace!(
					target: LOG_TARGET,
					"Ignoring invalid catch up, error importing prevote: {:?}",
					e,
				);

				return None
			},
		}
	}

	// then precommits.
	for SignedPrecommit { precommit, id, signature } in catch_up.precommits {
		match round.import_precommit(env, precommit, id, signature) {
			Ok(_) => {},
			Err(e) => {
				trace!(
					target: LOG_TARGET,
					"Ignoring invalid catch up, error importing precommit: {:?}",
					e,
				);

				return None
			},
		}
	}

	let state = round.state();
	if !state.completable {
		return None
	}

	Some(round)
}
