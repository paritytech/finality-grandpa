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
use futures::{
	channel::mpsc, future::Fuse, pin_mut, select_biased, Future, FutureExt, Sink, SinkExt, Stream,
	StreamExt,
};
use log::{debug, trace};

use crate::{
	round::{Round, RoundParams, State as RoundState},
	validate_commit,
	voter::{
		background_round::BackgroundRound,
		voting_round::{CompletableRound, VotingRound},
	},
	weights::VoteWeight,
	BlockNumberOps, CatchUp, Chain, Commit, CommitValidationResult, CompactCommit, Error, Message,
	SignedMessage, SignedPrecommit, SignedPrevote, VoterSet,
};

use self::{background_round::BackgroundRoundCommit, Environment as EnvironmentT};

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

impl<Hash, Number> From<CommitValidationResult<Hash, Number>> for CommitProcessingOutcome {
	fn from(result: CommitValidationResult<Hash, Number>) -> Self {
		if result.ghost.is_some() {
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
pub trait Environment<Hash, Number>: Chain<Hash, Number> + Clone
where
	Hash: Eq,
{
	type Id: Clone + Debug + Ord;
	type Signature: Clone + Eq;
	type Error: From<Error>;
	// FIXME: is the unpin really needed?
	// FIXME: maybe it makes sense to make this a FusedFuture to avoid having to wrap it in Fuse<...>
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

pub async fn run<Hash, Number, Environment, GlobalIncoming, GlobalOutgoing>(
	environment: Environment,
	voters: VoterSet<Environment::Id>,
	global_communication: (GlobalIncoming, GlobalOutgoing),
	last_round_number: u64,
	last_round_votes: Vec<SignedMessage<Hash, Number, Environment::Signature, Environment::Id>>,
	last_round_base: (Hash, Number),
	best_finalized: (Hash, Number),
) -> Result<(), Environment::Error>
where
	Hash: Clone + Debug + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
	GlobalIncoming: Stream<
		Item = Result<
			GlobalCommunicationIncoming<Hash, Number, Environment::Signature, Environment::Id>,
			Environment::Error,
		>,
	>,
	GlobalOutgoing: Sink<
		GlobalCommunicationOutgoing<Hash, Number, Environment::Signature, Environment::Id>,
		Error = Environment::Error,
	>,
{
	let last_round_state = RoundState::genesis(last_round_base.clone());
	let (previous_round_state_updates_out, previous_round_state_updates_in) =
		futures::channel::mpsc::channel(4);

	let (global_incoming, global_outgoing) = global_communication;
	let global_incoming = global_incoming.fuse();
	pin_mut!(global_incoming, global_outgoing);

	let (mut background_round_commits_tx, background_round_commit_incoming) =
		futures::channel::mpsc::channel(4);

	let (background_round_commit_outgoing, mut background_round_commits_rx) =
		futures::channel::mpsc::channel(4);

	let background_round = BackgroundRound::restore(
		environment.clone(),
		voters.clone(),
		last_round_number,
		last_round_base,
		last_round_votes,
		previous_round_state_updates_out,
		background_round_commit_incoming,
		background_round_commit_outgoing.clone(),
	)
	.await;

	let mut best_finalized_number = best_finalized.1.clone();

	let voting_round = VotingRound::new(
		environment.clone(),
		voters.clone(),
		last_round_number + 1,
		// TODO: use finalized from previous round state?
		best_finalized,
		last_round_state,
		previous_round_state_updates_in,
	)
	.await;

	// FIXME: create VoterState struct,
	// easier to pass around to handlers

	let mut current_round_number = voting_round.round_number();
	let background_round = background_round
		.map(|round| round.run().fuse())
		.unwrap_or_else(Fuse::terminated);
	let voting_round = voting_round.run().fuse();

	pin_mut!(background_round, voting_round);

	// track best finalized number in rounds and global finalized (through global commits)
	// only use finalized in rounds for base
	// use global finalized to avoid calling Environment::finalize_block
	loop {
		select_biased! {
			completable_round = voting_round => {
				let (new_voting_round, (new_background_round, new_background_round_commits)) =
					handle_completable_round(
						&environment,
						&mut best_finalized_number,
						completable_round?,
						background_round_commit_outgoing.clone(),
					)
					.await?;

				current_round_number = new_voting_round.round_number();
				background_round_commits_tx = new_background_round_commits;

				// we need to update futures we're polling on this select
				// loop to point to the new rounds
				background_round.set(new_background_round.run().fuse());
				voting_round.set(new_voting_round.run().fuse());
			},
			res = background_round => {
				handle_concluded_round(res).await?;
			},
			background_round_commit = background_round_commits_rx.select_next_some() => {
				handle_background_round_commit(
					&environment,
					&mut best_finalized_number,
					background_round_commit,
					global_outgoing.as_mut(),
				)
				.await?;
			},
			global_message = global_incoming.select_next_some() => {
				match global_message? {
					GlobalCommunicationIncoming::Commit(commit_round_number, commit, callback) => {
						handle_incoming_commit_message(
							&environment,
							&mut best_finalized_number,
							&voters,
							current_round_number,
							commit_round_number,
							commit.into(),
							&mut background_round_commits_tx,
							callback,
						)
						.await?;
					},
					GlobalCommunicationIncoming::CatchUp(catch_up, callback) => {
						if let Some((new_voting_round, (new_background_round, new_background_round_commits))) =
							handle_incoming_catch_up_message(
								&environment,
								&mut best_finalized_number,
								&voters,
								current_round_number,
								catch_up,
								callback,
								background_round_commit_outgoing.clone(),
							)
							.await?
						{
							current_round_number = new_voting_round.round_number();
							background_round_commits_tx = new_background_round_commits;

							// we need to update futures we're polling on this select
							// loop to point to the new rounds
							background_round.set(new_background_round.run().fuse());
							voting_round.set(new_voting_round.run().fuse());
						}
					},
				}
			},
		}
	}
}

async fn handle_background_round_commit<Hash, Number, Environment, GlobalOutgoing>(
	environment: &Environment,
	best_finalized_number: &mut Number,
	background_round_commit: BackgroundRoundCommit<
		Hash,
		Number,
		Environment::Id,
		Environment::Signature,
	>,
	mut global_outgoing: Pin<&mut GlobalOutgoing>,
) -> Result<(), Environment::Error>
where
	Hash: Clone + Eq,
	Number: Copy + Ord,
	Environment: EnvironmentT<Hash, Number>,
	GlobalOutgoing: Sink<
		GlobalCommunicationOutgoing<Hash, Number, Environment::Signature, Environment::Id>,
		Error = Environment::Error,
	>,
{
	if background_round_commit.broadcast {
		// FIXME: deal with error
		let _ = global_outgoing
			.send(GlobalCommunicationOutgoing::Commit(
				background_round_commit.round_number,
				background_round_commit.commit.clone(),
			))
			.await;
	}

	if background_round_commit.commit.target_number > *best_finalized_number {
		let new_best_finalized_number = background_round_commit.commit.target_number;

		environment
			.finalize_block(
				background_round_commit.commit.target_hash.clone(),
				background_round_commit.commit.target_number,
				background_round_commit.round_number,
				background_round_commit.commit,
			)
			.await?;

		*best_finalized_number = new_best_finalized_number;
	}

	Ok(())
}

async fn handle_completable_round<Hash, Number, Environment>(
	environment: &Environment,
	best_finalized_number: &mut Number,
	completable_round: CompletableRound<Hash, Number, Environment>,
	background_round_commits: mpsc::Sender<
		BackgroundRoundCommit<Hash, Number, Environment::Id, Environment::Signature>,
	>,
) -> Result<
	(
		VotingRound<Hash, Number, Environment>,
		(
			BackgroundRound<Hash, Number, Environment>,
			mpsc::Sender<(
				Commit<Hash, Number, Environment::Signature, Environment::Id>,
				Callback<CommitProcessingOutcome>,
			)>,
		),
	),
	Environment::Error,
>
where
	Hash: Clone + Debug + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	log::trace!("here2");

	let completable_round_number = completable_round.round.number();
	let completable_round_state = completable_round.round.state();
	// FIXME: deal with unwrap
	let completable_round_finalized = completable_round_state.finalized.clone().unwrap();
	let voters = completable_round.round.voters().clone();

	debug!("completed voting round, finalized: {:?}", completable_round_finalized);

	if completable_round_finalized.1 > *best_finalized_number {
		environment.finalize_block(
			completable_round_finalized.0.clone(),
			completable_round_finalized.1,
			completable_round_number,
			Commit {
				target_hash: completable_round_finalized.0.clone(),
				target_number: completable_round_finalized.1,
				precommits: completable_round.round.finalizing_precommits(environment)
					.expect("always returns none if something was finalized; this is checked above; qed")
					.collect(),
			},
		).await?;

		*best_finalized_number = completable_round_finalized.1;
	}

	let (previous_round_state_updates_out, previous_round_state_updates_in) =
		futures::channel::mpsc::channel(4);

	let (background_round_commits_out, background_round_commits_in) =
		futures::channel::mpsc::channel(4);

	let background_round = BackgroundRound::new(
		environment.clone(),
		completable_round.incoming,
		completable_round.round,
		previous_round_state_updates_out,
		background_round_commits_in,
		background_round_commits,
	)
	.await;

	let voting_round = VotingRound::new(
		environment.clone(),
		voters.clone(),
		completable_round_number + 1,
		completable_round_finalized,
		completable_round_state,
		previous_round_state_updates_in,
	)
	.await;

	Ok((voting_round, (background_round, background_round_commits_out)))
}

async fn handle_concluded_round<Error>(result: Result<(), Error>) -> Result<(), Error> {
	result
}

async fn handle_incoming_commit_message<Hash, Number, Environment>(
	environment: &Environment,
	best_finalized_number: &mut Number,
	voters: &VoterSet<Environment::Id>,
	current_round_number: u64,
	commit_round_number: u64,
	commit: Commit<Hash, Number, Environment::Signature, Environment::Id>,
	background_round_commits: &mut mpsc::Sender<(
		Commit<Hash, Number, Environment::Signature, Environment::Id>,
		Callback<CommitProcessingOutcome>,
	)>,
	mut callback: Callback<CommitProcessingOutcome>,
) -> Result<(), Environment::Error>
where
	Hash: Clone + Debug + Eq + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	match current_round_number.checked_sub(1) {
		Some(background_round_number) if background_round_number == commit_round_number => {
			// FIXME: deal with error due to dropped channel
			let _ = background_round_commits.send((commit, callback)).await;
			return Ok(())
		},
		_ => {},
	}

	let commit_validation_result = validate_commit(&commit, voters, environment)?;

	if let Some((ref finalized_hash, finalized_number)) = commit_validation_result.ghost {
		if finalized_number > *best_finalized_number {
			environment
				.finalize_block(
					finalized_hash.clone(),
					finalized_number,
					commit_round_number,
					commit,
				)
				.await?;

			*best_finalized_number = finalized_number;
		}
	}

	callback.run(commit_validation_result.into());

	Ok(())
}

async fn handle_incoming_catch_up_message<Hash, Number, Environment>(
	environment: &Environment,
	best_finalized_number: &mut Number,
	voters: &VoterSet<Environment::Id>,
	current_round_number: u64,
	catch_up: CatchUp<Hash, Number, Environment::Signature, Environment::Id>,
	mut callback: Callback<CatchUpProcessingOutcome>,
	background_round_commits: mpsc::Sender<
		BackgroundRoundCommit<Hash, Number, Environment::Id, Environment::Signature>,
	>,
) -> Result<
	Option<(
		VotingRound<Hash, Number, Environment>,
		(
			BackgroundRound<Hash, Number, Environment>,
			mpsc::Sender<(
				Commit<Hash, Number, Environment::Signature, Environment::Id>,
				Callback<CommitProcessingOutcome>,
			)>,
		),
	)>,
	Environment::Error,
>
where
	Hash: Clone + Debug + Eq + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	let round = if let Some(round) =
		validate_catch_up(catch_up, environment, voters, current_round_number)
	{
		round
	} else {
		callback.run(CatchUpProcessingOutcome::Bad);
		return Ok(None)
	};

	let round_data = environment.round_data(round.number()).await;

	let (previous_round_state_updates_out, previous_round_state_updates_in) =
		futures::channel::mpsc::channel(4);

	let (background_round_commits_out, background_round_commits_in) =
		futures::channel::mpsc::channel(4);

	let round_number = round.number();
	let round_state = round.state();
	// FIXME
	let round_state_finalized = round.state().finalized.clone().unwrap();

	let background_round = BackgroundRound::new(
		environment.clone(),
		round_data.incoming.fuse(),
		round,
		previous_round_state_updates_out,
		background_round_commits_in,
		background_round_commits,
	)
	.await;

	let voting_round = VotingRound::new(
		environment.clone(),
		voters.clone(),
		round_number + 1,
		// FIXME: use global value for finalized in rounds
		round_state_finalized.clone(),
		round_state,
		previous_round_state_updates_in,
	)
	.await;

	if round_state_finalized.1 > *best_finalized_number {
		let new_best_finalized_number = round_state_finalized.1;

		// environment
		// 	.finalize_block(
		// 		background_round_commit.commit.target_hash.clone(),
		// 		background_round_commit.commit.target_number,
		// 		background_round_commit.round_number,
		// 		background_round_commit.commit,
		// 	)
		// 	.await?;

		*best_finalized_number = new_best_finalized_number;
	}

	// FIXME: run Environment::completed hook

	callback.run(CatchUpProcessingOutcome::Good);

	Ok(Some((voting_round, (background_round, background_round_commits_out))))
}

fn validate_catch_up<Hash, Number, Environment>(
	catch_up: CatchUp<Hash, Number, Environment::Signature, Environment::Id>,
	env: &Environment,
	voters: &VoterSet<Environment::Id>,
	best_round_number: u64,
) -> Option<Round<Environment::Id, Hash, Number, Environment::Signature>>
where
	Hash: Clone + Debug + Eq + Ord,
	Number: BlockNumberOps,
	Environment: EnvironmentT<Hash, Number>,
{
	if catch_up.round_number <= best_round_number {
		trace!(target: "afg", "Ignoring because best round number is {}",
			   best_round_number);

		// FIXME: should be outcome::useless?
		return None
	}

	// check threshold support in prevotes and precommits.
	{
		let mut map = std::collections::BTreeMap::new();

		for prevote in &catch_up.prevotes {
			if !voters.contains(&prevote.id) {
				trace!(target: "afg",
					   "Ignoring invalid catch up, invalid voter: {:?}",
					   prevote.id,
				);

				return None
			}

			map.entry(prevote.id.clone()).or_insert((false, false)).0 = true;
		}

		for precommit in &catch_up.precommits {
			if !voters.contains(&precommit.id) {
				trace!(target: "afg",
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
			trace!(target: "afg",
				   "Ignoring invalid catch up, missing voter threshold"
			);

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
				trace!(target: "afg",
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
				trace!(target: "afg",
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
