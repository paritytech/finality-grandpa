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

use futures::prelude::*;
use futures::sync::mpsc::{self, UnboundedReceiver};
#[cfg(feature = "std")]
use log::trace;
#[cfg(feature = "derive-codec")]
use parity_codec::{Encode, Decode};

use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::Arc;

use crate::round::State as RoundState;
use crate::{
	CatchUp, Chain, Commit, CompactCommit, Equivocation, Message, Prevote, Precommit,
	PrimaryPropose, SignedMessage, BlockNumberOps, validate_commit, CommitValidationResult,
	HistoricalVotes,
};
use crate::voter_set::VoterSet;
use past_rounds::PastRounds;
use voting_round::{VotingRound, State as VotingRoundState};

mod past_rounds;
mod voting_round;

/// Necessary environment for a voter.
///
/// This encapsulates the database and networking layers of the chain.
pub trait Environment<H: Eq, N: BlockNumberOps>: Chain<H, N> {
	type Timer: Future<Item=(),Error=Self::Error>;
	type Id: Hash + Clone + Eq + ::std::fmt::Debug;
	type Signature: Eq + Clone;
	type In: Stream<Item=SignedMessage<H, N, Self::Signature, Self::Id>, Error=Self::Error>;
	type Out: Sink<SinkItem=Message<H, N>, SinkError=Self::Error>;
	type Error: From<crate::Error> + ::std::error::Error;

	/// Produce data necessary to start a round of voting.
	///
	/// The input stream should provide messages which correspond to known blocks
	/// only.
	///
	/// The voting logic will push unsigned messages over-eagerly into the
	/// output stream. It is the job of this stream to determine if those messages
	/// should be sent (for example, if the process actually controls a permissioned key)
	/// and then to sign the message, multicast it to peers, and schedule it to be
	/// returned by the `In` stream.
	///
	/// This allows the voting logic to maintain the invariant that only incoming messages
	/// may alter the state, and the logic remains the same regardless of whether a node
	/// is a regular voter, the proposer, or simply an observer.
	///
	/// Furthermore, this means that actual logic of creating and verifying
	/// signatures is flexible and can be maintained outside this crate.
	fn round_data(&self, round: u64) -> RoundData<
		Self::Id,
		Self::Timer,
		Self::In,
		Self::Out,
	>;

	/// Return a timer that will be used to delay the broadcast of a commit
	/// message. This delay should not be static to minimize the amount of
	/// commit messages that are sent (e.g. random value in [0, 1] seconds).
	fn round_commit_timer(&self) -> Self::Timer;

	/// Note that we've done a primary proposal in the given round.
	fn proposed(&self, round: u64, propose: PrimaryPropose<H, N>) -> Result<(), Self::Error>;

	/// Note that we have prevoted in the given round.
	fn prevoted(&self, round: u64, prevote: Prevote<H, N>) -> Result<(), Self::Error>;

	/// Note that we have precommitted in the given round.
	fn precommitted(&self, round: u64, precommit: Precommit<H, N>) -> Result<(), Self::Error>;

	/// Note that a round was completed. This is called when a round has been
	/// voted in. Should return an error when something fatal occurs.
	fn completed(
		&self,
		round: u64,
		state: RoundState<H, N>,
		base: (H, N),
		votes: &HistoricalVotes<H, N, Self::Signature, Self::Id>,
	) -> Result<(), Self::Error>;

	/// Called when a block should be finalized.
	// TODO: make this a future that resolves when it's e.g. written to disk?
	fn finalize_block(&self, hash: H, number: N, round: u64, commit: Commit<H, N, Self::Signature, Self::Id>) -> Result<(), Self::Error>;

	// Note that an equivocation in prevotes has occurred.s
	fn prevote_equivocation(&self, round: u64, equivocation: Equivocation<Self::Id, Prevote<H, N>, Self::Signature>);
	// Note that an equivocation in precommits has occurred.
	fn precommit_equivocation(&self, round: u64, equivocation: Equivocation<Self::Id, Precommit<H, N>, Self::Signature>);
}

/// Communication between nodes that is not round-localized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommunicationOut<H, N, S, Id> {
	/// A commit message.
	Commit(u64, Commit<H, N, S, Id>),
}

/// The outcome of processing a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitProcessingOutcome {
	/// It was beneficial to process this commit.
	Good(GoodCommit),
	/// It wasn't beneficial to process this commit. We wasted resources.
	Bad(BadCommit),
}

/// The result of processing for a good commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoodCommit {
	_priv: (), // lets us add stuff without breaking API.
}

impl GoodCommit {
	pub(crate) fn new() -> Self {
		GoodCommit { _priv: () }
	}
}

/// The result of processing for a bad commit
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadCommit {
	_priv: (), // lets us add stuff without breaking API.
	num_precommits: usize,
	num_duplicated_precommits: usize,
	num_equivocations: usize,
	num_invalid_voters: usize,
}

impl BadCommit {
	/// Get the number of precommits
	pub fn num_precommits(&self) -> usize {
		self.num_precommits
	}

	/// Get the number of duplicated precommits
	pub fn num_duplicated(&self) -> usize {
		self.num_duplicated_precommits
	}

	/// Get the number of equivocations in the precommits
	pub fn num_equivocations(&self) -> usize {
		self.num_equivocations
	}

	/// Get the number of invalid voters in the precommits
	pub fn num_invalid_voters(&self) -> usize {
		self.num_invalid_voters
	}
}

impl<H, N> From<CommitValidationResult<H, N>> for BadCommit {
	fn from(r: CommitValidationResult<H, N>) -> Self {
		BadCommit {
			num_precommits: r.num_precommits,
			num_duplicated_precommits: r.num_duplicated_precommits,
			num_equivocations: r.num_equivocations,
			num_invalid_voters: r.num_invalid_voters,
			_priv: (),
		}
	}
}

/// The outcome of processing a catch up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatchUpProcessingOutcome {
	/// It was beneficial to process this catch up.
	Good(GoodCatchUp),
	/// It wasn't beneficial to process this catch up, it is invalid and we
	/// wasted resources.
	Bad(BadCatchUp),
	/// The catch up wasn't processed because it is useless, e.g. it is for a
	/// round lower than we're currently in.
	Useless,
}

/// The result of processing for a good catch up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoodCatchUp {
	_priv: (), // lets us add stuff without breaking API.
}

impl GoodCatchUp {
	pub(crate) fn new() -> Self {
		GoodCatchUp { _priv: () }
	}
}

/// The result of processing for a bad catch up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadCatchUp {
	_priv: (), // lets us add stuff without breaking API.
}

impl BadCatchUp {
	pub(crate) fn new() -> Self {
		BadCatchUp { _priv: () }
	}
}

/// Callback used to pass information about the outcome of importing a given
/// message (e.g. vote, commit, catch up). Useful to propagate data to the
/// network after making sure the import is successful.
pub enum Callback<O> {
	/// Default value.
	Blank,
	/// Callback to execute given a processing outcome.
	Work(Box<FnMut(O) + Send>),
}

#[cfg(test)]
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

/// Communication between nodes that is not round-localized.
#[cfg_attr(test, derive(Clone))]
pub enum CommunicationIn<H, N, S, Id> {
	/// A commit message.
	Commit(u64, CompactCommit<H, N, S, Id>, Callback<CommitProcessingOutcome>),
	/// A catch up message.
	CatchUp(CatchUp<H, N, S, Id>, Callback<CatchUpProcessingOutcome>),
}

/// Data necessary to participate in a round.
pub struct RoundData<Id, Timer, Input, Output> {
	/// Local voter id (if any.)
	pub voter_id: Option<Id>,
	/// Timer before prevotes can be cast. This should be Start + 2T
	/// where T is the gossip time estimate.
	pub prevote_timer: Timer,
	/// Timer before precommits can be cast. This should be Start + 4T
	pub precommit_timer: Timer,
	/// Incoming messages.
	pub incoming: Input,
	/// Outgoing messages.
	pub outgoing: Output,
}

struct Buffered<S: Sink> {
	inner: S,
	buffer: VecDeque<S::SinkItem>,
}

impl<S: Sink> Buffered<S> {
	fn new(inner: S) -> Buffered<S> {
		Buffered {
			buffer: VecDeque::new(),
			inner
		}
	}

	// push an item into the buffered sink.
	// the sink _must_ be driven to completion with `poll` afterwards.
	fn push(&mut self, item: S::SinkItem) {
		self.buffer.push_back(item);
	}

	// returns ready when the sink and the buffer are completely flushed.
	fn poll(&mut self) -> Poll<(), S::SinkError> {
		let polled = self.schedule_all()?;

		match polled {
			Async::Ready(()) => self.inner.poll_complete(),
			Async::NotReady => {
				self.inner.poll_complete()?;
				Ok(Async::NotReady)
			}
		}
	}

	fn schedule_all(&mut self) -> Poll<(), S::SinkError> {
		while let Some(front) = self.buffer.pop_front() {
			match self.inner.start_send(front) {
				Ok(AsyncSink::Ready) => continue,
				Ok(AsyncSink::NotReady(front)) => {
					self.buffer.push_front(front);
					break;
				}
				Err(e) => return Err(e),
			}
		}

		if self.buffer.is_empty() {
			Ok(Async::Ready(()))
		} else {
			Ok(Async::NotReady)
		}
	}
}

/// A future that maintains and multiplexes between different rounds,
/// and caches votes.
///
/// This voter also implements the commit protocol.
/// The commit protocol allows a node to broadcast a message that finalizes a
/// given block and includes a set of precommits as proof.
///
/// - When a round is completable and we precommitted we start a commit timer
/// and start accepting commit messages;
/// - When we receive a commit message if it targets a block higher than what
/// we've finalized we validate it and import its precommits if valid;
/// - When our commit timer triggers we check if we've received any commit
/// message for a block equal to what we've finalized, if we haven't then we
/// broadcast a commit.
///
/// Additionally, we also listen to commit messages from rounds that aren't
/// currently running, we validate the commit and dispatch a finalization
/// notification (if any) to the environment.
pub struct Voter<H, N, E: Environment<H, N>, GlobalIn, GlobalOut> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
	GlobalIn: Stream<Item=CommunicationIn<H, N, E::Signature, E::Id>, Error=E::Error>,
	GlobalOut: Sink<SinkItem=CommunicationOut<H, N, E::Signature, E::Id>, SinkError=E::Error>,
{
	env: Arc<E>,
	voters: VoterSet<E::Id>,
	best_round: VotingRound<H, N, E>,
	past_rounds: PastRounds<H, N, E>,
	finalized_notifications: UnboundedReceiver<(H, N, u64, Commit<H, N, E::Signature, E::Id>)>,
	last_finalized_number: N,
	global_in: GlobalIn,
	global_out: Buffered<GlobalOut>,
	// the commit protocol might finalize further than the current round (if we're
	// behind), we keep track of last finalized in round so we don't violate any
	// assumptions from round-to-round.
	last_finalized_in_rounds: (H, N),
}

impl<H, N, E: Environment<H, N>, GlobalIn, GlobalOut> Voter<H, N, E, GlobalIn, GlobalOut> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
	GlobalIn: Stream<Item=CommunicationIn<H, N, E::Signature, E::Id>, Error=E::Error>,
	GlobalOut: Sink<SinkItem=CommunicationOut<H, N, E::Signature, E::Id>, SinkError=E::Error>,
{
	/// Create new `Voter` tracker with given round number and base block.
	///
	/// Provide data about the last completed round. If there is no
	/// known last completed round, the genesis state (round number 0),
	/// should be provided.
	///
	/// The input stream for commit messages should provide commits which
	/// correspond to known blocks only (including all its precommits). It
	/// is also responsible for validating the signature data in commit
	/// messages.
	pub fn new(
		env: Arc<E>,
		voters: VoterSet<E::Id>,
		global_comms: (GlobalIn, GlobalOut),
		last_round_number: u64,
		last_round_state: RoundState<H, N>,
		last_finalized: (H, N),
	) -> Self {
		let (finalized_sender, finalized_notifications) = mpsc::unbounded();
		let last_finalized_number = last_finalized.1.clone();
		let (_, last_round_state) = crate::bridge_state::bridge_state(last_round_state);

		let best_round = VotingRound::new(
			last_round_number + 1,
			voters.clone(),
			last_finalized.clone(),
			Some(last_round_state),
			finalized_sender,
			env.clone(),
		);

		let (global_in, global_out) = global_comms;

		// TODO: load last round (or more), re-process all votes from them,
		// and background until irrelevant

		Voter {
			env,
			voters,
			best_round,
			past_rounds: PastRounds::new(),
			finalized_notifications,
			last_finalized_number,
			last_finalized_in_rounds: last_finalized,
			global_in,
			global_out: Buffered::new(global_out),
		}
	}

	fn prune_background_rounds(&mut self) -> Result<(), E::Error> {
		// Do work on all background rounds, broadcasting any commits generated.
		while let Async::Ready(Some((number, commit))) = self.past_rounds.poll()? {
			self.global_out.push(CommunicationOut::Commit(number, commit));
		}

		while let Async::Ready(res) = self.finalized_notifications.poll()
			.expect("unbounded receivers do not have spurious errors; qed")
		{
			let (f_hash, f_num, round, commit) =
				res.expect("one sender always kept alive in self.best_round; qed");


			self.past_rounds.update_finalized(f_num);

			if self.set_last_finalized_number(f_num.clone()) {
				self.env.finalize_block(f_hash.clone(), f_num.clone(), round, commit)?;
			}

			if f_num > self.last_finalized_in_rounds.1 {
				self.last_finalized_in_rounds = (f_hash, f_num);
			}
		}

		Ok(())
	}

	/// Process all incoming messages from other nodes.
	///
	/// Commit messages are handled with extra care. If a commit message references
	/// a currently backgrounded round, we send it to that round so that when we commit
	/// on that round, our commit message will be informed by those that we've seen.
	///
	/// Otherwise, we will simply handle the commit and issue a finalization command
	/// to the environment.
	fn process_incoming(&mut self) -> Result<(), E::Error> {
		while let Async::Ready(Some(item)) = self.global_in.poll()? {
			match item {
				CommunicationIn::Commit(round_number, commit, mut process_commit_outcome) => {
					trace!(target: "afg", "Got commit for round_number {:?}: target_number: {:?}, target_hash: {:?}",
						round_number,
						commit.target_number,
						commit.target_hash,
					);

					let commit: Commit<_, _, _, _> = commit.into();

					// if the commit is for a background round dispatch to round committer.
					// that returns Some if there wasn't one.
					if let Some(commit) = self.past_rounds.import_commit(round_number, commit) {
						// otherwise validate the commit and signal the finalized block
						// (if any) to the environment
						let validation_result = validate_commit(&commit, &self.voters, &*self.env)?;

						if let Some((finalized_hash, finalized_number)) = validation_result.ghost {
							// this can't be moved to a function because the compiler
							// will complain about getting two mutable borrows to self
							// (due to the call to `self.rounds.get_mut`).
							let last_finalized_number = &mut self.last_finalized_number;

							if finalized_number > *last_finalized_number {
								*last_finalized_number = finalized_number.clone();
								self.env.finalize_block(finalized_hash, finalized_number, round_number, commit)?;
							}
							process_commit_outcome.run(CommitProcessingOutcome::Good(GoodCommit::new()));
						} else {
							// Failing validation of a commit is bad.
							process_commit_outcome.run(
								CommitProcessingOutcome::Bad(BadCommit::from(validation_result)),
							);
						}
					} else {
						// Import to backgrounded round is good.
						process_commit_outcome.run(CommitProcessingOutcome::Good(GoodCommit::new()));
					}
				}
				CommunicationIn::CatchUp(catch_up, mut process_catch_up_outcome) => {
					trace!(target: "afg", "Got catch-up message for round {}", catch_up.round_number);

					if catch_up.round_number <= self.best_round.round_number() {
						trace!(target: "afg", "Ignoring because best round number is {}",
							self.best_round.round_number());

						process_catch_up_outcome.run(CatchUpProcessingOutcome::Useless);

						return Ok(())
					}

					// check threshold support in prevotes and precommits.
					{
						let mut map = std::collections::HashMap::new();
						let voters = &self.voters;

						for prevote in &catch_up.prevotes {
							if !voters.contains_key(&prevote.id) {
								trace!(target: "afg",
									"Ignoring invalid catch up, invalid voter: {:?}",
									prevote.id,
								);

								process_catch_up_outcome.run(CatchUpProcessingOutcome::Bad(BadCatchUp::new()));
								return Ok(())
							}
							map.entry(prevote.id.clone()).or_insert((false, false)).0 = true;
						}

						for precommit in &catch_up.precommits {
							if !voters.contains_key(&precommit.id) {
								trace!(target: "afg",
									"Ignoring invalid catch up, invalid voter: {:?}",
									precommit.id,
								);

								process_catch_up_outcome.run(CatchUpProcessingOutcome::Bad(BadCatchUp::new()));
								return Ok(())
							}
							map.entry(precommit.id.clone()).or_insert((false, false)).1 = true;
						}

						let (pv, pc) = map.into_iter().fold(
							(0, 0),
							|(mut pv, mut pc), (id, (prevoted, precommitted))| {
								let weight = voters.info(&id).map(|i| i.weight()).unwrap_or(0);

								if prevoted {
									pv += weight;
								}

								if precommitted {
									pc += weight;
								}

								(pv, pc)
							},
						);

						let threshold = voters.threshold();
						if pv < threshold || pc < threshold {
							trace!(target: "afg",
								"Ignoring invalid catch up, missing voter threshold"
							);

							process_catch_up_outcome.run(CatchUpProcessingOutcome::Bad(BadCatchUp::new()));
							return Ok(())
						}
					}

					let mut round = crate::round::Round::new(crate::round::RoundParams {
						round_number: catch_up.round_number,
						voters: self.voters.clone(),
						base: (catch_up.base_hash, catch_up.base_number),
					});

					// import prevotes first.
					for crate::SignedPrevote { prevote, id, signature } in catch_up.prevotes {
						match round.import_prevote(&*self.env, prevote, id, signature) {
							Ok(_) => {},
							Err(e) => {
								trace!(target: "afg",
									"Ignoring invalid catch up, error importing prevote: {:?}",
									e,
								);

								process_catch_up_outcome.run(CatchUpProcessingOutcome::Bad(BadCatchUp::new()));
								return Ok(());
							},
						}
					}

					// then precommits.
					for crate::SignedPrecommit { precommit, id, signature } in catch_up.precommits {
						match round.import_precommit(&*self.env, precommit, id, signature) {
							Ok(_) => {},
							Err(e) => {
								trace!(target: "afg",
									"Ignoring invalid catch up, error importing precommit: {:?}",
									e,
								);

								process_catch_up_outcome.run(CatchUpProcessingOutcome::Bad(BadCatchUp::new()));
								return Ok(());
							},
						}
					}

					let state = round.state();
					if !state.completable {
						process_catch_up_outcome.run(CatchUpProcessingOutcome::Bad(BadCatchUp::new()));
						return Ok(())
					}

					// beyond this point, we set this round to the past and
					// start voting in the next round.
					let mut just_completed = VotingRound::completed(
						round,
						self.best_round.finalized_sender(),
						self.env.clone(),
					);

					let new_best = VotingRound::new(
						catch_up.round_number + 1,
						self.voters.clone(),
						self.last_finalized_in_rounds.clone(),
						Some(just_completed.bridge_state()),
						self.best_round.finalized_sender(),
						self.env.clone(),
					);

					// update last-finalized in rounds _after_ starting new round.
					// otherwise the base could be too eagerly set forward.
					if let Some((f_hash, f_num)) = state.finalized.clone() {
						if f_num > self.last_finalized_in_rounds.1 {
							self.last_finalized_in_rounds = (f_hash, f_num);
						}
					}

					self.env.completed(
						just_completed.round_number(),
						just_completed.round_state(),
						just_completed.dag_base(),
						just_completed.historical_votes(),
					)?;

					self.past_rounds.push(&*self.env, just_completed);

					// stop voting in the best round before we background it, just in case it contradicts what
					// we're doing now.
					self.best_round.stop_voting();
					self.past_rounds.push(
						&*self.env,
						std::mem::replace(&mut self.best_round, new_best),
					);

					process_catch_up_outcome.run(CatchUpProcessingOutcome::Good(GoodCatchUp::new()));
				},
			}
		}

		Ok(())
	}

	// process the logic of the best round.
	fn process_best_round(&mut self) -> Poll<(), E::Error> {
		// If the current `best_round` is completable and we've already precommitted,
		// we start a new round at `best_round + 1`.
		let should_start_next = {
			let completable = match self.best_round.poll()? {
				Async::Ready(()) => true,
				Async::NotReady => false,
			};

			let precommitted = match self.best_round.state() {
				Some(&VotingRoundState::Precommitted) => true, // start when we've cast all votes.
				_ => false,
			};

			completable && precommitted
		};

		if !should_start_next { return Ok(Async::NotReady) }

		trace!(target: "afg", "Best round at {} has become completable. Starting new best round at {}",
			self.best_round.round_number(),
			self.best_round.round_number() + 1,
		);

		self.completed_best_round()?;

		// round has been updated. so we need to re-poll.
		self.poll()
	}

	fn completed_best_round(&mut self) -> Result<(), E::Error> {
		self.env.completed(
			self.best_round.round_number(),
			self.best_round.round_state(),
			self.best_round.dag_base(),
			self.best_round.historical_votes(),
		)?;

		let old_round_number = self.best_round.round_number();

		let next_round = VotingRound::new(
			old_round_number + 1,
			self.voters.clone(),
			self.last_finalized_in_rounds.clone(),
			Some(self.best_round.bridge_state()),
			self.best_round.finalized_sender(),
			self.env.clone(),
		);

		let old_round = ::std::mem::replace(&mut self.best_round, next_round);
		self.past_rounds.push(&*self.env, old_round);
		Ok(())
	}

	fn set_last_finalized_number(&mut self, finalized_number: N) -> bool {
		let last_finalized_number = &mut self.last_finalized_number;
		if finalized_number > *last_finalized_number {
			*last_finalized_number = finalized_number;
			return true;
		}
		false
	}
}

impl<H, N, E: Environment<H, N>, GlobalIn, GlobalOut> Future for Voter<H, N, E, GlobalIn, GlobalOut> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
	GlobalIn: Stream<Item=CommunicationIn<H, N, E::Signature, E::Id>, Error=E::Error>,
	GlobalOut: Sink<SinkItem=CommunicationOut<H, N, E::Signature, E::Id>, SinkError=E::Error>,
{
	type Item = ();
	type Error = E::Error;

	fn poll(&mut self) -> Poll<(), E::Error> {
		self.process_incoming()?;
		self.prune_background_rounds()?;
		self.global_out.poll()?;

		self.process_best_round()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tokio::prelude::FutureExt;
	use tokio::runtime::current_thread;
	use crate::SignedPrecommit;
	use crate::testing::{self, GENESIS_HASH, Environment, Id};
	use std::time::Duration;

	#[test]
	fn talking_to_myself() {
		let local_id = Id(5);
		let voters = std::iter::once((local_id, 100)).collect();

		let (network, routing_task) = testing::make_network();
		let (signal, exit) = ::exit_future::signal();

		let global_comms = network.make_global_comms();
		let env = Arc::new(Environment::new(network, local_id));
		current_thread::block_on_all(::futures::future::lazy(move || {
			// initialize chain
			let last_finalized = env.with_chain(|chain| {
				chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
				chain.last_finalized()
			});

			let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

			// run voter in background. scheduling it to shut down at the end.
			let finalized = env.finalized_stream();
			let voter = Voter::new(
				env.clone(),
				voters,
				global_comms,
				0,
				last_round_state,
				last_finalized,
			);
			::tokio::spawn(exit.clone()
				.until(voter.map_err(|_| panic!("Error voting"))).map(|_| ()));

			::tokio::spawn(exit.until(routing_task).map(|_| ()));

			// wait for the best block to finalize.
			finalized
				.take_while(|&(_, n, _)| Ok(n < 6))
				.for_each(|_| Ok(()))
				.map(|_| signal.fire())
		})).unwrap();
	}

	#[test]
	fn finalizing_at_fault_threshold() {
		// 10 voters
		let voters: VoterSet<_> = (0..10).map(|i| (Id(i), 1)).collect();

		let (network, routing_task) = testing::make_network();
		let (signal, exit) = ::exit_future::signal();

		current_thread::block_on_all(::futures::future::lazy(move || {
			::tokio::spawn(exit.clone().until(routing_task).map(|_| ()));

			// 3 voters offline.
			let finalized_streams = (0..7).map(move |i| {
				let local_id = Id(i);
				// initialize chain
				let env = Arc::new(Environment::new(network.clone(), local_id));
				let last_finalized = env.with_chain(|chain| {
					chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
					chain.last_finalized()
				});

				let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

				// run voter in background. scheduling it to shut down at the end.
				let finalized = env.finalized_stream();
				let voter = Voter::new(
					env.clone(),
					voters.clone(),
					network.make_global_comms(),
					0,
					last_round_state,
					last_finalized,
				);
				::tokio::spawn(exit.clone()
					.until(voter.map_err(|_| panic!("Error voting"))).map(|_| ()));

				// wait for the best block to be finalized by all honest voters
				finalized
					.take_while(|&(_, n, _)| Ok(n < 6))
					.for_each(|_| Ok(()))
			});

			::futures::future::join_all(finalized_streams).map(|_| signal.fire())
		})).unwrap();
	}

	#[test]
	fn broadcast_commit() {
		let local_id = Id(5);
		let voters: VoterSet<_> = std::iter::once((local_id, 100)).collect();

		let (network, routing_task) = testing::make_network();
		let (commits, _) = network.make_global_comms();

		let (signal, exit) = ::exit_future::signal();

		let global_comms = network.make_global_comms();
		let env = Arc::new(Environment::new(network, local_id));
		current_thread::block_on_all(::futures::future::lazy(move || {
			// initialize chain
			let last_finalized = env.with_chain(|chain| {
				chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
				chain.last_finalized()
			});

			let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

			// run voter in background. scheduling it to shut down at the end.
			let voter = Voter::new(
				env.clone(),
				voters.clone(),
				global_comms,
				0,
				last_round_state,
				last_finalized,
			);
			::tokio::spawn(exit.clone()
				.until(voter.map_err(|_| panic!("Error voting"))).map(|_| ()));

			::tokio::spawn(exit.until(routing_task).map(|_| ()));

			// wait for the node to broadcast a commit message
			commits.take(1).for_each(|_| Ok(())).map(|_| signal.fire())
		})).unwrap();
	}

	#[test]
	fn broadcast_commit_only_if_newer() {
		let local_id = Id(5);
		let test_id = Id(42);
		let voters: VoterSet<_> = [
			(local_id, 100),
			(test_id, 201),
		].iter().cloned().collect();

		let (network, routing_task) = testing::make_network();
		let (commits_stream, commits_sink) = network.make_global_comms();
		let (round_stream, round_sink) = network.make_round_comms(1, test_id);

		let prevote = Message::Prevote(Prevote {
			target_hash: "E",
			target_number: 6,
		});

		let precommit = Message::Precommit(Precommit {
			target_hash: "E",
			target_number: 6,
		});

		let commit = (1, Commit {
			target_hash: "E",
			target_number: 6,
			precommits: vec![SignedPrecommit {
				precommit: Precommit { target_hash: "E", target_number: 6 },
				signature: testing::Signature(test_id.0),
				id: test_id
			}],
		});

		let (signal, exit) = ::exit_future::signal();

		let global_comms = network.make_global_comms();
		let env = Arc::new(Environment::new(network, local_id));
		current_thread::block_on_all(::futures::future::lazy(move || {
			// initialize chain
			let last_finalized = env.with_chain(|chain| {
				chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
				chain.last_finalized()
			});

			let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

			// run voter in background. scheduling it to shut down at the end.
			let voter = Voter::new(
				env.clone(),
				voters.clone(),
				global_comms,
				0,
				last_round_state,
				last_finalized,
			);
			::tokio::spawn(exit.clone()
				.until(voter.map_err(|e| panic!("Error voting: {:?}", e))).map(|_| ()));

			::tokio::spawn(exit.clone().until(routing_task).map(|_| ()));

			::tokio::spawn(exit.until(::futures::future::lazy(|| {
				round_stream.into_future().map_err(|(e, _)| e)
					.and_then(|(value, stream)| { // wait for a prevote
						assert!(match value {
							Some(SignedMessage { message: Message::Prevote(_), id: Id(5), .. }) => true,
							_ => false,
						});
						let votes = vec![prevote, precommit].into_iter().map(Result::Ok);
						round_sink.send_all(futures::stream::iter_result(votes)).map(|_| stream) // send our prevote
					})
					.and_then(|stream| {
						stream.take_while(|value| match value { // wait for a precommit
							SignedMessage { message: Message::Precommit(_), id: Id(5), .. } => Ok(false),
							_ => Ok(true),
						}).for_each(|_| Ok(()))
					})
					.and_then(|_| {
						// send our commit
						commits_sink.send(CommunicationOut::Commit(commit.0, commit.1))
					})
					.map_err(|_| ())
			})).map(|_| ()));

			// wait for the first commit (ours)
			commits_stream.into_future().map_err(|_| ())
				.and_then(|(_, stream)| {
					stream.take(1).for_each(|_| Ok(())) // the second commit should never arrive
						.timeout(Duration::from_millis(500)).map_err(|_| ())
				})
				.then(|res| {
					assert!(res.is_err()); // so the previous future times out
					signal.fire();
					futures::future::ok::<(), ()>(())
				})
		})).unwrap();
	}

	#[test]
	fn import_commit_for_any_round() {
		let local_id = Id(5);
		let test_id = Id(42);
		let voters: VoterSet<_> = [
			(local_id, 100),
			(test_id, 201),
		].iter().cloned().collect();

		let (network, routing_task) = testing::make_network();
		let (_, commits_sink) = network.make_global_comms();

		let (signal, exit) = ::exit_future::signal();

		// this is a commit for a previous round
		let commit = (0, Commit {
			target_hash: "E",
			target_number: 6,
			precommits: vec![SignedPrecommit {
				precommit: Precommit { target_hash: "E", target_number: 6 },
				signature: testing::Signature(test_id.0),
				id: test_id
			}],
		});

		let global_comms = network.make_global_comms();
		let env = Arc::new(Environment::new(network, local_id));
		current_thread::block_on_all(::futures::future::lazy(move || {
			// initialize chain
			let last_finalized = env.with_chain(|chain| {
				chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
				chain.last_finalized()
			});

			let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

			// run voter in background. scheduling it to shut down at the end.
			let voter = Voter::new(
				env.clone(),
				voters.clone(),
				global_comms,
				1,
				last_round_state,
				last_finalized,
			);
			::tokio::spawn(exit.clone()
				.until(voter.map_err(|_| panic!("Error voting"))).map(|_| ()));

			::tokio::spawn(exit.until(routing_task).map(|_| ()));

			::tokio::spawn(commits_sink.send(CommunicationOut::Commit(commit.0, commit.1))
				.map_err(|_| ()).map(|_| ()));

			// wait for the commit message to be processed which finalized block 6
			env.finalized_stream()
				.take_while(|&(_, n, _)| Ok(n < 6))
				.for_each(|_| Ok(()))
				.map(|_| signal.fire())
		})).unwrap();
	}

	#[test]
	fn skips_to_latest_round_after_catch_up() {
		// 3 voters
		let voters: VoterSet<_> = (0..3).map(|i| (Id(i), 1)).collect();

		let (network, routing_task) = testing::make_network();
		let (signal, exit) = ::exit_future::signal();

		current_thread::block_on_all(::futures::future::lazy(move || {
			::tokio::spawn(exit.clone().until(routing_task).map(|_| ()));

			// initialize unsynced voter at round 0
			let mut unsynced_voter = {
				let local_id = Id(4);

				let env = Arc::new(Environment::new(network.clone(), local_id));
				let last_finalized = env.with_chain(|chain| {
					chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
					chain.last_finalized()
				});

				let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

				Voter::new(
					env.clone(),
					voters.clone(),
					network.make_global_comms(),
					0,
					last_round_state,
					last_finalized,
				)
			};

			let pv = |id| crate::SignedPrevote {
				prevote: crate::Prevote { target_hash: "C", target_number: 4 },
				id: Id(id),
				signature: testing::Signature(99),
			};

			let pc = |id| crate::SignedPrecommit {
				precommit: crate::Precommit { target_hash: "C", target_number: 4 },
				id: Id(id),
				signature: testing::Signature(99),
			};

			// send in a catch-up message for round 5.
			network.send_message(CommunicationIn::CatchUp(
				CatchUp {
					base_number: 1,
					base_hash: GENESIS_HASH,
					round_number: 5,
					prevotes: vec![pv(0), pv(1), pv(2)],
					precommits: vec![pc(0), pc(1), pc(2)],
				},
				Callback::Blank,
			));

			// poll until it's caught up.
			// should skip to round 6
			::futures::future::poll_fn(move || -> Poll<(), ()> {
				let poll = unsynced_voter.poll().map_err(|_| ())?;
				if unsynced_voter.best_round.round_number() == 6 {
					Ok(Async::Ready(()))
				} else {
					Ok(poll)
				}
			}).map(move |_| signal.fire())
		})).unwrap();
	}
}
