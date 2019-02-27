// Copyright 2018 Parity Technologies (UK) Ltd.
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

//! A voter in GRANDPA. This transitions between rounds and casts votes.
//!
//! Voters rely on some external context to function:
//!   - setting timers to cast votes
//!   - incoming vote streams
//!   - providing voter weights.

use futures::prelude::*;
use futures::task;
use futures::stream::{self, futures_unordered::FuturesUnordered};
use futures::sync::mpsc::{self, UnboundedReceiver};

use std::collections::{HashMap, VecDeque};
use std::cmp;
use std::hash::Hash;
use std::sync::Arc;

use crate::round::State as RoundState;
use crate::{
	Chain, Commit, CompactCommit, Equivocation, Message, Prevote, Precommit, SignedMessage,
	BlockNumberOps, VoterSet, validate_commit
};
use voting_round::{VotingRound, State as VotingRoundState};

mod voting_round;

/// Necessary environment for a voter.
///
/// This encapsulates the database and networking layers of the chain.
pub trait Environment<H: Eq, N: BlockNumberOps>: Chain<H, N> {
	type Timer: Future<Item=(),Error=Self::Error>;
	type Id: Hash + Clone + Eq + ::std::fmt::Debug;
	type Signature: Eq + Clone;
	type In: Stream<Item=SignedMessage<H, N, Self::Signature, Self::Id>,Error=Self::Error>;
	type Out: Sink<SinkItem=Message<H, N>,SinkError=Self::Error>;
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
		Self::Timer,
		Self::In,
		Self::Out
	>;

	/// Return a timer that will be used to delay the broadcast of a commit
	/// message. This delay should not be static to minimize the amount of
	/// commit messages that are sent (e.g. random value in [0, 1] seconds).
	fn round_commit_timer(&self) -> Self::Timer;

	/// Note that a round was completed. This is called when a round has been
	/// voted in. Should return an error when something fatal occurs.
	fn completed(&self, round: u64, state: RoundState<H, N>) -> Result<(), Self::Error>;

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
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub enum CommunicationOut<H, N, S, Id> {
	/// A commit message.
	#[cfg_attr(feature = "derive-codec", codec(index = "0"))]
	Commit(u64, Commit<H, N, S, Id>),
	/// Auxiliary messages out.
	#[cfg_attr(feature = "derive-codec", codec(index = "1"))]
	Auxiliary(AuxiliaryCommunication<H, N, Id>),
}

/// Communication between nodes that is not round-localized.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub enum CommunicationIn<H, N, S, Id> {
	/// A commit message.
	#[cfg_attr(feature = "derive-codec", codec(index = "0"))]
	Commit(u64, CompactCommit<H, N, S, Id>),
	/// Auxiliary messages out.
	#[cfg_attr(feature = "derive-codec", codec(index = "1"))]
	Auxiliary(AuxiliaryCommunication<H, N, Id>),
}

/// Communication between nodes that is not round-localized.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub enum AuxiliaryCommunication<H, N, Id> {
	/// A request for catch-up.
	#[cfg_attr(feature = "derive-codec", codec(index = "0"))]
	CatchUpRequest(CatchUpRequest<Id>),
	/// A response for catch-up request.
	#[cfg_attr(feature = "derive-codec", codec(index = "1"))]
	CatchUp(CatchUp<H, N>)
}

/// A request to catch-up, given current round.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct CatchUpRequest<Id> {
	/// The voter the request is from.
	pub from: Id,
	/// The round this voter claims to be at.
	pub current_round: u64,
}

/// A message for catching-up to a round.
///
/// This is a summary of all votes needed to witness a round's completion.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "derive-codec", derive(Encode, Decode))]
pub struct CatchUp<H, N> {
	/// Prevotes in the round.
	pub prevotes: Vec<Prevote<H, N>>,
	/// Precommits in the round.
	pub precommits: Vec<Precommit<H, N>>,
}

/// Data necessary to participate in a round.
pub struct RoundData<Timer, Input, Output> {
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

// wraps a voting round with a new future that resolves when the round can
// be discarded from the working set.
//
// that point is when the round-estimate is finalized.
struct BackgroundRound<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	inner: VotingRound<H, N, E>,
	task: Option<task::Task>,
	finalized_number: N,
	round_committer: Option<RoundCommitter<H, N, E>>,
}

impl<H, N, E: Environment<H, N>> BackgroundRound<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	fn round_number(&self) -> u64 {
		self.inner.round_number()
	}

	fn is_done(&self) -> bool {
		// no need to listen on a round anymore once the estimate is finalized.
		self.round_committer.is_none() && self.inner.round_state().estimate
			.map_or(false, |x| (x.1) <= self.finalized_number)
	}

	fn update_finalized(&mut self, new_finalized: N) {
		self.finalized_number = cmp::max(self.finalized_number, new_finalized);

		// wake up the future to be polled if done.
		if self.is_done() {
			if let Some(ref task) = self.task {
				task.notify();
			}
		}
	}
}

enum BackgroundRoundChange<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	/// Background round has become irrelevant and can be discarded.
	Irrelevant(u64),
	/// Background round has a commit message to issue but should continue
	/// being driven afterwards.
	Committed(Commit<H, N, E::Signature, E::Id>),
}

impl<H, N, E: Environment<H, N>> Future for BackgroundRound<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	type Item = BackgroundRoundChange<H, N, E>;
	type Error = E::Error;

	fn poll(&mut self) -> Poll<Self::Item, E::Error> {
		self.task = Some(::futures::task::current());

		self.inner.poll()?;

		self.round_committer = match self.round_committer.take() {
			None => None,
			Some(mut committer) => match committer.commit(&mut self.inner)? {
				Async::Ready(None) => None,
				Async::Ready(Some(commit)) => return Ok(Async::Ready(
					BackgroundRoundChange::Committed(commit)
				)),
				Async::NotReady => Some(committer),
			}
		};

		if self.is_done() {
			// if this is fully done (has committed _and_ estimate finalized)
			// we bail for real.
			Ok(Async::Ready(BackgroundRoundChange::Irrelevant(self.round_number())))
		} else {
			Ok(Async::NotReady)
		}
	}
}

struct RoundCommitter<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	commit_timer: E::Timer,
	import_commits: stream::Fuse<mpsc::UnboundedReceiver<Commit<H, N, E::Signature, E::Id>>>,
	last_commit: Option<Commit<H, N, E::Signature, E::Id>>,
}

impl<H, N, E: Environment<H, N>> RoundCommitter<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	fn new(
		commit_timer: E::Timer,
		commit_receiver: mpsc::UnboundedReceiver<Commit<H, N, E::Signature, E::Id>>,
	) -> Self {
		RoundCommitter {
			commit_timer,
			import_commits: commit_receiver.fuse(),
			last_commit: None,
		}
	}

	fn import_commit(
		&mut self,
		voting_round: &mut VotingRound<H, N, E>,
		commit: Commit<H, N, E::Signature, E::Id>,
	) -> Result<bool, E::Error> {
		// ignore commits for a block lower than we already finalized
		if commit.target_number < voting_round.finalized().map(|(_, n)| *n).unwrap_or(N::zero()) {
			return Ok(true);
		}

		if voting_round.check_and_import_from_commit(&commit)?.is_none() {
			return Ok(false)
		}

		self.last_commit = Some(commit);

		Ok(true)
	}

	fn commit(&mut self, voting_round: &mut VotingRound<H, N, E>)
		-> Poll<Option<Commit<H, N, E::Signature, E::Id>>, E::Error>
	{
		while let Ok(Async::Ready(Some(commit))) = self.import_commits.poll() {
			if !self.import_commit(voting_round, commit)? {
				trace!(target: "afg", "Ignoring invalid commit");
			}
		}

		try_ready!(self.commit_timer.poll());

		match (self.last_commit.take(), voting_round.finalized()) {
			(None, Some(_)) => {
				Ok(Async::Ready(voting_round.finalizing_commit().cloned()))
			},
			(Some(Commit { target_number, .. }), Some((_, finalized_number))) if target_number < *finalized_number => {
				Ok(Async::Ready(voting_round.finalizing_commit().cloned()))
			},
			_ => {
				Ok(Async::Ready(None))
			},
		}
	}
}

struct SelfReturningFuture<F> {
	inner: Option<F>,
}

impl<F> From<F> for SelfReturningFuture<F> {
	fn from(f: F) -> Self {
		SelfReturningFuture { inner: Some(f) }
	}
}

impl<F> SelfReturningFuture<F> {
	fn mutate<X: FnOnce(&mut F)>(&mut self, x: X) {
		if let Some(ref mut inner) = self.inner {
			x(inner)
		}
	}
}

impl<F: Future> Future for SelfReturningFuture<F> {
	type Item = (F::Item, F);
	type Error = F::Error;

	fn poll(&mut self) -> Poll<Self::Item, F::Error> {
		match self.inner.take() {
			None => panic!("poll after return is not done in this module; qed"),
			Some(mut f) => match f.poll()? {
				Async::Ready(item) => Ok(Async::Ready((item, f))),
				Async::NotReady => {
					self.inner = Some(f);
					Ok(Async::NotReady)
				}
			}
		}
	}
}

/// A stream for past rounds, which produces any commit messages from those
/// rounds and drives them to completion.
struct PastRounds<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	past_rounds: FuturesUnordered<SelfReturningFuture<BackgroundRound<H, N, E>>>,
	commit_senders: HashMap<u64, mpsc::UnboundedSender<Commit<H, N, E::Signature, E::Id>>>,
}

impl<H, N, E: Environment<H, N>> PastRounds<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	fn new() -> Self {
		PastRounds {
			past_rounds: FuturesUnordered::new(),
			commit_senders: HashMap::new(),
		}
	}

	// push an old voting round onto this stream.
	fn push(&mut self, env: &E, round: VotingRound<H, N, E>) {
		let round_number = round.round_number();
		let (tx, rx) = mpsc::unbounded();
		let background = BackgroundRound {
			inner: round,
			task: None,
			// https://github.com/paritytech/finality-grandpa/issues/50
			finalized_number: N::zero(),
			round_committer: Some(RoundCommitter::new(
				env.round_commit_timer(),
				rx,
			)),
		};
		self.past_rounds.push(background.into());
		self.commit_senders.insert(round_number, tx);
	}

	fn update_finalized(&mut self, f_num: N) {
		// have the task check if it should be pruned.
		// if so, this future will be re-polled
		for bg in self.past_rounds.iter_mut() {
			bg.mutate(|f| f.update_finalized(f_num));
		}
	}

	// import the commit into the given backgrounded round. If not possible,
	// just return and process the commit.
	fn import_commit(&self, round_number: u64, commit: Commit<H, N, E::Signature, E::Id>)
		-> Option<Commit<H, N, E::Signature, E::Id>>
	{
		if let Some(sender) = self.commit_senders.get(&round_number) {
			sender.unbounded_send(commit).map_err(|e| e.into_inner()).err()
		} else {
			Some(commit)
		}
	}
}

impl<H, N, E: Environment<H, N>> Stream for PastRounds<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	type Item = (u64, Commit<H, N, E::Signature, E::Id>);
	type Error = E::Error;

	fn poll(&mut self) -> Poll<Option<Self::Item>, E::Error> {
		loop {
			match self.past_rounds.poll()? {
				Async::Ready(Some((BackgroundRoundChange::Irrelevant(number), _))) => {
					self.commit_senders.remove(&number);
				}
				Async::Ready(Some((BackgroundRoundChange::Committed(commit), round))) => {
					let number = round.round_number();

					// reschedule until irrelevant.
					self.past_rounds.push(round.into());

					debug!(
						target: "afg", "Committing: round_number = {}, \
						target_number = {:?}, target_hash = {:?}",
						number,
						commit.target_number,
						commit.target_hash,
					);

					return Ok(Async::Ready(Some((number, commit))));
				}
				Async::Ready(None) => return Ok(Async::Ready(None)),
				Async::NotReady => return Ok(Async::NotReady),
			}
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
	prospective_round: Option<VotingRound<H, N, E>>,
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
			prospective_round: None,
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
	///
	/// When witnessing a commit from a future round, we also start a prospective round
	/// that can allow us to catch up when we've been stuck behind.
	fn process_incoming(&mut self) -> Result<(), E::Error> {
		let mut highest_incoming_foreign_commit = None;
		while let Async::Ready(Some(item)) = self.global_in.poll()? {
			match item {
				CommunicationIn::Commit(round_number, commit) => {
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
						if let Some((finalized_hash, finalized_number)) = validate_commit(
							&commit,
							&self.voters,
							&*self.env,
						)? {
							highest_incoming_foreign_commit = Some(highest_incoming_foreign_commit
								.map_or(round_number, |n| cmp::max(n, round_number)));

							// this can't be moved to a function because the compiler
							// will complain about getting two mutable borrows to self
							// (due to the call to `self.rounds.get_mut`).
							let last_finalized_number = &mut self.last_finalized_number;

							if finalized_number > *last_finalized_number {
								*last_finalized_number = finalized_number.clone();
								self.env.finalize_block(finalized_hash, finalized_number, round_number, commit)?;
							}
						}
					}
				}
				CommunicationIn::Auxiliary(_aux) => {}, // Do nothing.
			}
		}

		if let Some(round_number) = highest_incoming_foreign_commit {
			self.maybe_start_prospective_round(round_number)?;
		}

		Ok(())
	}

	fn maybe_start_prospective_round(&mut self, round_number: u64) -> Result<(), E::Error> {
		let prospective_round_number =
			self.prospective_round.as_ref().map(|r| r.round_number());

		// we saw a commit for a round `r` that is at least 2 higher than
		// our current best round so we start a prospective round at `r + 1`
		//
		// we also start a prospective round only if our last prospective round is before
		// the given commit message. we could technically restart if they are the same,
		// but if commit messages at the round are live then other messages are likely to be
		// as well. Not restarting gives the best chance of completing the round faster.
		let should_start_prospective = round_number > self.best_round.round_number() + 1 &&
			prospective_round_number.map_or(true, |n| round_number > n);

		if should_start_prospective {
			trace!(target: "afg", "Imported commit for later round than current best {}, starting prospective round at {}",
				self.best_round.round_number(),
				round_number + 1,
			);

			// the GHOST-base in general for a round r is the best finalized
			// block in r-2 or earlier. We can't use the commit's base, since
			// it's only r-1 relative to the new prospective.
			//
			// We use, in this order:
			//   - a finalized a block in the current prospective round or
			//   - a finalized block in the active round, or
			//   - the last finalized in prior rounds
			let ghost_base = self.prospective_round.as_ref()
				.and_then(|r| r.round_state().finalized.clone())
				.or_else(|| self.best_round.round_state().finalized.clone())
				.unwrap_or_else(|| self.last_finalized_in_rounds.clone());

			// we set `last_round_state` to `None` so that no votes are cast
			self.prospective_round = Some(VotingRound::new(
				round_number + 1,
				self.voters.clone(),
				ghost_base,
				None,
				self.best_round.finalized_sender(),
				self.env.clone(),
			));
		}

		Ok(())
	}

	// Processes the current prospective round, returns `Async::NotReady` when
	// the `best_round` has been updated and the caller should re-poll.
	fn process_prospective_round(&mut self) -> Poll<Option<bool>, E::Error> {
		let mut best_round_completable = None;

		// poll the prospective round (if any).
		if let Some(mut prospective_round) = self.prospective_round.take() {
			match prospective_round.poll() {
				// if it is completable then we background it and start the new
				// `best_round` at `prospective_round.number + 1`.
				Ok(Async::Ready(())) => {
					trace!(target: "afg", "Prospective round at {} has become completable. Starting best round at {}",
						   prospective_round.round_number(),
						   prospective_round.round_number() + 1);

					self.completed_prospective_round(prospective_round)?;

					// round has been updated, so we return `NotReady` to trigger a re-poll.
					return Ok(Async::NotReady);
				},
				Ok(_) => {
					assert!(self.best_round.round_number() < prospective_round.round_number());
					if let Async::Ready(()) = self.best_round.poll()? {
						// if `best_round` is completable and we've caught up with prospective round,
						// then we background the current `best_round` and set the `best_round`
						// to `prospective_round`.
						if self.best_round.round_number() == prospective_round.round_number() - 1 {
							trace!(target: "afg", "Best round at {} has caught up with prospective round at {}. \
												   Setting best round to prospective round.",
								   self.best_round.round_number(),
								   prospective_round.round_number());

							prospective_round.bridge_state_from(&mut self.best_round);
							self.completed_best_round(Some(prospective_round))?;

							// round has been updated, so we return `NotReady` to trigger a re-poll.
							return Ok(Async::NotReady);
						}

						best_round_completable = Some(true);

					} else {
						// otherwise we keep the current `prospective_round` running.
						self.prospective_round = Some(prospective_round);
						best_round_completable = Some(false);
					}
				},
				Err(e) => {
					// the `prospective_round` may fail because the commit votes
					// haven't been validated against typical voting rules,
					// e.g. we could start the round with a base that is too
					// high compared to other honest voters and would fail to
					// import their votes.
					trace!(target: "afg", "Prospective round at {} has failed with: {}.",
						   prospective_round.round_number(),
						   e,
					);
				}
			}
		}

		Ok(Async::Ready(best_round_completable))
	}

	fn process_best_round(&mut self, best_round_completable: Option<bool>) -> Poll<(), E::Error> {
		// If the current `best_round` is completable and we've already precommitted,
		// we start a new round at `best_round + 1`.
		let should_start_next = {
			let completable = match best_round_completable {
				Some(true) => true,
				Some(false) => false,
				None => match self.best_round.poll()? {
					Async::Ready(()) => true,
					Async::NotReady => false,
				},
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

		self.completed_best_round(None)?;

		// round has been updated. so we need to re-poll.
		self.poll()
	}

	fn completed_best_round(&mut self, next_round: Option<VotingRound<H, N, E>>) -> Result<(), E::Error> {
		self.env.completed(self.best_round.round_number(), self.best_round.round_state())?;

		let old_round_number = self.best_round.round_number();

		let next_round = next_round.unwrap_or_else(||
			VotingRound::new(
				old_round_number + 1,
				self.voters.clone(),
				self.last_finalized_in_rounds.clone(),
				Some(self.best_round.bridge_state()),
				self.best_round.finalized_sender(),
				self.env.clone(),
			)
		);

		let old_round = ::std::mem::replace(&mut self.best_round, next_round);
		self.past_rounds.push(&*self.env, old_round);
		Ok(())
	}

	fn completed_prospective_round(&mut self, mut prospective_round: VotingRound<H, N, E>)
		-> Result<(), E::Error>
	{
		self.env.completed(prospective_round.round_number(), prospective_round.round_state())?;

		self.best_round = VotingRound::new(
			prospective_round.round_number() + 1,
			self.voters.clone(),
			// the finalized commit target that triggered
			// the prospective round was used as base.
			prospective_round.dag_base(),
			Some(prospective_round.bridge_state()),
			self.best_round.finalized_sender(),
			self.env.clone(),
		);

		self.past_rounds.push(&*self.env, prospective_round);

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

		// this returns `Async::NotReady` when the `best_round` is updated and we should re-poll.
		match self.process_prospective_round()? {
			Async::Ready(best_round_completable) => self.process_best_round(best_round_completable),
			Async::NotReady => self.poll(),
		}
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
	fn skips_to_latest_round() {
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

			// poll the unsynced voter until it reaches a round higher than 5
			::tokio::spawn(::futures::future::poll_fn(move || {
				if unsynced_voter.best_round.round_number() > 5 {
					Ok(Async::Ready(()))
				} else {
					unsynced_voter.poll().map_err(|_| ())
				}
			}).map(|_| signal.fire()));

			// initialize all remaining voters at round 5
			let synced_voters = (0..3).map(move |i| {
				let local_id = Id(i);

				// initialize chain
				let env = Arc::new(Environment::new(network.clone(), local_id));
				let last_finalized = env.with_chain(|chain| {
					chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
					chain.last_finalized()
				});

				let last_round_state = RoundState::genesis((GENESIS_HASH, 1));

				// run voter in background starting at round 5. scheduling it to shut down when signalled.
				let voter = Voter::new(
					env.clone(),
					voters.clone(),
					network.make_global_comms(),
					5,
					last_round_state,
					last_finalized,
				);

				exit.clone()
					.until(voter.map_err(|_| panic!("Error voting")))
					.map(|_| ())
					.map_err(|_| ())
			});

			::futures::future::join_all(synced_voters)
		})).unwrap();
	}
}
