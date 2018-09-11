// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-afg.

// finality-afg is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// finality-afg is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-afg. If not, see <http://www.gnu.org/licenses/>.

//! A voter in AfG. This transitions between rounds and casts votes.
//!
//! Voters rely on some external context to function:
//!   - setting timers to cast votes
//!   - incoming vote streams
//!   - providing voter weights.

use futures::prelude::*;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use round::{Round, State as RoundState};

use ::{Chain, Equivocation, Message, Prevote, Precommit, SignedMessage};

/// Necessary environment for a voter.
///
/// This encapsulates the database and networking layers of the chain.
pub trait Environment<H>: Chain<H> {
	type Timer: Future<Item=(),Error=Self::Error>;
	type Id: Hash + Clone + Eq + ::std::fmt::Debug;
	type Signature: Eq + Clone;
	type In: Stream<Item=SignedMessage<H, Self::Signature, Self::Id>,Error=Self::Error>;
	type Out: Sink<SinkItem=Message<H>,SinkError=Self::Error>;
	type Error: From<::Error>;

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
		Self::Id,
		Self::In,
		Self::Out
	>;

	/// Note that a round was completed. This is called when a round has been
	/// voted in.
	fn completed(&self, round: u64, state: RoundState<H>);

	/// Called when a block should be finalized.
	// TODO: make this a future that resolves when it's e.g. written to disk?
	fn finalize_block(&self, hash: H, number: u32);

	// Note that an equivocation in prevotes has occurred.
	fn prevote_equivocation(&self, round: u64, equivocation: Equivocation<Self::Id, Prevote<H>, Self::Signature>);
	// Note that an equivocation in precommits has occurred.
	fn precommit_equivocation(&self, round: u64, equivocation: Equivocation<Self::Id, Precommit<H>, Self::Signature>);
}

/// Data necessary to participate in a round.
pub struct RoundData<Timer, Id, Input, Output> {
	/// Timer before prevotes can be cast. This should be Start + 2T
	/// where T is the gossip time estimate.
	pub prevote_timer: Timer,
	/// Timer before precommits can be cast. This should be Start + 4T
	pub precommit_timer: Timer,
	/// All voters in this round.
	pub voters: HashMap<Id, usize>,
	/// Incoming messages.
	pub incoming: Input,
	/// Outgoing messages.
	pub outgoing: Output,
}

enum State<T> {
	Start(T, T),
	Prevoted(T),
	Precommitted,
}

struct Buffered<S: Sink> {
	inner: S,
	buffer: VecDeque<S::SinkItem>,
}

impl<S: Sink> Buffered<S> {
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

/// Logic for a voter on a specific round.
pub struct VotingRound<H, E: Environment<H>> where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug {
	env: Arc<E>,
	votes: Round<E::Id, H, E::Signature>,
	incoming: E::In,
	outgoing: Buffered<E::Out>,
	state: Option<State<E::Timer>>,
	last_round_state: RoundState<H>,
	primary_block: Option<(H, usize)>,
	finalized_sender: UnboundedSender<u32>,
}

impl<H, E: Environment<H>> VotingRound<H, E> where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug {
	// Poll the round. When the round is completable and messages have been flushed, it will return `Async::Ready` but
	// can continue to be polled.
	fn poll(&mut self) -> Poll<(), E::Error> {
		let pre_state = self.votes.state();
		while let Async::Ready(Some(incoming)) = self.incoming.poll()? {
			let SignedMessage { message, signature, id } = incoming;

			match message {
				Message::Prevote(prevote) => {
					if let Some(e) = self.votes.import_prevote(&*self.env, prevote, id, signature)? {
						self.env.prevote_equivocation(self.votes.number(), e);
					}
				}
				Message::Precommit(precommit) => {
					if let Some(e) = self.votes.import_precommit(&*self.env, precommit, id, signature)? {
						self.env.precommit_equivocation(self.votes.number(), e);
					}
				}
			};
		}

		let post_state = self.votes.state();
		self.notify(pre_state, post_state);

		self.prevote()?;
		self.precommit()?;

		try_ready!(self.outgoing.poll());

		if self.votes.completable() {
			Ok(Async::Ready(()))
		} else {
			Ok(Async::NotReady)
		}
	}

	fn prevote(&mut self) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Start(mut prevote_timer, precommit_timer)) => {
				let should_prevote = match prevote_timer.poll() {
					Err(e) => return Err(e),
					Ok(Async::Ready(())) => true,
					Ok(Async::NotReady) => self.votes.completable(),
				};

				if should_prevote {
					if let Some(prevote) = self.construct_prevote()? {
						self.outgoing.push(Message::Prevote(prevote));
					}
					self.state = Some(State::Prevoted(precommit_timer));
				} else {
					self.state = Some(State::Start(prevote_timer, precommit_timer));
				}
			}
			x => { self.state = x; }
		}

		Ok(())
	}

	fn precommit(&mut self) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Prevoted(mut precommit_timer)) => {
				let should_precommit = match precommit_timer.poll() {
					Err(e) => return Err(e),
					Ok(Async::Ready(())) => true,
					Ok(Async::NotReady) => self.votes.completable(),
				};

				if should_precommit {
					let precommit = self.construct_precommit();
					self.outgoing.push(Message::Precommit(precommit));
					self.state = Some(State::Precommitted);
				} else {
					self.state = Some(State::Prevoted(precommit_timer));
				}
			}
			x => { self.state = x; }
		}

		Ok(())
	}

	// construct a prevote message based on local state.
	fn construct_prevote(&self) -> Result<Option<Prevote<H>>, E::Error> {
		let last_round_estimate = self.last_round_state.estimate.clone()
			.expect("Rounds only started when prior round completable; qed");

		let find_descendent_of = match self.primary_block {
			None => {
				// vote for best chain containing prior round-estimate.
				last_round_estimate.0
			}
			Some(ref primary_block) => {
				// we will vote for the best chain containing `p_hash` iff
				// the last round's prevote-GHOST included that block and
				// that block is a strict descendent of the last round-estimate that we are
				// aware of.
				let last_prevote_g = self.last_round_state.prevote_ghost.clone()
					.expect("Rounds only started when prior round completable; qed");

				// if the blocks are equal, we don't check ancestry.
				if primary_block == &last_prevote_g {
					primary_block.0.clone()
				} else if primary_block.1 >= last_prevote_g.1 {
					last_round_estimate.0
				} else {
					// from this point onwards, the number of the primary-broadcasted
					// block is less than the last prevote-GHOST's number.
					// if the primary block is in the ancestry of p-G we vote for the
					// best chain containing it.
					let &(ref p_hash, p_num) = primary_block;
					match self.env.ancestry(last_round_estimate.0.clone(), last_prevote_g.0) {
						Ok(ancestry) => {
							let offset = last_prevote_g.1.saturating_sub(p_num + 1);
							if ancestry.get(offset).map_or(false, |b| b == p_hash) {
								p_hash.clone()
							} else {
								last_round_estimate.0
							}
						}
						Err(::Error::NotDescendent) => last_round_estimate.0,
					}
				}
			}
		};

		let best_chain = self.env.best_chain_containing(find_descendent_of);
		debug_assert!(best_chain.is_some(), "Previously known block has disappeared from chain");

		let t = match best_chain {
			Some(target) => target,
			None => {
				// If this block is considered unknown, something has gone wrong.
				// log and handle, but skip casting a vote.
				warn!(target: "afg", "Could not cast prevote: previously known block has disappeared");
				return Ok(None)
			}
		};

		Ok(Some(Prevote {
			target_hash: t.0,
			target_number: t.1 as u32,
		}))
	}

	// construct a precommit message based on local state.
	fn construct_precommit(&self) -> Precommit<H> {
		let t = match self.votes.state().prevote_ghost {
			Some(target) => target,
			None => self.votes.base(),
		};

		Precommit {
			target_hash: t.0,
			target_number: t.1 as u32,
		}
	}

	// notify when new blocks are finalized or when the round-estimate is updated
	fn notify(&self, last_state: RoundState<H>, new_state: RoundState<H>) {
		if last_state.finalized != new_state.finalized && new_state.completable {
			// send notification only when the round is completable and we've cast votes.
			// this is a workaround for avoiding restarting the round based on
			match (&self.state, new_state.finalized) {
				(&Some(State::Precommitted), Some((_, f_num))) => {
					let _ = self.finalized_sender.unbounded_send(f_num as _);
				}
				_ => {}
			}
		}

		if last_state.estimate != new_state.estimate {
			// TODO: notify the next round about it.
		}
	}
}

// wraps a voting round with a new future that resolves when the round can
// be discarded from the working set.
//
// that point is when the round-estimate is finalized.
struct BackgroundRound<H, E: Environment<H>>
	where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug
{
	inner: VotingRound<H, E>,
	task: Option<::futures::task::Task>,
	finalized_number: u32,
}

impl<H, E: Environment<H>> BackgroundRound<H, E>
	where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug
{
	fn is_done(&self) -> bool {
		self.inner.votes.state().estimate
			.map_or(false, |x| (x.1 as u32) <= self.finalized_number)
	}

	fn update_finalized(&mut self, new_finalized: u32) {
		self.finalized_number = ::std::cmp::max(self.finalized_number, new_finalized);

		// wake up the future to be polled if done.
		if self.is_done() {
			if let Some(ref task) = self.task {
				task.notify();
			}
		}
	}
}

impl<H, E: Environment<H>> Future for BackgroundRound<H, E>
	where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug
{
	type Item = u64; // round number
	type Error = E::Error;

	fn poll(&mut self) -> Poll<u64, E::Error> {
		self.task = Some(::futures::task::current());
		self.inner.poll()?;

		if self.is_done() {
			Ok(Async::Ready(self.inner.votes.number()))
		} else {
			Ok(Async::NotReady)
		}
	}
}

/// A future that maintains and multiplexes between different rounds,
/// and caches votes.
pub struct Voter<H, E: Environment<H>>
	where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug
{
	env: Arc<E>,
	best_round: VotingRound<H, E>,
	past_rounds: FuturesUnordered<BackgroundRound<H, E>>,
	finalized_notifications: UnboundedReceiver<u32>,
}

impl<H, E: Environment<H>> Voter<H, E>
	where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug
{
	/// Create new `Voter` tracker with given round number and base block.
	///
	/// Provide data about the last completed round. If there is no
	/// known last completed round, the genesis state (round number 0),
	/// should be provided.
	pub fn new(
		env: Arc<E>,
		last_round: u64,
		last_round_state: RoundState<H>,
		last_finalized: (H, usize),
	) -> Self {
		let (finalized_sender, finalized_notifications) = mpsc::unbounded();

		let next_number = last_round + 1;
		let round_data = env.round_data(next_number);

		let round_params = ::round::RoundParams {
			round_number: next_number as _,
			voters: round_data.voters,
			base: last_finalized,
		};

		let best_round = VotingRound {
			env: env.clone(),
			votes: Round::new(round_params),
			incoming: round_data.incoming,
			outgoing: Buffered {
				inner: round_data.outgoing,
				buffer: VecDeque::new(),
			},
			state: Some(
				State::Start(round_data.prevote_timer, round_data.precommit_timer)
			),
			last_round_state,
			primary_block: None,
			finalized_sender,
		};

		Voter {
			env,
			best_round,
			past_rounds: FuturesUnordered::new(),
			finalized_notifications,
		}
	}

	fn prune_background(&mut self) -> Result<(), E::Error> {
		while let Async::Ready(res) = self.finalized_notifications.poll()
			.expect("unbounded receivers do not have spurious errors; qed")
		{
			let notification = res.expect("one sender always kept alive in self.best_round; qed");

			for bg in self.past_rounds.iter_mut() {
				bg.update_finalized(notification);
			}
		}

		// pump all completed rounds out.
		while let Async::Ready(Some(_)) = self.past_rounds.poll()? { }
		Ok(())
	}
}

impl<H, E: Environment<H>> Future for Voter<H, E>
	where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug
{
	type Item = ();
	type Error = E::Error;

	fn poll(&mut self) -> Poll<(), E::Error> {
		self.prune_background()?;
		let should_start_next = match self.best_round.poll()? {
			Async::Ready(()) => match self.best_round.state {
				Some(State::Precommitted) => true, // start when we've cast all votes.
				_ => panic!("Returns ready only when round completable and messages flushed; \
					completable implies precommit sent; qed"),
			},
			Async::NotReady => false,
		};

		if !should_start_next { return Ok(Async::NotReady) }

		let next_number = self.best_round.votes.number() + 1;
		let next_round_data = self.env.round_data(next_number);

		// TODO: choose start block based on what's finalized.
		let base = self.best_round.votes.base();

		let round_params = ::round::RoundParams {
			round_number: next_number as _,
			voters: next_round_data.voters,
			base,
		};

		let next_round = VotingRound {
			env: self.env.clone(),
			votes: Round::new(round_params),
			incoming: next_round_data.incoming,
			outgoing: Buffered {
				inner: next_round_data.outgoing,
				buffer: VecDeque::new(),
			},
			state: Some(
				State::Start(next_round_data.prevote_timer, next_round_data.precommit_timer)
			),
			last_round_state: self.best_round.votes.state(),
			primary_block: None,
			finalized_sender: self.best_round.finalized_sender.clone(),
		};

		let old_round = ::std::mem::replace(&mut self.best_round, next_round);
		let background = BackgroundRound {
			inner: old_round,
			task: None,
			finalized_number: 0, // TODO: do that right.
		};

		self.past_rounds.push(background);

		// round has been updated. so we need to re-poll.
		self.poll()
	}
}
