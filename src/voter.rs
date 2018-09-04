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
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use round::Round;

use ::{Chain, Equivocation, Message, Prevote, Precommit, SignedMessage};

/// Necessary environment for a voter.
pub trait Environment<H>: Chain<H> {
	type Timer: Future<Item=(),Error=Self::Error>;
	type Id: Hash + Clone + Eq + ::std::fmt::Debug;
	type Signature: Eq + Clone;
	type In: Stream<Item=SignedMessage<H, Self::Signature, Self::Id>,Error=Self::Error>;
	type Out: Sink<SinkItem=Message<H>,SinkError=Self::Error>;
	type Error: From<::Error>;

	/// Produce data necessary to start a round of voting.
	fn round_data(&self, round: usize) -> RoundData<
		Self::Timer,
		Self::Id,
		Self::In,
		Self::Out
	>;

	fn prevote_equivocation(&self, equivocation: Equivocation<Self::Id, Prevote<H>, Self::Signature>);
	fn precommit_equivocation(&self, equivocation: Equivocation<Self::Id, Precommit<H>, Self::Signature>);
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

/// Logic for a voter.
pub struct VotingRound<H, E: Environment<H>> where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug {
	votes: Round<E::Id, H, E::Signature>,
	incoming: E::In,
	outgoing: Buffered<E::Out>,
	state: Option<State<E::Timer>>,
}

impl<H, E: Environment<H>> VotingRound<H, E> where H: Hash + Clone + Eq + Ord + ::std::fmt::Debug {
	fn poll(&mut self, env: &E) -> Poll<(), E::Error> {
		let mut state = self.votes.state();
		while let Some(incoming) = try_ready!(self.incoming.poll()) {
			let SignedMessage { message, signature, id } = incoming;

			match message {
				Message::Prevote(prevote) => {
					if let Some(e) = self.votes.import_prevote(env, prevote, id, signature)? {
						env.prevote_equivocation(e);
					}
				}
				Message::Precommit(precommit) => {
					if let Some(e) = self.votes.import_precommit(env, precommit, id, signature)? {
						env.precommit_equivocation(e);
					}
				}
			};

			let next_state = self.votes.state();
			// TODO: finality and estimate-update notification based on state comparison
			state = next_state;
		}

		self.prevote()?;
		self.precommit()?;

		try_ready!(self.outgoing.poll());

		if state.completable {
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
					self.outgoing.push(Message::Prevote(unimplemented!()));
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
					// TODO: cast precommit.
					self.outgoing.push(Message::Precommit(unimplemented!()));
					self.state = Some(State::Precommitted);
				} else {
					self.state = Some(State::Prevoted(precommit_timer));
				}
			}
			x => { self.state = x; }
		}

		Ok(())
	}
}
