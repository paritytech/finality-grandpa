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
use std::hash::Hash;
use ::Client;

/// Necessary environment for a voter.
pub trait Environment: Chain<Self::Hash> {
	type Timer: IntoFuture<Item=(),Error=Self::Error>;
	type Id: Hash + Clone + Eq + ::std::fmt::Debug;
	type Signature: Eq + Clone;
	type H: Hash + Clone + Eq + Ord + ::std::fmt::Debug;
	type In: Stream<Item=SignedMessage<Self::Hash, Self::Signature, Self::Id>,Error=Self::Error>;
	type Out: Sink<SinkItem=Message<Self::Hash>,SinkError=Self::Error>;
	type Error: From<::Error>;

	/// Produce data necessary to start a round of voting.
	fn round_data(&self, round: usize) -> RoundData<
		Self::Timer,
		Self::Id,
		Self::Signature,
		Self::In,
		Self::Out
	>;
}

/// Data necessary to participate in a round.
pub struct RoundData<Timer, Id, Signature, Input, Outgoing> {
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

/// Logic for a voter.
pub struct VotingRound<E: Environment> {
	tracker: Round<E::Id, E::H, E::Signature>,
	incoming: E::Incoming,
	outgoing: E::Outgoing,
	state: Option<State<E::Timer>>,
}

impl<E: Environment> VotingRound<E> {
	fn poll(&mut self, env: &E) -> Poll<(), E::Error> {
		while let Some(incoming) = try_ready!(self.incoming.poll()) {
			let ::SignedMessage { message, signature, id } = incoming;

			match message {
				Message::Prevote(prevote) => {
					// TODO: handle equivocation.
					if let Some(e) = self.tracker.import_prevote(env, prevote, id, signature)? {

					}
				}
				Message::Precommit(precommit) => {
					// TODO: handle equivocation.
					if let Some(e) = self.tracker.import_precommit(env, prevote, id, signature)? {

					}
				}
			}
		}

		self.prevote()?;
		self.precommit()?;
	}

	fn prevote(&mut self) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Start(mut prevote_timer, precommit_timer)) => {
				let should_prevote = match prevote_timer.poll() {
					Err(e) => return Err(e),
					Ok(Async::Ready(())) => true,
					Ok(Async::NotReady) => self.tracker.completable(),
				};

				if should_prevote {
					// TODO: cast prevote.
					self.state = Some(State::Prevoted(precommit_timer));
					unimplemented!();
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
					Ok(Async::NotReady) => self.tracker.completable(),
				};

				if should_precommit {
					// TODO: cast precommit.
					self.state = Some(State::Precommitted);
					unimplemented!();
				} else {
					self.state = Some(State::Prevoted(precommit_timer));
				}
			}
			x => { self.state = x; }
		}

		Ok(())
	}
}
