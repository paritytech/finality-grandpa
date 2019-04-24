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

//! Logic for voting and handling messages within a single round.

use futures::prelude::*;
use futures::sync::mpsc::UnboundedSender;

use std::hash::Hash;
use std::sync::Arc;

use crate::round::{Round, State as RoundState};
use crate::{
	Commit, Message, Prevote, Precommit, PrimaryPropose, SignedMessage,
	SignedPrecommit, BlockNumberOps, validate_commit, ImportResult,
};
use crate::voter_set::VoterSet;
use super::{Environment, Buffered};

/// The state of a voting round.
pub(super) enum State<T> {
	Start(T, T),
	Proposed(T, T),
	Prevoted(T),
	Precommitted,
}

impl<T> std::fmt::Debug for State<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			State::Start(..) => write!(f, "Start"),
			State::Proposed(..) => write!(f, "Proposed"),
			State::Prevoted(_) => write!(f, "Prevoted"),
			State::Precommitted => write!(f, "Precommitted"),
		}
	}
}

/// Logic for a voter on a specific round.
pub(super) struct VotingRound<H, N, E: Environment<H, N>> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	env: Arc<E>,
	voting: Voting,
	votes: Round<E::Id, H, N, E::Signature>,
	incoming: E::In,
	outgoing: Buffered<E::Out>,
	state: Option<State<E::Timer>>, // state machine driving votes.
	bridged_round_state: Option<crate::bridge_state::PriorView<H, N>>, // updates to later round
	last_round_state: Option<crate::bridge_state::LatterView<H, N>>, // updates from prior round
	primary_block: Option<(H, N)>, // a block posted by primary as a hint.
	finalized_sender: UnboundedSender<(H, N, u64, Commit<H, N, E::Signature, E::Id>)>,
	best_finalized: Option<Commit<H, N, E::Signature, E::Id>>,
}

/// Whether we should vote in the current round (i.e. push votes to the sink.)
enum Voting {
	/// Voting is disabled for the current round.
	No,
	/// Voting is enabled for the current round (prevotes and precommits.)
	Yes,
	/// Voting is enabled for the current round and we are the primary proposer
	/// (we can also push primary propose messages).
	Primary,
}

impl Voting {
	/// Whether the voter should cast round votes (prevotes and precommits.)
	fn is_active(&self) -> bool {
		match self {
			Voting::Yes => true,
			Voting::Primary => true,
			_ => false,
		}
	}

	/// Whether the voter is the primary proposer.
	fn is_primary(&self) -> bool {
		match self {
			Voting::Primary => true,
			_ => false,
		}
	}
}

impl<H, N, E: Environment<H, N>> VotingRound<H, N, E> where
	H: Hash + Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	/// Create a new voting round.
	pub (super) fn new(
		round_number: u64,
		voters: VoterSet<E::Id>,
		base: (H, N),
		last_round_state: Option<crate::bridge_state::LatterView<H, N>>,
		finalized_sender: UnboundedSender<(H, N, u64, Commit<H, N, E::Signature, E::Id>)>,
		env: Arc<E>,
	) -> VotingRound<H, N, E> {
		let round_data = env.round_data(round_number);
		let round_params = crate::round::RoundParams {
			voters,
			base,
			round_number,
		};

		let votes = Round::new(round_params);

		let voting = if round_data.voter_id.as_ref() == Some(&votes.primary_voter().0) {
			Voting::Primary
		} else if round_data.voter_id
			.as_ref()
			.map(|id| votes.voters().contains_key(id))
			.unwrap_or(false)
		{
			Voting::Yes
		} else {
			Voting::No
		};

		VotingRound {
			votes,
			voting,
			incoming: round_data.incoming,
			outgoing: Buffered::new(round_data.outgoing),
			state: Some(
				State::Start(round_data.prevote_timer, round_data.precommit_timer)
			),
			bridged_round_state: None,
			primary_block: None,
			best_finalized: None,
			env,
			last_round_state,
			finalized_sender,
		}
	}

	/// Poll the round. When the round is completable and messages have been flushed, it will return `Async::Ready` but
	/// can continue to be polled.
	pub(super) fn poll(&mut self) -> Poll<(), E::Error> {
		trace!(target: "afg", "Polling round {}, state = {:?}, step = {:?}", self.votes.number(), self.votes.state(), self.state);
		let pre_state = self.votes.state();
		self.process_incoming()?;

		// we only cast votes when we have access to the previous round state.
		// we might have started this round as a prospect "future" round to
		// check whether the voter is lagging behind the current round.
		if let Some(last_round_state) = self.last_round_state.as_ref().map(|s| s.get().clone()) {
			self.primary_propose(&last_round_state)?;
			self.prevote(&last_round_state)?;
			self.precommit(&last_round_state)?;
		}

		try_ready!(self.outgoing.poll());
		self.process_incoming()?; // in case we got a new message signed locally.

		// broadcast finality notifications after attempting to cast votes
		let post_state = self.votes.state();
		self.notify(pre_state, post_state);

		if self.votes.completable() {
			Ok(Async::Ready(()))
		} else {
			Ok(Async::NotReady)
		}
	}

	/// Inspect the state of this round.
	pub(super) fn state(&self) -> Option<&State<E::Timer>> {
		self.state.as_ref()
	}

	/// Get the round number.
	pub(super) fn round_number(&self) -> u64 {
		self.votes.number()
	}

	/// Get the base block in the dag.
	pub(super) fn dag_base(&self) -> (H, N) {
		self.votes.base()
	}

	/// Get the round state.
	pub(super) fn round_state(&self) -> RoundState<H, N> {
		self.votes.state()
	}

	/// Get the voters in this round.
	pub(super) fn voters(&self) -> &VoterSet<E::Id> {
		self.votes.voters()
	}

	/// Get the best block finalized in this round.
	pub(super) fn finalized(&self) -> Option<&(H, N)> {
		self.votes.finalized()
	}

	/// Check a commit. If it's valid, import all the votes into the round as well.
	/// Returns the finalized base if it checks out.
	pub(super) fn check_and_import_from_commit(
		&mut self,
		commit: &Commit<H, N, E::Signature, E::Id>
	) -> Result<Option<(H, N)>, E::Error> {
		let base = validate_commit(&commit, self.voters(), &*self.env)?.ghost;
		if base.is_none() { return Ok(None) }

		for SignedPrecommit { precommit, signature, id } in commit.precommits.iter().cloned() {
			let import_result = self.votes.import_precommit(&*self.env, precommit, id, signature)?;
			if let ImportResult { equivocation: Some(e), .. } = import_result {
				self.env.precommit_equivocation(self.round_number(), e);
			}
		}

		Ok(base)
	}

	/// Get a clone of the finalized sender.
	pub(super) fn finalized_sender(&self)
		-> UnboundedSender<(H, N, u64, Commit<H, N, E::Signature, E::Id>)>
	{
		self.finalized_sender.clone()
	}

	// call this when we build on top of a given round in order to get a handle
	// to updates to the latest round-state.
	pub(super) fn bridge_state(&mut self) -> crate::bridge_state::LatterView<H, N> {
		let (prior_view, latter_view) = crate::bridge_state::bridge_state(self.votes.state());
		if self.bridged_round_state.is_some() {
			warn!(target: "afg", "Bridged state from round {} more than once.",
				self.votes.number());
		}

		self.bridged_round_state = Some(prior_view);
		latter_view
	}

	// call this to bridge state from another around.
	pub(super) fn bridge_state_from(&mut self, other: &mut Self) {
		self.last_round_state = Some(other.bridge_state())
	}

	/// Get a commit justifying the best finalized block.
	pub(super) fn finalizing_commit(&self) -> Option<&Commit<H, N, E::Signature, E::Id>> {
		self.best_finalized.as_ref()
	}

	/// Return all imported votes for the round (prevotes and precommits).
	pub(super) fn votes(&self) -> Vec<SignedMessage<H, N, E::Signature, E::Id>> {
		let prevotes = self.votes.prevotes().into_iter().map(|(id, prevote, signature)| {
			SignedMessage {
				id,
				signature,
				message: Message::Prevote(prevote),
			}
		});

		let precommits = self.votes.precommits().into_iter().map(|(id, precommit, signature)| {
			SignedMessage {
				id,
				signature,
				message: Message::Precommit(precommit),
			}
		});

		prevotes.chain(precommits).collect()
	}

	fn process_incoming(&mut self) -> Result<(), E::Error> {
		while let Async::Ready(Some(incoming)) = self.incoming.poll()? {
			trace!(target: "afg", "Got incoming message");
			let SignedMessage { message, signature, id } = incoming;
			if !self.env.is_equal_or_descendent_of(self.votes.base().0, message.target().0.clone()) {
				trace!(target: "afg", "Ignoring message targeting {:?} lower than round base {:?}",
					   message.target(),
					   self.votes.base(),
				);
				continue;
			}

			match message {
				Message::Prevote(prevote) => {
					let import_result = self.votes.import_prevote(&*self.env, prevote, id, signature)?;
					if let ImportResult { equivocation: Some(e), .. } = import_result {
						self.env.prevote_equivocation(self.votes.number(), e);
					}
				}
				Message::Precommit(precommit) => {
					let import_result = self.votes.import_precommit(&*self.env, precommit, id, signature)?;
					if let ImportResult { equivocation: Some(e), .. } = import_result {
						self.env.precommit_equivocation(self.votes.number(), e);
					}
				}
				Message::PrimaryPropose(primary) => {
					let primary_id = self.votes.primary_voter().0.clone();
					if id == primary_id {
						self.primary_block = Some((primary.target_hash, primary.target_number));
					}
				}
			};
		}

		Ok(())
	}

	fn primary_propose(&mut self, last_round_state: &RoundState<H, N>) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Start(prevote_timer, precommit_timer)) => {
				let maybe_estimate = last_round_state.estimate.clone();

				match (maybe_estimate, self.voting.is_primary()) {
					(Some(last_round_estimate), true) => {
						let maybe_finalized = last_round_state.finalized.clone();

						// Last round estimate has not been finalized.
						let should_send_primary = maybe_finalized.map_or(true, |f| last_round_estimate.1 > f.1);
						if should_send_primary {
							debug!(target: "afg", "Sending primary block hint for round {}", self.votes.number());
							let primary = PrimaryPropose {
								target_hash: last_round_estimate.0,
								target_number: last_round_estimate.1,
							};
							self.env.proposed(self.round_number(), primary.clone())?;
							self.outgoing.push(Message::PrimaryPropose(primary));
							self.state = Some(State::Proposed(prevote_timer, precommit_timer));

							return Ok(());
						} else {
							debug!(target: "afg", "Last round estimate has been finalized, \
								not sending primary block hint for round {}", self.votes.number());
						}
					},
					(None, true) => {
						debug!(target: "afg", "Last round estimate does not exist, \
							not sending primary block hint for round {}", self.votes.number());
					},
					_ => {},
				}

				self.state = Some(State::Start(prevote_timer, precommit_timer));
			},
			x => { self.state = x; }
		}

		Ok(())
	}

	fn prevote(&mut self, last_round_state: &RoundState<H, N>) -> Result<(), E::Error> {
		let state = self.state.take();

		let mut handle_prevote = |mut prevote_timer: E::Timer, precommit_timer: E::Timer, proposed| {
			let should_prevote = match prevote_timer.poll() {
				Err(e) => return Err(e),
				Ok(Async::Ready(())) => true,
				Ok(Async::NotReady) => self.votes.completable(),
			};

			if should_prevote {
				if self.voting.is_active() {
					if let Some(prevote) = self.construct_prevote(last_round_state)? {
						debug!(target: "afg", "Casting prevote for round {}", self.votes.number());
						self.env.prevoted(self.round_number(), prevote.clone())?;
						self.outgoing.push(Message::Prevote(prevote));
					}
				}
				self.state = Some(State::Prevoted(precommit_timer));
			} else {
				if proposed {
					self.state = Some(State::Proposed(prevote_timer, precommit_timer));
				} else {
					self.state = Some(State::Start(prevote_timer, precommit_timer));
				}
			}

			Ok(())
		};

		match state {
			Some(State::Start(prevote_timer, precommit_timer)) => {
				handle_prevote(prevote_timer, precommit_timer, false)?;
			},
			Some(State::Proposed(prevote_timer, precommit_timer)) => {
				handle_prevote(prevote_timer, precommit_timer, true)?;
			},
			x => { self.state = x; }
		}

		Ok(())
	}

	fn precommit(&mut self, last_round_state: &RoundState<H, N>) -> Result<(), E::Error> {
		match self.state.take() {
			Some(State::Prevoted(mut precommit_timer)) => {
				let last_round_estimate = last_round_state.estimate.clone()
					.expect("Rounds only started when prior round completable; qed");

				let should_precommit = {
					// we wait for the last round's estimate to be equal to or
					// the ancestor of the current round's p-Ghost before precommitting.
					self.votes.state().prevote_ghost.as_ref().map_or(false, |p_g| {
						p_g == &last_round_estimate ||
							self.env.is_equal_or_descendent_of(last_round_estimate.0, p_g.0.clone())
					})
				} && match precommit_timer.poll() {
					Err(e) => return Err(e),
					Ok(Async::Ready(())) => true,
					Ok(Async::NotReady) => self.votes.completable(),
				};

				if should_precommit {
					if self.voting.is_active() {
						debug!(target: "afg", "Casting precommit for round {}", self.votes.number());
						let precommit = self.construct_precommit();
						self.env.precommitted(self.round_number(), precommit.clone())?;
						self.outgoing.push(Message::Precommit(precommit));
					}
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
	fn construct_prevote(&self, last_round_state: &RoundState<H, N>) -> Result<Option<Prevote<H, N>>, E::Error> {
		let last_round_estimate = last_round_state.estimate.clone()
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
				let last_prevote_g = last_round_state.prevote_ghost.clone()
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
							let to_sub = p_num + N::one();

							let offset: usize = if last_prevote_g.1 < to_sub {
								0
							} else {
								(last_prevote_g.1 - to_sub).as_()
							};

							if ancestry.get(offset).map_or(false, |b| b == p_hash) {
								p_hash.clone()
							} else {
								last_round_estimate.0
							}
						}
						Err(crate::Error::NotDescendent) => last_round_estimate.0,
					}
				}
			}
		};

		let best_chain = self.env.best_chain_containing(find_descendent_of.clone());
		debug_assert!(best_chain.is_some(), "Previously known block {:?} has disappeared from chain", find_descendent_of);

		let t = match best_chain {
			Some(target) => target,
			None => {
				// If this block is considered unknown, something has gone wrong.
				// log and handle, but skip casting a vote.
				warn!(target: "afg", "Could not cast prevote: previously known block {:?} has disappeared", find_descendent_of);
				return Ok(None)
			}
		};

		Ok(Some(Prevote {
			target_hash: t.0,
			target_number: t.1,
		}))
	}

	// construct a precommit message based on local state.
	fn construct_precommit(&self) -> Precommit<H, N> {
		let t = match self.votes.state().prevote_ghost {
			Some(target) => target,
			None => self.votes.base(),
		};

		Precommit {
			target_hash: t.0,
			target_number: t.1,
		}
	}

	// notify when new blocks are finalized or when the round-estimate is updated
	fn notify(&mut self, last_state: RoundState<H, N>, new_state: RoundState<H, N>) {
		if last_state == new_state { return }

		if let Some(ref b) = self.bridged_round_state {
			b.update(new_state.clone());
		}

		if last_state.finalized != new_state.finalized && new_state.completable {
			// send notification only when the round is completable and we've cast votes.
			// this is a workaround that ensures when we re-instantiate the voter after
			// a shutdown, we never re-create the same round with a base that was finalized
			// in this round or after.
			match (&self.state, new_state.finalized) {
				(&Some(State::Precommitted), Some((ref f_hash, ref f_number))) => {
					let commit = Commit {
						target_hash: f_hash.clone(),
						target_number: f_number.clone(),
						precommits: self.votes.finalizing_precommits(&*self.env)
							.expect("always returns none if something was finalized; this is checked above; qed")
							.collect(),
					};
					let finalized = (f_hash.clone(), f_number.clone(), self.votes.number(), commit.clone());

					let _ = self.finalized_sender.unbounded_send(finalized);
					self.best_finalized = Some(commit);
				}
				_ => {}
			}
		}
	}
}
