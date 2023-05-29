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

use std::mem;

use futures::{
	channel::{mpsc, oneshot},
	future, select, stream, FutureExt, SinkExt, StreamExt,
};
use log::{debug, trace, warn};

use crate::{
	round::{Round, RoundParams, State as RoundState},
	voter::Environment as EnvironmentT,
	voter_set::VoterSet,
	Error, Message, Precommit, Prevote, PrimaryPropose, SignedMessage, LOG_TARGET,
};

/// The state of a voting round.
pub enum State<Timer> {
	/// The voting round has just started.
	Start(Timer, Timer),
	/// Already tried to propose in the voting round, the boolean indicates
	/// whether we actually proposed anything or not. Sending a primary proposal
	/// implies that we are the round primary proposer and that the last round
	/// estimate has not been finalized yet.
	Proposed(Timer, Timer, bool),
	/// Already prevoted in the voting round.
	Prevoted(Timer),
	/// Already precommitted in the voting round.
	Precommitted,
	/// State is poisoned. This is a temporary state for a round and we should
	/// always switch back to it later. If it is found in the wild, that means
	/// there was either a panic or a bug in the state machine code.
	Poisoned,
}

impl<Timer> std::fmt::Debug for State<Timer> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			State::Start(..) => write!(f, "Start"),
			State::Proposed(_, _, proposed) => write!(f, "Proposed({})", proposed),
			State::Prevoted(..) => write!(f, "Prevoted"),
			State::Precommitted => write!(f, "Precommitted"),
			State::Poisoned => write!(f, "Poisoned"),
		}
	}
}

/// Whether we should vote in the current round (i.e. push votes to the sink).
enum Voting {
	/// Voting is disabled for the current round.
	No,
	/// Voting is enabled for the current round (prevotes and precommits).
	Yes,
	/// Voting is enabled for the current round and we are the primary proposer
	/// (we can also push primary propose messages).
	Primary,
}

impl Voting {
	/// Whether the voter should cast round votes (prevotes and precommits).
	fn is_active(&self) -> bool {
		matches!(self, Voting::Yes | Voting::Primary)
	}

	/// Whether the voter is the primary proposer.
	fn is_primary(&self) -> bool {
		matches!(self, Voting::Primary)
	}
}

pub struct CompletableRound<Environment>
where
	Environment: EnvironmentT,
{
	pub incoming: stream::Fuse<Environment::Incoming>,
	pub round:
		Round<Environment::Id, Environment::Hash, Environment::Number, Environment::Signature>,
}

pub struct VotingRound<Environment>
where
	Environment: EnvironmentT,
{
	environment: Environment,
	voting: Voting,
	incoming: stream::Fuse<Environment::Incoming>,
	outgoing: Environment::Outgoing,
	round: Round<Environment::Id, Environment::Hash, Environment::Number, Environment::Signature>,
	state: State<future::Fuse<Environment::Timer>>,
	primary_block: Option<(Environment::Hash, Environment::Number)>,
	previous_round_state: RoundState<Environment::Hash, Environment::Number>,
}

#[derive(Clone)]
pub struct VotingRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
	report_round_state_sender:
		mpsc::Sender<oneshot::Sender<super::report::RoundState<Environment::Id>>>,
}

impl<Environment> VotingRoundHandle<Environment>
where
	Environment: EnvironmentT,
{
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

impl<Environment> VotingRound<Environment>
where
	Environment: EnvironmentT,
{
	pub async fn new(
		environment: Environment,
		voters: VoterSet<Environment::Id>,
		round_number: u64,
		round_base: (Environment::Hash, Environment::Number),
		previous_round_state: RoundState<Environment::Hash, Environment::Number>,
	) -> VotingRound<Environment> {
		let round_data = environment.round_data(round_number).await;
		let round_params = RoundParams { voters, base: round_base, round_number };
		let round = Round::new(round_params);

		let voting = if round_data.voter_id.as_ref() == Some(round.primary_voter().0) {
			Voting::Primary
		} else if round_data.voter_id.as_ref().map_or(false, |id| round.voters().contains(id)) {
			Voting::Yes
		} else {
			Voting::No
		};

		let incoming = round_data.incoming.fuse();
		let state =
			State::Start(round_data.prevote_timer.fuse(), round_data.precommit_timer.fuse());

		VotingRound {
			environment,
			voting,
			incoming,
			outgoing: round_data.outgoing,
			round,
			state,
			primary_block: None,
			previous_round_state,
		}
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

	async fn handle_incoming_message(
		&mut self,
		message: SignedMessage<
			Environment::Hash,
			Environment::Number,
			Environment::Signature,
			Environment::Id,
		>,
	) -> Result<(), Environment::Error> {
		debug!(target: LOG_TARGET, "got incoming message: {:?}", message.message);
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
			Message::PrimaryPropose(primary) => {
				let primary_id = self.round.primary_voter().0.clone();

				// note that id here refers to the party which has cast the vote
				// and not the id of the party which has received the vote message.
				if id == primary_id {
					self.primary_block = Some((primary.target_hash, primary.target_number));
				} else {
					// TODO: log about invalid primary proposer
				}
			},
		}

		Ok(())
	}

	async fn primary_propose(&mut self) -> Result<bool, Environment::Error> {
		if !self.voting.is_primary() {
			return Ok(false)
		}

		match self.previous_round_state.estimate.as_ref() {
			Some(previous_round_estimate) => {
				// a primary proposal is only sent if the previous round estimate has not been finalized.
				let should_send_primary = self
					.previous_round_state
					.finalized
					.as_ref()
					.map_or(true, |finalized| previous_round_estimate.1 > finalized.1);

				if should_send_primary {
					debug!(
						target: LOG_TARGET,
						"Sending primary block hint for round {}",
						self.round.number()
					);

					let primary = PrimaryPropose {
						target_hash: previous_round_estimate.0.clone(),
						target_number: previous_round_estimate.1,
					};

					self.environment.proposed(self.round.number(), primary.clone()).await?;
					self.outgoing.send(Message::PrimaryPropose(primary)).await?;

					Ok(true)
				} else {
					debug!(
						target: LOG_TARGET,
						"Previous round estimate has been finalized, not sending primary block hint for round {}",
						self.round.number(),
					);

					Ok(false)
				}
			},
			None => {
				debug!(
					target: LOG_TARGET,
					"Previous round estimate does not exist, not sending primary block hint for round {}",
					self.round.number(),
				);

				Ok(false)
			},
		}
	}

	async fn prevote(&mut self, prevote_timer_ready: bool) -> Result<bool, Environment::Error> {
		let should_prevote = prevote_timer_ready || self.round.completable();

		if should_prevote {
			if self.voting.is_active() {
				if let Some(prevote) = self.construct_prevote().await? {
					debug!(target: LOG_TARGET, "Casting prevote for round {}", self.round.number());

					self.round.set_prevoted_index();
					self.environment.prevoted(self.round.number(), prevote.clone()).await?;
					self.outgoing.send(Message::Prevote(prevote)).await?;
				} else {
					// when we can't construct a prevote, we should cease voting
					// for the rest of the round.
					self.voting = Voting::No;
				}
			}

			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Construct a prevote message based on local state.
	async fn construct_prevote(
		&self,
	) -> Result<Option<Prevote<Environment::Hash, Environment::Number>>, Environment::Error> {
		use num::{AsPrimitive, One};

		let previous_round_estimate = self
			.previous_round_state
			.estimate
			.as_ref()
			.expect("Rounds only started when prior round completable; qed");

		let find_descendent_of = match self.primary_block {
			None => {
				// vote for best chain containing prior round-estimate.
				previous_round_estimate.0.clone()
			},
			Some(ref primary_block) => {
				// we will vote for the best chain containing `p_hash` iff the
				// previous round's prevote-GHOST included that block and that
				// block is a strict descendent of the previous round-estimate
				// that we are aware of.
				let previous_prevote_ghost = self
					.previous_round_state
					.prevote_ghost
					.as_ref()
					.expect("Rounds only started when prior round completable; qed");

				// if the blocks are equal, we don't check ancestry.
				if primary_block == previous_prevote_ghost {
					primary_block.0.clone()
				} else if primary_block.1 >= previous_prevote_ghost.1 {
					previous_round_estimate.0.clone()
				} else {
					// from this point onwards, the number of the
					// primary-broadcasted block is less than the previous
					// prevote-GHOST's number. if the primary block is in the
					// ancestry of p-G we vote for the best chain containing it.
					match self.environment.ancestry(
						previous_round_estimate.0.clone(),
						previous_prevote_ghost.0.clone(),
					) {
						Ok(ancestry) => {
							let to_sub = primary_block.1 + Environment::Number::one();

							let offset: usize = if previous_prevote_ghost.1 < to_sub {
								0
							} else {
								(previous_prevote_ghost.1 - to_sub).as_()
							};

							if ancestry.get(offset).map_or(false, |hash| *hash == primary_block.0) {
								primary_block.0.clone()
							} else {
								previous_round_estimate.0.clone()
							}
						},
						Err(Error::NotDescendent) => {
							// This is only possible in case of massive equivocation
							warn!(
								target: LOG_TARGET,
								"Possible case of massive equivocation: \
								previous round prevote GHOST: {:?} is not a descendant of previous round estimate: {:?}",
								previous_prevote_ghost,
								previous_round_estimate,
							);

							previous_round_estimate.0.clone()
						},
					}
				}
			},
		};

		if let Some(target) =
			self.environment.best_chain_containing(find_descendent_of.clone()).await?
		{
			Ok(Some(Prevote { target_hash: target.0, target_number: target.1 }))
		} else {
			// If this block is considered unknown something has gone wrong,
			// we'll skip casting a vote.
			warn!(
				target: LOG_TARGET,
				"Could not cast prevote: previously known block {:?} has disappeared",
				find_descendent_of,
			);

			Ok(None)
		}
	}

	async fn precommit(&mut self, precommit_timer_ready: bool) -> Result<bool, Environment::Error> {
		let previous_round_estimate = self
			.previous_round_state
			.estimate
			.as_ref()
			.expect("Rounds only started when prior round completable; qed");

		let should_precommit = {
			// we wait for the previous round's estimate to be equal to or
			// the ancestor of the current round's p-Ghost before precommitting.
			let previous_round_estimate_lower_or_equal_to_prevote_ghost =
				self.round.state().prevote_ghost.as_ref().map_or(false, |p_g| {
					p_g == previous_round_estimate ||
						self.environment.is_equal_or_descendent_of(
							previous_round_estimate.0.clone(),
							p_g.0.clone(),
						)
				});

			previous_round_estimate_lower_or_equal_to_prevote_ghost &&
				(precommit_timer_ready || self.round.completable())
		};

		if should_precommit {
			if self.voting.is_active() {
				debug!(target: LOG_TARGET, "Casting precommit for round {}", self.round.number());

				let precommit = self.construct_precommit();
				self.round.set_precommitted_index();

				self.environment.precommitted(self.round.number(), precommit.clone()).await?;
				self.outgoing.send(Message::Precommit(precommit)).await?;
			}

			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Construct a precommit message based on local state.
	fn construct_precommit(&self) -> Precommit<Environment::Hash, Environment::Number> {
		let target = match self.round.state().prevote_ghost {
			Some(target) => target,
			None => self.round.base(),
		};

		Precommit { target_hash: target.0, target_number: target.1 }
	}

	fn is_completable(&self) -> bool {
		// early exit if the current round is not completable
		if !self.round.completable() || self.round.finalized().is_none() {
			return false
		}

		// make sure that the previous round estimate has been finalized
		if let RoundState {
			estimate: Some((_, previous_round_estimate)),
			finalized: Some((_, previous_round_finalized)),
			..
		} = self.previous_round_state
		{
			// either it was already finalized in the previous round
			let finalized_in_previous_round = previous_round_estimate <= previous_round_finalized;

			// or it must be finalized in the current round
			let finalized_in_current_round =
				self.round.finalized().map_or(false, |(_, current_round_finalized)| {
					previous_round_estimate <= *current_round_finalized
				});

			return finalized_in_previous_round || finalized_in_current_round
		}

		false
	}

	/// Starts and processes the voting round with the given round number.
	pub async fn run(
		&mut self,
		mut report_round_state_receiver: mpsc::Receiver<
			oneshot::Sender<super::report::RoundState<Environment::Id>>,
		>,
		mut previous_round_state_updates: mpsc::Receiver<
			RoundState<Environment::Hash, Environment::Number>,
		>,
	) -> Result<(), Environment::Error> {
		macro_rules! handle_inputs {
			($timer:expr) => {{
				select! {
					// process any incoming message for the round
					message = self.incoming.select_next_some() => {
						self.handle_incoming_message(message?).await?;
						false
					},
					// process any state updates from the previous round
					round_state = previous_round_state_updates.select_next_some() => {
						self.previous_round_state = round_state;
						false
					},
					response_sender = report_round_state_receiver.select_next_some() => {
						self.handle_report_round_state_request(response_sender).await?;
						false
					},
					// process the given timer (for prevoting or precommitting)
					_ = &mut $timer => {
						true
					},
				}
			}};
			() => {
				// if no timer is given (e.g. after we precommitted) we call
				// `handle_inputs!` with a future that never resolves, making sure
				// that only incoming votes and previous round state updates are
				// processed.
				handle_inputs!(futures::future::pending::<()>())
			};
		}

		loop {
			trace!(target: LOG_TARGET, "Round: {}, state: {:?}", self.round.number(), self.state);

			match mem::replace(&mut self.state, State::Poisoned) {
				State::Start(prevote_timer, precommit_timer) => {
					// FIXME: explain why we only need to try this once
					// add test for sending primary (i.e. previous round estimate not finalized)
					let proposed = self.primary_propose().await?;
					self.state = State::Proposed(prevote_timer, precommit_timer, proposed);
				},
				State::Proposed(mut prevote_timer, precommit_timer, proposed) => {
					let prevote_timer_ready = handle_inputs!(prevote_timer);
					let prevoted = self.prevote(prevote_timer_ready).await?;

					if prevoted {
						self.state = State::Prevoted(precommit_timer);
					} else {
						self.state = State::Proposed(prevote_timer, precommit_timer, proposed);
					}
				},
				State::Prevoted(mut precommit_timer) => {
					let precommit_timer_ready = handle_inputs!(precommit_timer);
					let precommitted = self.precommit(precommit_timer_ready).await?;

					if precommitted {
						self.state = State::Precommitted;
					} else {
						self.state = State::Prevoted(precommit_timer);
					}
				},
				State::Precommitted => {
					let _ = handle_inputs!();

					if self.is_completable() {
						break
					} else {
						self.state = State::Precommitted;
					}
				},
				State::Poisoned => {
					// TODO: log and handle error
					unreachable!();
				},
			}
		}

		Ok(())
	}

	pub fn start(
		mut self,
		previous_round_state_updates: mpsc::Receiver<
			RoundState<Environment::Hash, Environment::Number>,
		>,
	) -> (
		impl futures::Future<Output = Result<CompletableRound<Environment>, Environment::Error>>,
		VotingRoundHandle<Environment>,
	) {
		let (report_round_state_sender, report_round_state_receiver) = mpsc::channel(4);

		let run = async {
			self.run(report_round_state_receiver, previous_round_state_updates).await?;
			Ok(CompletableRound { incoming: self.incoming, round: self.round })
		};

		(run, VotingRoundHandle { report_round_state_sender })
	}
}
