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

#[cfg(feature = "std")]
use futures::prelude::*;
use futures::stream;
use futures::task;
use futures::channel::mpsc;
#[cfg(feature = "std")]
use log::{debug, trace};

use std::cmp;

use crate::{Commit, BlockNumberOps};
use super::Environment;
use super::voting_round::VotingRound;

// wraps a voting round with a new future that resolves when the round can
// be discarded from the working set.
//
// that point is when the round-estimate is finalized.
pub(super) struct BackgroundRound<H, N, E: Environment<H, N>> where
	H: Clone + Eq + Ord + ::std::fmt::Debug + Send + Sync,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	inner: VotingRound<H, N, E>,
	waker: Option<task::Waker>,
	finalized_number: N,
	round_committer: RoundCommitter<H, N, E>,
}

impl<H, N, E: Environment<H, N> + Send + Sync> BackgroundRound<H, N, E> where
	H: Clone + Eq + Ord + ::std::fmt::Debug + Send + Sync + Unpin,
	N: Copy + BlockNumberOps + ::std::fmt::Debug + Send + Sync + Unpin,
{

	// push an old voting round onto this stream.
	pub(super) fn new(env: &E, round: VotingRound<H, N, E>) -> Self {
		let round_number = round.round_number();
		let (tx, rx) = mpsc::unbounded();
		let background = BackgroundRound {
			inner: round,
			waker: None,
			// https://github.com/paritytech/finality-grandpa/issues/50
			finalized_number: N::zero(),
			round_committer: RoundCommitter::new(
				env.round_commit_timer(),
				rx,
			),
		};
		// self.past_rounds.push(background.into());
		// self.commit_senders.insert(round_number, tx);
		background
	}

	fn round_number(&self) -> u64 {
		self.inner.round_number()
	}

	fn voting_round(&self) -> &VotingRound<H, N, E> {
		&self.inner
	}

	fn is_done(&self) -> bool {
		// no need to listen on a round anymore once the estimate is finalized.
		//
		// we map `None` to true because
		//   - rounds are not backgrounded when incomplete unless we've skipped forward
		//   - if we skipped forward we may never complete this round and we don't need
		//     to keep it forever.
		self.round_committer.is_none() && self.inner.round_state().estimate
			.map_or(true, |x| x.1 <= self.finalized_number)
	}

	fn update_finalized(&mut self, new_finalized: N) {
		self.finalized_number = cmp::max(self.finalized_number, new_finalized);

		// wake up the future to be polled if done.
		if self.is_done() {
			if let Some(ref waker) = self.waker {
				waker.wake_by_ref();
			}
		}
	}

	pub(super) async fn poll(&mut self) -> Result<Option<(u64, Commit<H, N, E::Signature, E::Id>)>, E::Error> {
		self.inner.poll().await;

		let commit_result = match self.round_committer.commit(&mut self.inner).await? {
			Some(commit) => {
				let number = self.round_number();

				debug!(
					target: "afg", "Committing: round_number = {}, \
					target_number = {:?}, target_hash = {:?}",
					number,
					commit.target_number,
					commit.target_hash,
				);

				Ok(Some((number, commit)))
			},
			None => None,
		};

		if self.is_done() {
			// if this is fully concluded (has committed _and_ estimate finalized)
			// we bail for real.
			self.inner.env().concluded(
				self.inner.round_number(),
				self.inner.round_state(),
				self.inner.dag_base(),
				self.inner.historical_votes(),
			)?;
		}

		commit_result
	}
}

enum BackgroundRoundChange<H, N, E: Environment<H, N> + Send + Sync> where
	H: Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug + Send + Sync,
{
	/// Background round has fully concluded and can be discarded.
	Concluded(u64),
	/// Background round has a commit message to issue but should continue
	/// being driven afterwards.
	Committed(Commit<H, N, E::Signature, E::Id>),
}

impl<H, N, E: Environment<H, N>> Unpin for BackgroundRound<H, N, E> where
	H: Clone + Eq + Ord + ::std::fmt::Debug + Send + Sync,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
}

struct RoundCommitter<H, N, E: Environment<H, N>> where
	H: Clone + Eq + Ord + ::std::fmt::Debug,
	N: Copy + BlockNumberOps + ::std::fmt::Debug,
{
	commit_timer: E::Timer,
	import_commits: stream::Fuse<mpsc::UnboundedReceiver<Commit<H, N, E::Signature, E::Id>>>,
	last_commit: Option<Commit<H, N, E::Signature, E::Id>>,
}

impl<H, N, E: Environment<H, N>> RoundCommitter<H, N, E> where
	H: Clone + Eq + Ord + ::std::fmt::Debug + Send + Sync + Unpin,
	N: Copy + BlockNumberOps + ::std::fmt::Debug + Unpin,
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
		if commit.target_number < voting_round.finalized().map_or_else(N::zero, |(_, n)| *n) {
			return Ok(true);
		}

		if voting_round.check_and_import_from_commit(&commit)?.is_none() {
			return Ok(false)
		}

		self.last_commit = Some(commit);

		Ok(true)
	}

	async fn commit(&mut self, voting_round: &mut VotingRound<H, N, E>)
		-> Result<Option<Commit<H, N, E::Signature, E::Id>>, E::Error>
	{
		while let Some(commit) = self.import_commits.next().await {
			if !self.import_commit(voting_round, commit)? {
				trace!(target: "afg", "Ignoring invalid commit");
			}
		}

		(&mut self.commit_timer).await?;

		match (self.last_commit.take(), voting_round.finalized()) {
			(None, Some(_)) => {
				Ok(voting_round.finalizing_commit().cloned())
			},
			(Some(Commit { target_number, .. }), Some((_, finalized_number))) if target_number < *finalized_number => {
				Ok(voting_round.finalizing_commit().cloned())
			},
			_ => {
				Ok(None)
			},
		}
	}
}

// A stream for past rounds, which produces any commit messages from those
// rounds and drives them to completion.
// pub(super) struct PastRounds<H, N, E: Environment<H, N>> where
// 	H: Clone + Eq + Ord + ::std::fmt::Debug + Send + Sync,
// 	N: Copy + BlockNumberOps + ::std::fmt::Debug,
// {
// 	past_rounds: FuturesUnordered<SelfReturningFuture<BackgroundRound<H, N, E>>>,
// 	commit_senders: HashMap<u64, mpsc::UnboundedSender<Commit<H, N, E::Signature, E::Id>>>,
// }

// impl<H, N, E: Environment<H, N>> PastRounds<H, N, E> where
// 	H: Clone + Eq + Ord + ::std::fmt::Debug + Send + Sync + Unpin,
// 	N: Copy + BlockNumberOps + ::std::fmt::Debug + Unpin,
// {
// 	/// Create a new past rounds stream.
// 	pub(super) fn new() -> Self {
// 		PastRounds {
// 			past_rounds: FuturesUnordered::new(),
// 			commit_senders: HashMap::new(),
// 		}
// 	}

// 	// push an old voting round onto this stream.
// 	pub(super) fn push(&mut self, env: &E, round: VotingRound<H, N, E>) {
// 		let round_number = round.round_number();
// 		let (tx, rx) = mpsc::unbounded();
// 		let background = BackgroundRound {
// 			inner: round,
// 			waker: None,
// 			// https://github.com/paritytech/finality-grandpa/issues/50
// 			finalized_number: N::zero(),
// 			round_committer: Some(RoundCommitter::new(
// 				env.round_commit_timer(),
// 				rx,
// 			)),
// 			inner_future: None,
// 		};
// 		self.past_rounds.push(background.into());
// 		self.commit_senders.insert(round_number, tx);
// 	}

// 	/// update the last finalized block. this will lead to
// 	/// any irrelevant background rounds being pruned.
// 	pub(super) fn update_finalized(&mut self, f_num: N) {
// 		// have the task check if it should be pruned.
// 		// if so, this future will be re-polled
// 		for bg in self.past_rounds.iter_mut() {
// 			bg.mutate(|f| f.update_finalized(f_num));
// 		}
// 	}

// 	/// Get the underlying `VotingRound` items that are being run in the background.
// 	pub(super) fn voting_rounds(&self) -> impl Iterator<Item = &VotingRound<H, N, E>> {
// 		self.past_rounds
// 			.iter()
// 			.filter_map(|self_returning_future| self_returning_future.inner.as_ref())
// 			.map(|background_round| background_round.voting_round())
// 	}

// 	// import the commit into the given backgrounded round. If not possible,
// 	// just return and process the commit.
// 	pub(super) fn import_commit(&self, round_number: u64, commit: Commit<H, N, E::Signature, E::Id>)
// 		-> Option<Commit<H, N, E::Signature, E::Id>>
// 	{
// 		if let Some(sender) = self.commit_senders.get(&round_number) {
// 			sender.unbounded_send(commit).map_err(|e| e.into_inner()).err()
// 		} else {
// 			Some(commit)
// 		}
// 	}
// }
