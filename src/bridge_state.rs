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

//! Bridging round state between rounds.

use crate::round::State as RoundState;
use async_rwlock::{RwLock, RwLockReadGuard};
use std::sync::Arc;

// round state bridged across rounds.
struct Bridged<H, N> {
	inner: RwLock<RoundState<H, N>>,
}

impl<H, N> Bridged<H, N> {
	fn new(inner: RwLock<RoundState<H, N>>) -> Self {
		Bridged {
			inner,
		}
	}
}

/// A prior view of a round-state.
pub(crate) struct PriorView<H, N>(Arc<Bridged<H, N>>);

impl<H, N> PriorView<H, N> {
	/// Push an update to the latter view.
	pub(crate) async fn update(&self, new: RoundState<H, N>) {
		*self.0.inner.write().await = new;
	}
}

/// A latter view of a round-state.
pub(crate) struct LatterView<H, N>(Arc<Bridged<H, N>>);

impl<H, N> LatterView<H, N> {
	/// Fetch a handle to the last round-state.
	pub(crate) async fn get(&self) -> RwLockReadGuard<'_, RoundState<H, N>> {
		self.0.inner.read().await
	}
}

/// Constructs two views of a bridged round-state.
///
/// The prior view is held by a round which produces the state and pushes updates to a latter view.
/// When updating, the latter view's task is updated.
///
/// The latter view is held by the subsequent round, which blocks certain activity
/// while waiting for events on an older round.
pub(crate) fn bridge_state<H, N>(initial: RoundState<H, N>) -> (PriorView<H, N>, LatterView<H, N>) {
	let inner = Arc::new(Bridged::new(RwLock::new(initial)));
	(
		PriorView(inner.clone()), LatterView(inner)
	)
}

#[cfg(test)]
mod tests {
	use async_std::sync::Barrier;
	use futures::executor::LocalPool;
	use futures::task::SpawnExt;
	use super::*;

	#[test]
	fn bridging_state() {
		let initial = RoundState {
			prevote_ghost: None,
			finalized: None,
			estimate: None,
			completable: false,
		};

		let mut pool = LocalPool::new();
		let (prior, latter) = bridge_state(initial);

		let barrier = Arc::new(Barrier::new(2));
		let barrier_other = barrier.clone();

		pool.spawner().spawn(async move {
			barrier_other.wait().await;
			prior.update(RoundState {
				prevote_ghost: Some(("5", 5)),
				finalized: Some(("1", 1)),
				estimate: Some(("3", 3)),
				completable: true,
			}).await;
		}).unwrap();

		pool.run_until(async {
			barrier.wait().await;
			while !latter.get().await.finalized.is_some() {}
		});
	}
}
