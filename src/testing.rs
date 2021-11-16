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

//! Helpers for testing

pub mod chain {
	use crate::{
		std::{collections::BTreeMap, vec::Vec},
		Chain, Error,
	};

	pub const GENESIS_HASH: &str = "genesis";
	const NULL_HASH: &str = "NULL";

	struct BlockRecord {
		number: u32,
		parent: &'static str,
	}

	pub struct DummyChain {
		inner: BTreeMap<&'static str, BlockRecord>,
		pub leaves: Vec<&'static str>,
		finalized: (&'static str, u32),
	}

	impl DummyChain {
		pub fn new() -> Self {
			let mut inner = BTreeMap::new();
			inner.insert(GENESIS_HASH, BlockRecord { number: 1, parent: NULL_HASH });

			DummyChain { inner, leaves: vec![GENESIS_HASH], finalized: (GENESIS_HASH, 1) }
		}

		pub fn push_blocks(&mut self, mut parent: &'static str, blocks: &[&'static str]) {
			if blocks.is_empty() {
				return
			}

			let base_number = self.inner.get(parent).unwrap().number + 1;

			if let Some(pos) = self.leaves.iter().position(|x| x == &parent) {
				self.leaves.remove(pos);
			}

			for (i, descendent) in blocks.iter().enumerate() {
				self.inner
					.insert(descendent, BlockRecord { number: base_number + i as u32, parent });

				parent = descendent;
			}

			let new_leaf = blocks.last().unwrap();
			let new_leaf_number = self.inner.get(new_leaf).unwrap().number;

			let insertion_index = self
				.leaves
				.binary_search_by(|x| {
					self.inner.get(x).unwrap().number.cmp(&new_leaf_number).reverse()
				})
				.unwrap_or_else(|i| i);

			self.leaves.insert(insertion_index, new_leaf);
		}

		pub fn number(&self, hash: &'static str) -> u32 {
			self.inner.get(hash).unwrap().number
		}

		pub fn last_finalized(&self) -> (&'static str, u32) {
			self.finalized.clone()
		}

		pub fn set_last_finalized(&mut self, last_finalized: (&'static str, u32)) {
			self.finalized = last_finalized;
		}

		pub fn best_chain_containing(&self, base: &'static str) -> Option<(&'static str, u32)> {
			let base_number = self.inner.get(base)?.number;

			for leaf in &self.leaves {
				// leaves are in descending order.
				let leaf_number = self.inner.get(leaf).unwrap().number;
				if leaf_number < base_number {
					break
				}

				if leaf == &base {
					return Some((leaf, leaf_number))
				}

				if let Ok(_) = self.ancestry(base, leaf) {
					return Some((leaf, leaf_number))
				}
			}

			None
		}
	}

	impl Chain for DummyChain {
		type Hash = &'static str;
		type Number = u32;

		fn ancestry(
			&self,
			base: &'static str,
			mut block: &'static str,
		) -> Result<Vec<&'static str>, Error> {
			let mut ancestry = Vec::new();

			loop {
				match self.inner.get(block) {
					None => return Err(Error::NotDescendent),
					Some(record) => {
						block = record.parent;
					},
				}

				if block == NULL_HASH {
					return Err(Error::NotDescendent)
				}
				if block == base {
					break
				}

				ancestry.push(block);
			}

			Ok(ancestry)
		}
	}
}

#[cfg(feature = "std")]
pub mod environment {
	use super::chain::*;
	use crate::{
		round::State as RoundState,
		voter::{Callback, GlobalCommunicationIncoming, GlobalCommunicationOutgoing, RoundData},
		Chain, Commit, Equivocation, Error, HistoricalVotes, Message, Precommit, Prevote,
		PrimaryPropose, SignedMessage,
	};
	use async_trait::async_trait;
	use futures::{
		channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
		prelude::*,
	};
	use futures_timer::Delay;
	use log::warn;
	use parking_lot::Mutex;
	use std::{
		collections::HashMap,
		pin::Pin,
		sync::Arc,
		task::{Context, Poll},
		time::Duration,
	};

	#[derive(Hash, Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
	pub struct Id(pub u32);

	#[derive(Debug, Clone, PartialEq, Eq)]
	pub struct Signature(pub u32);

	#[derive(Clone)]
	pub struct Environment {
		inner: Arc<InnerEnvironment>,
	}

	pub struct InnerEnvironment {
		chain: Arc<Mutex<DummyChain>>,
		local_id: Id,
		network: Network,
		listeners: Mutex<
			Vec<UnboundedSender<(&'static str, u32, Commit<&'static str, u32, Signature, Id>)>>,
		>,
		last_completed_and_concluded: Mutex<(u64, u64)>,
	}

	impl Environment {
		pub fn new(network: Network, local_id: Id) -> Self {
			let inner = InnerEnvironment {
				chain: Arc::new(Mutex::new(DummyChain::new())),
				local_id,
				network,
				listeners: Mutex::new(Vec::new()),
				last_completed_and_concluded: Mutex::new((0, 0)),
			};

			Environment { inner: Arc::new(inner) }
		}

		pub fn new_with_chain(
			network: Network,
			local_id: Id,
			chain: Arc<Mutex<DummyChain>>,
		) -> Self {
			let inner = InnerEnvironment {
				chain,
				local_id,
				network,
				listeners: Mutex::new(Vec::new()),
				last_completed_and_concluded: Mutex::new((0, 0)),
			};

			Environment { inner: Arc::new(inner) }
		}

		pub fn with_chain<F, U>(&self, f: F) -> U
		where
			F: FnOnce(&mut DummyChain) -> U,
		{
			let mut chain = self.inner.chain.lock();
			f(&mut *chain)
		}

		/// Stream of finalized blocks.
		pub fn finalized_stream(
			&self,
		) -> UnboundedReceiver<(&'static str, u32, Commit<&'static str, u32, Signature, Id>)> {
			let (tx, rx) = mpsc::unbounded();
			self.inner.listeners.lock().push(tx);
			rx
		}

		fn set_last_completed(&self, last_completed: u64) {
			self.inner.last_completed_and_concluded.lock().0 = last_completed;
		}

		fn set_last_concluded(&self, last_concluded: u64) {
			self.inner.last_completed_and_concluded.lock().1 = last_concluded;
		}

		/// Get the last completed and concluded rounds.
		pub fn last_completed_and_concluded(&self) -> (u64, u64) {
			self.inner.last_completed_and_concluded.lock().clone()
		}
	}

	impl Chain for Environment {
		type Hash = &'static str;
		type Number = u32;

		fn ancestry(
			&self,
			base: &'static str,
			block: &'static str,
		) -> Result<Vec<&'static str>, Error> {
			self.inner.chain.lock().ancestry(base, block)
		}
	}

	#[async_trait]
	impl crate::voter::Environment for Environment {
		type Id = Id;
		type Signature = Signature;
		type Error = Error;
		type Timer = Box<dyn Future<Output = ()> + Send + Sync + Unpin>;
		type Incoming = Box<
			dyn Stream<Item = Result<SignedMessage<&'static str, u32, Signature, Id>, Error>>
				+ Send
				+ Sync
				+ Unpin,
		>;
		type Outgoing =
			Box<dyn Sink<Message<&'static str, u32>, Error = Error> + Send + Sync + Unpin>;

		async fn best_chain_containing(
			&self,
			base: &'static str,
		) -> Result<Option<(&'static str, u32)>, Self::Error> {
			Ok(self.inner.chain.lock().best_chain_containing(base))
		}

		async fn round_data(
			&self,
			round: u64,
		) -> RoundData<Self::Id, Self::Timer, Self::Incoming, Self::Outgoing> {
			const GOSSIP_DURATION: Duration = Duration::from_millis(100);

			let (incoming, outgoing) =
				self.inner.network.make_round_comms(round, self.inner.local_id);

			RoundData {
				voter_id: Some(self.inner.local_id),
				prevote_timer: Box::new(Delay::new(GOSSIP_DURATION)),
				precommit_timer: Box::new(Delay::new(GOSSIP_DURATION + GOSSIP_DURATION)),
				incoming: Box::new(incoming),
				outgoing: Box::new(outgoing),
			}
		}

		fn round_commit_timer(&self) -> Self::Timer {
			use rand::Rng;

			const COMMIT_DELAY_MILLIS: u64 = 100;

			let delay = Duration::from_millis(rand::thread_rng().gen_range(0..COMMIT_DELAY_MILLIS));

			Box::new(Delay::new(delay))
		}

		async fn proposed(
			&self,
			_round: u64,
			_propose: PrimaryPropose<&'static str, u32>,
		) -> Result<(), Self::Error> {
			Ok(())
		}

		async fn prevoted(
			&self,
			_round: u64,
			_prevote: Prevote<&'static str, u32>,
		) -> Result<(), Self::Error> {
			Ok(())
		}

		async fn precommitted(
			&self,
			_round: u64,
			_precommit: Precommit<&'static str, u32>,
		) -> Result<(), Self::Error> {
			Ok(())
		}

		async fn completed(
			&self,
			round: u64,
			_state: RoundState<&'static str, u32>,
			_base: (&'static str, u32),
			_votes: &HistoricalVotes<&'static str, u32, Self::Signature, Self::Id>,
		) -> Result<(), Error> {
			self.set_last_completed(round);
			Ok(())
		}

		async fn concluded(
			&self,
			round: u64,
			_state: RoundState<&'static str, u32>,
			_base: (&'static str, u32),
			_votes: &HistoricalVotes<&'static str, u32, Self::Signature, Self::Id>,
		) -> Result<(), Error> {
			self.set_last_concluded(round);
			Ok(())
		}

		async fn finalize_block(
			&self,
			hash: &'static str,
			number: u32,
			_round: u64,
			commit: Commit<&'static str, u32, Signature, Id>,
		) -> Result<(), Error> {
			let mut chain = self.inner.chain.lock();

			let last_finalized = chain.last_finalized();
			if number as u32 <= last_finalized.1 {
				warn!("Attempted to finalize backwards");
				self.inner
					.listeners
					.lock()
					.retain(|s| s.unbounded_send((hash, number as _, commit.clone())).is_ok());
				return Ok(())
			}

			assert!(
				chain.ancestry(last_finalized.0, hash).is_ok(),
				"Safety violation: reverting finalized block.",
			);

			chain.set_last_finalized((hash, number));

			self.inner
				.listeners
				.lock()
				.retain(|s| s.unbounded_send((hash, number as _, commit.clone())).is_ok());

			Ok(())
		}

		async fn prevote_equivocation(
			&self,
			round: u64,
			equivocation: Equivocation<Id, Prevote<&'static str, u32>, Signature>,
		) {
			panic!("Encountered equivocation in round {}: {:?}", round, equivocation);
		}

		async fn precommit_equivocation(
			&self,
			round: u64,
			equivocation: Equivocation<Id, Precommit<&'static str, u32>, Signature>,
		) {
			panic!("Encountered equivocation in round {}: {:?}", round, equivocation);
		}
	}

	// p2p network data for a round.
	struct BroadcastNetwork<M> {
		receiver: UnboundedReceiver<M>,
		raw_sender: UnboundedSender<M>,
		senders: Vec<UnboundedSender<M>>,
		history: Vec<M>,
	}

	impl<M: std::fmt::Debug + Clone> BroadcastNetwork<M> {
		fn new() -> Self {
			let (tx, rx) = mpsc::unbounded();
			BroadcastNetwork {
				receiver: rx,
				raw_sender: tx,
				senders: Vec::new(),
				history: Vec::new(),
			}
		}

		pub fn send_message(&self, message: M) {
			let _ = self.raw_sender.unbounded_send(message);
		}

		// add a node to the network for a round.
		fn add_node<N, F: Fn(N) -> M>(
			&mut self,
			f: F,
		) -> (impl Stream<Item = Result<M, Error>>, impl Sink<N, Error = Error>) {
			let (tx, rx) = mpsc::unbounded();
			let messages_out = self
				.raw_sender
				.clone()
				.sink_map_err(|e| panic!("Error sending messages: {:?}", e))
				.with(move |message| future::ready(Ok(f(message))));

			// get history to the node.
			for prior_message in self.history.iter().cloned() {
				let _ = tx.unbounded_send(prior_message);
			}

			self.senders.push(tx);

			(rx.map(Ok), messages_out)
		}

		// do routing work
		fn route(&mut self, cx: &mut Context) -> Poll<()> {
			loop {
				match Stream::poll_next(Pin::new(&mut self.receiver), cx) {
					Poll::Pending => return Poll::Pending,
					Poll::Ready(None) => return Poll::Ready(()),
					Poll::Ready(Some(item)) => {
						self.history.push(item.clone());
						log::trace!("pushing: {:?}", item);
						for sender in &self.senders {
							let _ = sender.unbounded_send(item.clone());
						}
					},
				}
			}
		}
	}

	/// Make a test network.
	/// Give the network future to node environments and spawn the routing task
	/// to run.
	pub fn make_network() -> (Network, NetworkRouting) {
		let global_messages = Arc::new(Mutex::new(GlobalMessageNetwork::new()));
		let rounds = Arc::new(Mutex::new(HashMap::new()));
		(
			Network { global_messages: global_messages.clone(), rounds: rounds.clone() },
			NetworkRouting { global_messages, rounds },
		)
	}

	type RoundNetwork = BroadcastNetwork<SignedMessage<&'static str, u32, Signature, Id>>;
	type GlobalMessageNetwork =
		BroadcastNetwork<GlobalCommunicationIncoming<&'static str, u32, Signature, Id>>;

	/// A test network. Instantiate this with `make_network`,
	#[derive(Clone)]
	pub struct Network {
		rounds: Arc<Mutex<HashMap<u64, RoundNetwork>>>,
		global_messages: Arc<Mutex<GlobalMessageNetwork>>,
	}

	impl Network {
		pub fn make_round_comms(
			&self,
			round_number: u64,
			node_id: Id,
		) -> (
			impl Stream<Item = Result<SignedMessage<&'static str, u32, Signature, Id>, Error>>,
			impl Sink<Message<&'static str, u32>, Error = Error>,
		) {
			log::trace!("make_round_comms: {:?}", round_number);
			let mut rounds = self.rounds.lock();
			rounds
				.entry(round_number)
				.or_insert_with(RoundNetwork::new)
				.add_node(move |message| SignedMessage {
					message,
					signature: Signature(node_id.0),
					id: node_id,
				})
		}

		pub fn make_global_comms(
			&self,
		) -> (
			impl Stream<
				Item = Result<GlobalCommunicationIncoming<&'static str, u32, Signature, Id>, Error>,
			>,
			impl Sink<GlobalCommunicationOutgoing<&'static str, u32, Signature, Id>, Error = Error>,
		) {
			let mut global_messages = self.global_messages.lock();
			global_messages.add_node(|message| match message {
				GlobalCommunicationOutgoing::Commit(r, commit) =>
					GlobalCommunicationIncoming::Commit(r, commit.into(), Callback::Blank),
			})
		}

		/// Send a message to all nodes.
		pub fn send_message(
			&self,
			message: GlobalCommunicationIncoming<&'static str, u32, Signature, Id>,
		) {
			self.global_messages.lock().send_message(message);
		}
	}

	/// the network routing task.
	#[derive(Clone)]
	pub struct NetworkRouting {
		rounds: Arc<Mutex<HashMap<u64, RoundNetwork>>>,
		global_messages: Arc<Mutex<GlobalMessageNetwork>>,
	}

	impl Future for NetworkRouting {
		type Output = ();

		fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
			let mut rounds = self.rounds.lock();
			rounds.retain(|_, round| match round.route(cx) {
				Poll::Ready(()) => false,
				Poll::Pending => true,
			});

			let mut global_messages = self.global_messages.lock();
			let _ = global_messages.route(cx);

			// FIXME: not being awake
			cx.waker().wake_by_ref();

			Poll::Pending
		}
	}
}
