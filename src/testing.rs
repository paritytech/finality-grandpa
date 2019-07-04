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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, Duration};

use crate::round::State as RoundState;
use crate::voter::{RoundData, CommunicationIn, CommunicationOut, Callback};
use tokio::timer::Delay;
use parking_lot::Mutex;
use futures::prelude::*;
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use super::{Chain, Commit, Error, Equivocation, Message, Prevote, Precommit, PrimaryPropose, SignedMessage, HistoricalVotes};

pub const GENESIS_HASH: &str = "genesis";
const NULL_HASH: &str = "NULL";

struct BlockRecord {
	number: u32,
	parent: &'static str,
}

pub struct DummyChain {
	inner: HashMap<&'static str, BlockRecord>,
	leaves: Vec<&'static str>,
	finalized: (&'static str, u32),
}

impl DummyChain {
	pub fn new() -> Self {
		let mut inner = HashMap::new();
		inner.insert(GENESIS_HASH, BlockRecord { number: 1, parent: NULL_HASH });

		DummyChain {
			inner,
			leaves: vec![GENESIS_HASH],
			finalized: (GENESIS_HASH, 1),
		}
	}

	pub fn push_blocks(&mut self, mut parent: &'static str, blocks: &[&'static str]) {
		if blocks.is_empty() { return }

		let base_number = self.inner.get(parent).unwrap().number + 1;

		if let Some(pos) = self.leaves.iter().position(|x| x == &parent) {
			self.leaves.remove(pos);
		}

		for (i, descendent) in blocks.iter().enumerate() {
			self.inner.insert(descendent, BlockRecord {
				number: base_number + i as u32,
				parent,
			});

			parent = descendent;
		}

		let new_leaf = blocks.last().unwrap();
		let new_leaf_number = self.inner.get(new_leaf).unwrap().number;

		let insertion_index = self.leaves.binary_search_by(
			|x| self.inner.get(x).unwrap().number.cmp(&new_leaf_number).reverse(),
		).unwrap_or_else(|i| i);

		self.leaves.insert(insertion_index, new_leaf);
	}

	pub fn number(&self, hash: &'static str) -> u32 {
		self.inner.get(hash).unwrap().number
	}

	pub fn last_finalized(&self) -> (&'static str, u32) {
		self.finalized.clone()
	}
}

impl Chain<&'static str, u32> for DummyChain {
	fn ancestry(&self, base: &'static str, mut block: &'static str) -> Result<Vec<&'static str>, Error> {
		let mut ancestry = Vec::new();

		loop {
			match self.inner.get(block) {
				None => return Err(Error::NotDescendent),
				Some(record) => { block = record.parent; }
			}

			if block == NULL_HASH { return Err(Error::NotDescendent) }
			if block == base { break }

			ancestry.push(block);
		}

		Ok(ancestry)
	}

	fn best_chain_containing(&self, base: &'static str) -> Option<(&'static str, u32)> {
		let base_number = self.inner.get(base)?.number;

		for leaf in &self.leaves {
			// leaves are in descending order.
			let leaf_number = self.inner.get(leaf).unwrap().number;
			if leaf_number < base_number { break }

			if leaf == &base {
				return Some((leaf, leaf_number))
			}

			if let Ok(_) = self.ancestry(base, leaf) {
				return Some((leaf, leaf_number));
			}
		}

		None
	}
}

#[derive(Hash, Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct Id(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature(pub u32);

pub struct Environment {
	chain: Mutex<DummyChain>,
	local_id: Id,
	network: Network,
	listeners: Mutex<Vec<UnboundedSender<(&'static str, u32, Commit<&'static str, u32, Signature, Id>)>>>,
}

impl Environment {
	pub fn new(network: Network, local_id: Id) -> Self {
		Environment {
			chain: Mutex::new(DummyChain::new()),
			local_id,
			network,
			listeners: Mutex::new(Vec::new()),
		}
	}

	pub fn with_chain<F, U>(&self, f: F) -> U where F: FnOnce(&mut DummyChain) -> U {
		let mut chain = self.chain.lock();
		f(&mut *chain)
	}

	/// Stream of finalized blocks.
	pub fn finalized_stream(&self) -> UnboundedReceiver<(&'static str, u32, Commit<&'static str, u32, Signature, Id>)> {
		let (tx, rx) = mpsc::unbounded();
		self.listeners.lock().push(tx);
		rx
	}
}

impl Chain<&'static str, u32> for Environment {
	fn ancestry(&self, base: &'static str, block: &'static str) -> Result<Vec<&'static str>, Error> {
		self.chain.lock().ancestry(base, block)
	}

	fn best_chain_containing(&self, base: &'static str) -> Option<(&'static str, u32)> {
		self.chain.lock().best_chain_containing(base)
	}
}

impl crate::voter::Environment<&'static str, u32> for Environment {
	type Timer = Box<Future<Item=(),Error=Error> + Send + 'static>;
	type Id = Id;
	type Signature = Signature;
	type In = Box<Stream<Item=SignedMessage<&'static str, u32, Signature, Id>,Error=Error> + Send + 'static>;
	type Out = Box<Sink<SinkItem=Message<&'static str, u32>,SinkError=Error> + Send + 'static>;
	type Error = Error;

	fn round_data(&self, round: u64) -> RoundData<Self::Id, Self::Timer, Self::In, Self::Out> {
		const GOSSIP_DURATION: Duration = Duration::from_millis(500);

		let now = Instant::now();
		let (incoming, outgoing) = self.network.make_round_comms(round, self.local_id);
		RoundData {
			voter_id: Some(self.local_id),
			prevote_timer: Box::new(Delay::new(now + GOSSIP_DURATION)
				.map_err(|_| panic!("Timer failed"))),
			precommit_timer: Box::new(Delay::new(now + GOSSIP_DURATION + GOSSIP_DURATION)
				.map_err(|_| panic!("Timer failed"))),
			incoming: Box::new(incoming),
			outgoing: Box::new(outgoing),
		}
	}

	fn round_commit_timer(&self) -> Self::Timer {
		use rand::Rng;

		const COMMIT_DELAY_MILLIS: u64 = 100;

		let delay = Duration::from_millis(
			rand::thread_rng().gen_range(0, COMMIT_DELAY_MILLIS));

		let now = Instant::now();
		Box::new(Delay::new(now + delay).map_err(|_| panic!("Timer failed")))
	}

	fn completed(
		&self,
		_round: u64,
		_state: RoundState<&'static str, u32>,
		_base: (&'static str, u32),
		_votes: &HistoricalVotes<&'static str, u32, Self::Signature, Self::Id>,
	) -> Result<(), Error> {
		Ok(())
	}

	fn finalize_block(&self, hash: &'static str, number: u32, _round: u64, commit: Commit<&'static str, u32, Signature, Id>) -> Result<(), Error> {
		let mut chain = self.chain.lock();

		if number as u32 <= chain.finalized.1 { panic!("Attempted to finalize backwards") }
		assert!(chain.ancestry(chain.finalized.0, hash).is_ok(), "Safety violation: reverting finalized block.");
		chain.finalized = (hash, number as _);
		self.listeners.lock().retain(|s| s.unbounded_send((hash, number as _, commit.clone())).is_ok());

		Ok(())
	}

	fn proposed(&self, _round: u64, _propose: PrimaryPropose<&'static str, u32>) -> Result<(), Self::Error> {
		Ok(())
	}

	fn prevoted(&self, _round: u64, _prevote: Prevote<&'static str, u32>) -> Result<(), Self::Error> {
		Ok(())
	}

	fn precommitted(&self, _round: u64, _precommit: Precommit<&'static str, u32>) -> Result<(), Self::Error> {
		Ok(())
	}

	fn prevote_equivocation(&self, round: u64, equivocation: Equivocation<Id, Prevote<&'static str, u32>, Signature>) {
		panic!("Encountered equivocation in round {}: {:?}", round, equivocation);
	}

	fn precommit_equivocation(&self, round: u64, equivocation: Equivocation<Id, Precommit<&'static str, u32>, Signature>) {
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

impl<M: Clone> BroadcastNetwork<M> {
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
	fn add_node<N, F: Fn(N) -> M>(&mut self, f: F) -> (
		impl Stream<Item=M,Error=Error>,
		impl Sink<SinkItem=N,SinkError=Error>
	) {
		let (tx, rx) = mpsc::unbounded();
		let messages_out = self.raw_sender.clone()
			.sink_map_err(|e| panic!("Error sending messages: {:?}", e))
			.with(move |message| Ok(f(message)));

		// get history to the node.
		for prior_message in self.history.iter().cloned() {
			let _ = tx.unbounded_send(prior_message);
		}

		self.senders.push(tx);
		let rx = rx.map_err(|e| panic!("Error receiving messages: {:?}", e));

		(rx, messages_out)
	}

	// do routing work
	fn route(&mut self) -> Poll<(), ()> {
		loop {
			match self.receiver.poll().map_err(|e| panic!("Error routing messages: {:?}", e))? {
				Async::NotReady => return Ok(Async::NotReady),
				Async::Ready(None) => return Ok(Async::Ready(())),
				Async::Ready(Some(item)) => {
					self.history.push(item.clone());
					for sender in &self.senders {
						let _ = sender.unbounded_send(item.clone());
					}
				}
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
		NetworkRouting { global_messages, rounds }
	)
}

type RoundNetwork = BroadcastNetwork<SignedMessage<&'static str, u32, Signature, Id>>;
type GlobalMessageNetwork = BroadcastNetwork<CommunicationIn<&'static str, u32, Signature, Id>>;

/// A test network. Instantiate this with `make_network`,
#[derive(Clone)]
pub struct Network {
	rounds: Arc<Mutex<HashMap<u64, RoundNetwork>>>,
	global_messages: Arc<Mutex<GlobalMessageNetwork>>,
}

impl Network {
	pub fn make_round_comms(&self, round_number: u64, node_id: Id) -> (
		impl Stream<Item=SignedMessage<&'static str, u32, Signature, Id>,Error=Error>,
		impl Sink<SinkItem=Message<&'static str, u32>,SinkError=Error>
	) {
		let mut rounds = self.rounds.lock();
		rounds.entry(round_number)
			.or_insert_with(RoundNetwork::new)
			.add_node(move |message| SignedMessage {
				message,
				signature: Signature(node_id.0),
				id: node_id,
			})
	}

	pub fn make_global_comms(&self) -> (
		impl Stream<Item=CommunicationIn<&'static str, u32, Signature, Id>,Error=Error>,
		impl Sink<SinkItem=CommunicationOut<&'static str, u32, Signature, Id>,SinkError=Error>
	) {
		let mut global_messages = self.global_messages.lock();
		global_messages.add_node(|message| match message {
			CommunicationOut::Commit(r, commit) => CommunicationIn::Commit(r, commit.into(), Callback::Blank),
		})
	}

	/// Send a message to all nodes.
	pub fn send_message(&self, message: CommunicationIn<&'static str, u32, Signature, Id>) {
		self.global_messages.lock().send_message(message);
	}
}

/// the network routing task.
pub struct NetworkRouting {
	rounds: Arc<Mutex<HashMap<u64, RoundNetwork>>>,
	global_messages: Arc<Mutex<GlobalMessageNetwork>>,
}

impl Future for NetworkRouting {
	type Item = ();
	type Error = ();

	fn poll(&mut self) -> Poll<(), ()> {
		let mut rounds = self.rounds.lock();
		rounds.retain(|_, round| match round.route() {
			Ok(Async::Ready(())) | Err(()) => false,
			Ok(Async::NotReady) => true,
		});

		let mut global_messages = self.global_messages.lock();
		let _ = global_messages.route();

		Ok(Async::NotReady)
	}
}
