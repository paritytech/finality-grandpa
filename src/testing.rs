// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of finality-grandpa.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with finality-grandpa. If not, see <http://www.gnu.org/licenses/>.

//! Helpers for testing

use std::collections::HashMap;
use std::sync::Arc;

use round::State as RoundState;
use voter::RoundData;
use tokio::timer::Delay;
use parking_lot::Mutex;
use futures::prelude::*;
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use super::{Chain, Error, Equivocation, Message, Prevote, Precommit, SignedMessage};

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
		use std::cmp::Ord;

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

impl Chain<&'static str> for DummyChain {
	fn ancestry(&self, base: &'static str, mut block: &'static str) -> Result<Vec<&'static str>, Error> {
		let mut ancestry = Vec::new();

		loop {
			match self.inner.get(block) {
				None => return Err(Error::NotDescendent),
				Some(record) => { block = record.parent; }
			}

			ancestry.push(block);

			if block == NULL_HASH { return Err(Error::NotDescendent) }
			if block == base { break }
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

#[derive(Hash, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Id(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature(u32);

pub struct Environment {
	chain: Mutex<DummyChain>,
	voters: HashMap<Id, usize>,
	local_id: Id,
	network: Network,
	listeners: Mutex<Vec<UnboundedSender<(&'static str, u32)>>>,
}

impl Environment {
	pub fn new(voters: HashMap<Id, usize>, network: Network, local_id: Id) -> Self {
		Environment {
			chain: Mutex::new(DummyChain::new()),
			voters,
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
	pub fn finalized_stream(&self) -> UnboundedReceiver<(&'static str, u32)> {
		let (tx, rx) = mpsc::unbounded();
		self.listeners.lock().push(tx);
		rx
	}
}

impl Chain<&'static str> for Environment {
	fn ancestry(&self, base: &'static str, block: &'static str) -> Result<Vec<&'static str>, Error> {
		self.chain.lock().ancestry(base, block)
	}

	fn best_chain_containing(&self, base: &'static str) -> Option<(&'static str, u32)> {
		self.chain.lock().best_chain_containing(base)
	}
}

impl ::voter::Environment<&'static str> for Environment {
	type Timer = Box<Future<Item=(),Error=Error> + Send + 'static>;
	type Id = Id;
	type Signature = Signature;
	type In = Box<Stream<Item=SignedMessage<&'static str, Signature, Id>,Error=Error> + Send + 'static>;
	type Out = Box<Sink<SinkItem=Message<&'static str>,SinkError=Error> + Send + 'static>;
	type Error = Error;

	fn round_data(&self, round: u64) -> RoundData<Self::Timer, Self::Id, Self::In, Self::Out> {
		use std::time::{Instant, Duration};
		const GOSSIP_DURATION: Duration = Duration::from_millis(500);

		let now = Instant::now();
		let (incoming, outgoing) = self.network.make_round_comms(round, self.local_id);
		RoundData {
			prevote_timer: Box::new(Delay::new(now + GOSSIP_DURATION)
				.map_err(|_| panic!("Timer failed"))),
			precommit_timer: Box::new(Delay::new(now + GOSSIP_DURATION + GOSSIP_DURATION)
				.map_err(|_| panic!("Timer failed"))),
			voters: self.voters.clone(),
			incoming: Box::new(incoming),
			outgoing: Box::new(outgoing),
		}
	}

	fn completed(&self, _round: u64, _state: RoundState<&'static str>) { }

	fn finalize_block(&self, hash: &'static str, number: u32) {
		let mut chain = self.chain.lock();

		if number as u32 <= chain.finalized.1 { panic!("Attempted to finalize backwards") }
		assert!(chain.ancestry(chain.finalized.0, hash).is_ok(), "Safety violation: reverting finalized block.");
		chain.finalized = (hash, number as _);
		self.listeners.lock().retain(|s| s.unbounded_send((hash, number as _)).is_ok());
	}

	fn prevote_equivocation(&self, round: u64, equivocation: Equivocation<Id, Prevote<&'static str>, Signature>) {
		panic!("Encountered equivocation in round {}: {:?}", round, equivocation);
	}

	fn precommit_equivocation(&self, round: u64, equivocation: Equivocation<Id, Precommit<&'static str>, Signature>) {
		panic!("Encountered equivocation in round {}: {:?}", round, equivocation);
	}
}

// p2p network data for a round.
struct RoundNetwork {
	receiver: UnboundedReceiver<SignedMessage<&'static str, Signature, Id>>,
	raw_sender: UnboundedSender<SignedMessage<&'static str, Signature, Id>>,
	senders: Vec<UnboundedSender<SignedMessage<&'static str, Signature, Id>>>,
	history: Vec<SignedMessage<&'static str, Signature, Id>>,
}

impl RoundNetwork {
	fn new() -> Self {
		let (tx, rx) = mpsc::unbounded();
		RoundNetwork {
			receiver: rx,
			raw_sender: tx,
			senders: Vec::new(),
			history: Vec::new(),
		}
	}

	// add a node to the network for a round.
	fn add_node(&mut self, id: Id) -> (
		impl Stream<Item=SignedMessage<&'static str, Signature, Id>,Error=Error>,
		impl Sink<SinkItem=Message<&'static str>,SinkError=Error>
	) {
		let (tx, rx) = mpsc::unbounded();
		let messages_out = self.raw_sender.clone()
			.sink_map_err(|e| panic!("Error sending messages: {:?}", e))
			.with(move |message| Ok(SignedMessage {
				message,
				signature: Signature(id.0),
				id: id,
			}));

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
	let rounds = Arc::new(Mutex::new(HashMap::new()));
	(
		Network { rounds: rounds.clone() },
		NetworkRouting { rounds }
	)
}

/// A test network. Instantiate this with `make_network`,
#[derive(Clone)]
pub struct Network {
	rounds: Arc<Mutex<HashMap<u64, RoundNetwork>>>,
}

impl Network {
	fn make_round_comms(&self, round_number: u64, node_id: Id) -> (
		impl Stream<Item=SignedMessage<&'static str, Signature, Id>,Error=Error>,
		impl Sink<SinkItem=Message<&'static str>,SinkError=Error>
	) {
		let mut rounds = self.rounds.lock();
		rounds.entry(round_number).or_insert_with(RoundNetwork::new).add_node(node_id)
	}
}

/// the network routing task.
pub struct NetworkRouting {
	rounds: Arc<Mutex<HashMap<u64, RoundNetwork>>>,
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

		Ok(Async::NotReady)
	}
}
