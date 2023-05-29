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

use crate::{
	testing::{
		self,
		chain::{DummyChain, GENESIS_HASH},
		environment::{Environment, Id, Signature},
	},
	voter,
	voter::{Callback, GlobalCommunicationIncoming, Voter},
	weights::{VoteWeight, VoterWeight},
	CatchUp, Commit, Message, Precommit, Prevote, SignedMessage, SignedPrecommit, VoterSet,
};
use futures::{
	executor::LocalPool, future, future::FutureExt, stream, stream::StreamExt, task::SpawnExt,
};
use futures_timer::Delay;
use parking_lot::Mutex;
use std::{collections::BTreeSet, iter, sync::Arc, time::Duration};

// FIXME: clean up tests by using async await syntax

#[test]
fn talking_to_myself() {
	let _ = env_logger::try_init();

	let local_id = Id(5);
	let voters = VoterSet::new(std::iter::once((local_id, 100))).unwrap();

	let (network, routing_task) = testing::environment::make_network();

	let global_comms = network.make_global_comms();
	let environment = Environment::new(network, local_id);

	// initialize chain
	let last_finalized = environment.with_chain(|chain| {
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	});

	// run voter in background. scheduling it to shut down at the end.
	let finalized = environment.finalized_stream();
	let (voter, _) = futures::executor::block_on(Voter::new(
		environment.clone(),
		voters.clone(),
		global_comms,
		0,
		Vec::new(),
		last_finalized,
		last_finalized,
	))
	.start();

	let mut pool = LocalPool::new();

	pool.spawner().spawn(routing_task).unwrap();
	pool.spawner().spawn(voter.map(|v| v.expect("Error voting"))).unwrap();

	// wait for the best block to finalize.
	pool.run_until(
		finalized
			.take_while(|&(_, n, _)| future::ready(n < 6))
			.for_each(|_| future::ready(())),
	)
}

#[test]
fn finalizing_at_fault_threshold() {
	let _ = env_logger::try_init();

	// 10 voters
	let voters = VoterSet::new((0..10).map(|i| (Id(i), 1))).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();
	let mut pool = LocalPool::new();

	// 3 voters offline.
	let finalized_streams = (0..7)
		.map(|i| {
			let local_id = Id(i);
			// initialize chain
			let env = Environment::new(network.clone(), local_id);
			let last_finalized = env.with_chain(|chain| {
				chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
				chain.last_finalized()
			});

			// run voter in background. scheduling it to shut down at the end.
			let finalized = env.finalized_stream();
			let (voter, _) = futures::executor::block_on(Voter::new(
				env.clone(),
				voters.clone(),
				network.make_global_comms(),
				0,
				Vec::new(),
				last_finalized,
				last_finalized,
			))
			.start();

			pool.spawner().spawn(voter.map(|v| v.expect("Error voting"))).unwrap();

			// wait for the best block to be finalized by all honest voters
			finalized
				.take_while(|&(_, n, _)| future::ready(n < 6))
				.for_each(|_| future::ready(()))
		})
		.collect::<Vec<_>>();

	pool.spawner().spawn(routing_task.map(|_| ())).unwrap();

	pool.run_until(future::join_all(finalized_streams.into_iter()));
}

#[test]
fn multiple_rounds() {
	let _ = env_logger::try_init();

	// 10 voters
	let voters = VoterSet::new((0..10).map(|i| (Id(i), 1))).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();
	let mut pool = LocalPool::new();
	let chain = Arc::new(Mutex::new(DummyChain::new()));
	let last_finalized = {
		let mut chain = chain.lock();
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	};

	let finalized_streams = (0..10)
		.map(|i| {
			let local_id = Id(i);
			// initialize chain
			let env = Environment::new_with_chain(network.clone(), local_id, chain.clone());

			// run voter in background. scheduling it to shut down at the end.
			let finalized = env.finalized_stream();
			let (voter, _) = futures::executor::block_on(Voter::new(
				env.clone(),
				voters.clone(),
				network.make_global_comms(),
				0,
				Vec::new(),
				last_finalized,
				last_finalized,
			))
			.start();

			pool.spawner().spawn(voter.map(|v| v.expect("Error voting"))).unwrap();

			// wait for the best block to be finalized by all honest voters
			finalized
				.take_while(|&(_, n, _)| future::ready(n < 16))
				.for_each(|_| future::ready(()))
		})
		.collect::<Vec<_>>();

	let hashes = ["F", "G", "H", "I", "J", "K", "L", "M", "N", "O"];
	let mut parent = "E";
	let grow_chain = async move {
		for i in 0..10 {
			Delay::new(Duration::from_millis(100)).await;
			let mut chain = chain.lock();
			chain.push_blocks(parent, &[hashes[i]]);
			parent = hashes[i];
		}
	};

	pool.spawner().spawn(routing_task.map(|_| ())).unwrap();
	pool.spawner().spawn(grow_chain).unwrap();

	pool.run_until(future::join_all(finalized_streams.into_iter()));
}

#[test]
fn exposing_voter_state() {
	let num_voters = 10;
	let voters_online = 7;
	let voters = VoterSet::new((0..num_voters).map(|i| (Id(i), 1))).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();
	let pool = futures::executor::ThreadPool::new().unwrap();

	// some voters offline
	let (finalized_streams, mut voter_handles): (Vec<_>, Vec<_>) = (0..voters_online)
		.map(|i| {
			let local_id = Id(i);
			// initialize chain
			let environment = Environment::new(network.clone(), local_id);
			let last_finalized = environment.with_chain(|chain| {
				chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
				chain.last_finalized()
			});

			// run voter in background. scheduling it to shut down at the end.
			let finalized = environment.finalized_stream();
			let (voter, voter_handle) = futures::executor::block_on(Voter::new(
				environment,
				voters.clone(),
				network.make_global_comms(),
				0,
				Vec::new(),
				last_finalized,
				last_finalized,
			))
			.start();

			pool.spawn_ok(voter.map(|v| v.expect("Error voting")));

			(
				// wait for the best block to be finalized by all honest voters
				finalized
					.take_while(|&(_, n, _)| future::ready(n < 6))
					.for_each(|_| future::ready(())),
				voter_handle,
			)
		})
		.unzip();

	let mut voter_handle = voter_handles[0].clone();
	voter_handles.iter_mut().all(|h| {
		futures::executor::block_on(h.report_voter_state()) ==
			futures::executor::block_on(voter_handle.report_voter_state())
	});

	let expected_round_state = super::report::RoundState::<Id> {
		total_weight: VoterWeight::new(num_voters.into()).expect("nonzero"),
		threshold_weight: VoterWeight::new(voters_online.into()).expect("nonzero"),
		prevote_current_weight: VoteWeight(0),
		prevote_ids: Default::default(),
		precommit_current_weight: VoteWeight(0),
		precommit_ids: Default::default(),
	};

	assert_eq!(
		futures::executor::block_on(voter_handle.report_voter_state()).unwrap(),
		super::report::VoterState {
			background_rounds: Default::default(),
			best_round: (1, expected_round_state.clone()),
		}
	);

	pool.spawn_ok(routing_task.map(|_| ()));

	futures::executor::block_on(future::join_all(finalized_streams.into_iter()));

	assert_eq!(
		futures::executor::block_on(voter_handle.report_voter_state())
			.unwrap()
			.best_round,
		(2, expected_round_state.clone())
	);
}

#[test]
fn broadcast_commit() {
	let _ = env_logger::try_init();

	let local_id = Id(5);
	let voters = VoterSet::new([(local_id, 100)].iter().cloned()).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();
	let (commits, _) = network.make_global_comms();

	let global_comms = network.make_global_comms();
	let env = Environment::new(network, local_id);

	// initialize chain
	let last_finalized = env.with_chain(|chain| {
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	});

	// run voter in background. scheduling it to shut down at the end.
	let (voter, _) = futures::executor::block_on(Voter::new(
		env.clone(),
		voters.clone(),
		global_comms,
		0,
		Vec::new(),
		last_finalized,
		last_finalized,
	))
	.start();

	let mut pool = LocalPool::new();
	pool.spawner().spawn(voter.map(|v| v.expect("Error voting"))).unwrap();
	pool.spawner().spawn(routing_task).unwrap();

	// wait for the node to broadcast a commit message
	pool.run_until(commits.take(1).for_each(|_| future::ready(())))
}

#[test]
fn broadcast_commit_only_if_newer() {
	let _ = env_logger::try_init();

	let local_id = Id(5);
	let test_id = Id(42);
	let voters =
		VoterSet::new([(local_id, 100), (test_id, 201)].iter().cloned()).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();
	let (commits_stream, commits_sink) = network.make_global_comms();
	let (round_stream, round_sink) = network.make_round_comms(1, test_id);

	let prevote = Message::Prevote(Prevote { target_hash: "E", target_number: 6 });

	let precommit = Message::Precommit(Precommit { target_hash: "E", target_number: 6 });

	let commit = (
		1,
		Commit {
			target_hash: "E",
			target_number: 6,
			precommits: vec![SignedPrecommit {
				precommit: Precommit { target_hash: "E", target_number: 6 },
				signature: Signature(test_id.0),
				id: test_id,
			}],
		},
	);

	let global_comms = network.make_global_comms();
	let env = Environment::new(network, local_id);

	// initialize chain
	let last_finalized = env.with_chain(|chain| {
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	});

	// run voter in background. scheduling it to shut down at the end.
	let (voter, _) = futures::executor::block_on(Voter::new(
		env.clone(),
		voters.clone(),
		global_comms,
		0,
		Vec::new(),
		last_finalized,
		last_finalized,
	))
	.start();

	let mut pool = LocalPool::new();

	pool.spawner().spawn(voter.map(|v| v.expect("Error voting: {:?}"))).unwrap();

	pool.spawner().spawn(routing_task.map(|_| ())).unwrap();

	pool.spawner()
		.spawn(
			round_stream
				.into_future()
				.then(|(value, stream)| {
					// wait for a prevote
					assert!(match value {
						Some(Ok(SignedMessage {
							message: Message::Prevote(_), id: Id(5), ..
						})) => true,
						_ => false,
					});
					let votes = vec![prevote, precommit].into_iter().map(Result::Ok);
					futures::stream::iter(votes).forward(round_sink).map(|_| stream) // send our prevote
				})
				.then(|stream| {
					stream
						.take_while(|value| match value {
							// wait for a precommit
							Ok(SignedMessage {
								message: Message::Precommit(_), id: Id(5), ..
							}) => future::ready(false),
							_ => future::ready(true),
						})
						.for_each(|_| future::ready(()))
				})
				.then(move |_| {
					// send our commit
					stream::iter(iter::once(Ok(voter::GlobalCommunicationOutgoing::Commit(
						commit.0, commit.1,
					))))
					.forward(commits_sink)
				})
				.map(|_| ()),
		)
		.unwrap();

	let res = pool.run_until(
		// wait for the first commit (ours)
		commits_stream.into_future().then(|(_, stream)| {
			// the second commit should never arrive
			let await_second = stream.take(1).for_each(|_| future::ready(()));
			let delay = Delay::new(Duration::from_millis(500));
			future::select(await_second, delay)
		}),
	);

	match res {
		future::Either::Right(((), _work)) => {
			// the future timed out as expected
		},
		_ => panic!("Unexpected result"),
	}
}

#[test]
fn import_commit_for_any_round() {
	let _ = env_logger::try_init();

	let local_id = Id(5);
	let test_id = Id(42);
	let voters =
		VoterSet::new([(local_id, 100), (test_id, 201)].iter().cloned()).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();
	let (_, commits_sink) = network.make_global_comms();

	// this is a commit for a previous round
	let commit = Commit {
		target_hash: "E",
		target_number: 6,
		precommits: vec![SignedPrecommit {
			precommit: Precommit { target_hash: "E", target_number: 6 },
			signature: Signature(test_id.0),
			id: test_id,
		}],
	};

	let global_comms = network.make_global_comms();
	let env = Environment::new(network, local_id);

	// initialize chain
	let last_finalized = env.with_chain(|chain| {
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	});

	// run voter in background.
	let (voter, _) = futures::executor::block_on(Voter::new(
		env.clone(),
		voters.clone(),
		global_comms,
		1,
		Vec::new(),
		last_finalized,
		last_finalized,
	))
	.start();

	let mut pool = LocalPool::new();
	pool.spawner().spawn(voter.map(|v| v.expect("Error voting"))).unwrap();
	pool.spawner().spawn(routing_task.map(|_| ())).unwrap();

	// Send the commit message.
	pool.spawner()
		.spawn(
			stream::iter(iter::once(Ok(voter::GlobalCommunicationOutgoing::Commit(
				0,
				commit.clone(),
			))))
			.forward(commits_sink)
			.map(|_| ()),
		)
		.unwrap();

	// Wait for the commit message to be processed.
	let finalized =
		pool.run_until(env.finalized_stream().into_future().map(move |(msg, _)| msg.unwrap().2));

	assert_eq!(finalized, commit);
}

#[test]
fn skips_to_latest_round_after_catch_up() {
	let _ = env_logger::try_init();

	// 3 voters
	let voters = VoterSet::new((0..3).map(|i| (Id(i), 1u64))).expect("nonempty");
	let total_weight = voters.total_weight();
	let threshold_weight = voters.threshold();
	let voter_ids: BTreeSet<Id> = (0..3).map(|i| Id(i)).collect();

	let (network, routing_task) = testing::environment::make_network();

	// FIXME: explain why we need threadpool here
	let pool = futures::executor::ThreadPool::new().unwrap();

	pool.spawn_ok(routing_task.map(|_| ()));

	// initialize unsynced voter at round 0
	let (env, unsynced_voter, mut unsynced_voter_handle) = {
		let local_id = Id(4);

		let env = Environment::new(network.clone(), local_id);
		let last_finalized = env.with_chain(|chain| {
			chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
			chain.last_finalized()
		});

		let (voter, voter_handle) = futures::executor::block_on(Voter::new(
			env.clone(),
			voters.clone(),
			network.make_global_comms(),
			0,
			Vec::new(),
			last_finalized,
			last_finalized,
		))
		.start();

		(env, voter, voter_handle)
	};

	let pv = |id| crate::SignedPrevote {
		prevote: crate::Prevote { target_hash: "C", target_number: 4 },
		id: Id(id),
		signature: Signature(99),
	};

	let pc = |id| crate::SignedPrecommit {
		precommit: crate::Precommit { target_hash: "C", target_number: 4 },
		id: Id(id),
		signature: Signature(99),
	};

	// send in a catch-up message for round 5.
	network.send_message(GlobalCommunicationIncoming::CatchUp(
		CatchUp {
			base_number: 1,
			base_hash: GENESIS_HASH,
			round_number: 5,
			prevotes: vec![pv(0), pv(1), pv(2)],
			precommits: vec![pc(0), pc(1), pc(2)],
		},
		Callback::Blank,
	));

	let mut voter_handle = unsynced_voter_handle.clone();
	let mut get_voter_state =
		|| futures::executor::block_on(unsynced_voter_handle.report_voter_state()).unwrap();

	// spawn the voter in the background
	pool.spawn_ok(unsynced_voter.map(|_| ()));

	// wait until it's caught up, it should skip to round 6 and send a
	// finality notification for the block that was finalized by catching
	// up.
	let caught_up = async move {
		loop {
			if voter_handle.report_voter_state().await.unwrap().best_round.0 == 6 {
				break
			}

			Delay::new(Duration::from_millis(10)).await;
		}
	};
	let finalized = env.finalized_stream().take(1).into_future();

	futures::executor::block_on(caught_up.then(|_| finalized.map(|_| ())));

	assert_eq!(
		get_voter_state().best_round,
		(
			6,
			super::report::RoundState::<Id> {
				total_weight,
				threshold_weight,
				prevote_current_weight: VoteWeight(0),
				prevote_ids: Default::default(),
				precommit_current_weight: VoteWeight(0),
				precommit_ids: Default::default(),
			}
		)
	);

	assert_eq!(
		get_voter_state().background_rounds.get(&5),
		Some(&super::report::RoundState::<Id> {
			total_weight,
			threshold_weight,
			prevote_current_weight: VoteWeight(3),
			prevote_ids: voter_ids.clone(),
			precommit_current_weight: VoteWeight(3),
			precommit_ids: voter_ids,
		})
	);
}

#[test]
fn pick_up_from_prior_without_grandparent_state() {
	let _ = env_logger::try_init();

	let local_id = Id(5);
	let voters = VoterSet::new(std::iter::once((local_id, 100))).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();

	let global_comms = network.make_global_comms();
	let env = Environment::new(network, local_id);

	// initialize chain
	let last_finalized = env.with_chain(|chain| {
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	});

	// run voter in background. scheduling it to shut down at the end.
	let (voter, _) = futures::executor::block_on(Voter::new(
		env.clone(),
		voters,
		global_comms,
		10,
		Vec::new(),
		last_finalized,
		last_finalized,
	))
	.start();

	let mut pool = LocalPool::new();
	pool.spawner().spawn(voter.map(|v| v.expect("Error voting"))).unwrap();
	pool.spawner().spawn(routing_task.map(|_| ())).unwrap();

	// wait for the best block to finalize.
	pool.run_until(
		env.finalized_stream()
			.take_while(|&(_, n, _)| future::ready(n < 6))
			.for_each(|_| future::ready(())),
	)
}

#[test]
fn pick_up_from_prior_with_grandparent_state() {
	let _ = env_logger::try_init();

	let local_id = Id(99);
	let voters = VoterSet::new((0..100).map(|i| (Id(i), 1))).expect("nonempty");

	let (network, routing_task) = testing::environment::make_network();

	let global_comms = network.make_global_comms();
	let env = Environment::new(network.clone(), local_id);
	let outer_env = env.clone();

	// initialize chain
	let last_finalized = env.with_chain(|chain| {
		chain.push_blocks(GENESIS_HASH, &["A", "B", "C", "D", "E"]);
		chain.last_finalized()
	});

	let mut pool = LocalPool::new();
	let mut last_round_votes = Vec::new();

	// round 1 state on disk: 67 prevotes for "E". 66 precommits for "D". 1 precommit "E".
	// the round is completable, but the estimate ("E") is not finalized.
	for id in 0..67 {
		let prevote = Message::Prevote(Prevote { target_hash: "E", target_number: 6 });
		let precommit = if id < 66 {
			Message::Precommit(Precommit { target_hash: "D", target_number: 5 })
		} else {
			Message::Precommit(Precommit { target_hash: "E", target_number: 6 })
		};

		last_round_votes.push(SignedMessage {
			message: prevote.clone(),
			signature: Signature(id),
			id: Id(id),
		});

		last_round_votes.push(SignedMessage {
			message: precommit.clone(),
			signature: Signature(id),
			id: Id(id),
		});

		// round 2 has the same votes.
		//
		// this means we wouldn't be able to start round 3 until
		// the estimate of round-1 moves backwards.
		let (_, round_sink) = network.make_round_comms(2, Id(id));
		let msgs = stream::iter(iter::once(Ok(prevote)).chain(iter::once(Ok(precommit))));
		pool.spawner().spawn(msgs.forward(round_sink).map(|r| r.unwrap())).unwrap();
	}

	// round 1 fresh communication. we send one more precommit for "D" so the estimate
	// moves backwards.
	let sender = Id(67);
	let (_, round_sink) = network.make_round_comms(1, sender);
	let last_precommit = Message::Precommit(Precommit { target_hash: "D", target_number: 3 });
	pool.spawner()
		.spawn(
			stream::iter(iter::once(Ok(last_precommit)))
				.forward(round_sink)
				.map(|r| r.unwrap()),
		)
		.unwrap();

	// run voter in background. scheduling it to shut down at the end.
	let (voter, _) = futures::executor::block_on(Voter::new(
		env.clone(),
		voters,
		global_comms,
		1,
		last_round_votes,
		last_finalized,
		last_finalized,
	))
	.start();

	pool.spawner()
		.spawn(voter.map(|v| v.expect("Error voting")).map(|_| ()))
		.unwrap();

	pool.spawner().spawn(routing_task.map(|_| ())).unwrap();

	// wait until we see a prevote on round 3 from our local ID,
	// indicating that the round 3 has started.

	let (round_stream, _) = network.make_round_comms(3, Id(1000));
	pool.run_until(
		round_stream
			.skip_while(move |v| {
				let v = v.as_ref().unwrap();
				if let Message::Prevote(_) = v.message {
					future::ready(v.id != local_id)
				} else {
					future::ready(true)
				}
			})
			.into_future()
			.map(|_| ()),
	);

	assert_eq!(outer_env.last_completed_and_concluded(), (2, 1));
}
