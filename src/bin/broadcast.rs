use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use goofy_goobers::message::Envelope;


const SYNC_INTERVAL: Duration = Duration::from_millis(250);

// Problem 3d
// const FANOUT: usize = 2;
// Problem 3e
const FANOUT: usize = 4;

struct NodeHandler {
    unacked_messages: Vec<u64>,
}

impl NodeHandler {
    fn new() -> NodeHandler {
        NodeHandler {
            unacked_messages: Default::default(),
        }
    }

    fn send_message(&mut self, message: u64) {
        self.unacked_messages.push(message);
    }

    pub fn sync_ok(&mut self, messages: &Vec<u64>) {
        self.unacked_messages.retain(|m| !messages.contains(m));
        eprintln!("acked {:?}, left {:?}", messages, self.unacked_messages);
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Broadcast {
        message: u64,
    },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<u64> },
    Topology {
        topology: HashMap<String, Vec<String>>
    },
    TopologyOk,
    Sync { messages: Vec<u64> },
    SyncOk { messages: Vec<u64> }
}

fn dispatch_message(message: &Envelope<Message>) {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, message).unwrap();
    stdout.write(b"\n").unwrap();
    stdout.flush().unwrap();
}

fn read_stdin<B: Debug + DeserializeOwned>(incoming_messages: Sender<Envelope<B>>) {
    for line in std::io::stdin().lock().lines().map(Result::unwrap) {
        let env = serde_json::from_str(&line).unwrap();
        incoming_messages.send(env).unwrap();
    }
}

fn main() {
    let mut my_node_id = Default::default();
    let mut node_topology: HashMap<String, Vec<String>> = Default::default();

    let mut messages = HashSet::new();

    let mut node_handlers: HashMap<String, NodeHandler> = HashMap::new();

    let (incoming_sender, incoming_receiver) = mpsc::channel();
    thread::spawn(move || read_stdin(incoming_sender));

    let mut deadline = Instant::now() + SYNC_INTERVAL;

    loop {
        match incoming_receiver.recv_timeout(deadline - Instant::now()) {
            Ok(env) => {
                // if env.is_from_node() {
                //     node_handlers.get_mut(&env.src).unwrap().handle_incoming_message(&env);
                // }

                match env.message() {
                    Message::Init { node_id, node_ids } => {
                        my_node_id = node_id.clone();
                        for (idx, node_id) in node_ids.iter().enumerate() {
                            node_handlers.insert(node_id.clone(), NodeHandler::new());
                            node_topology.insert(node_id.clone(), node_ids.iter().skip((idx + 1) % FANOUT).step_by(FANOUT).cloned().collect());
                        }
                        eprintln!("generated topology: {:?}", node_topology);

                        dispatch_message(&env.reply(Message::InitOk));
                    }

                    Message::Topology { .. } => {
                        // node_topology = topology.clone();
                        dispatch_message(&env.reply(Message::TopologyOk));
                    }

                    Message::Broadcast { message } => {
                        if messages.insert(*message) {
                            for neighbour in node_topology.get(&my_node_id).unwrap() {
                                node_handlers.get_mut(neighbour).unwrap().send_message(*message);
                            }
                        }

                        dispatch_message(&env.reply(Message::BroadcastOk));
                    }

                    Message::BroadcastOk => {}

                    Message::Sync { messages: incoming_messages } => {
                        for message in incoming_messages {
                            if messages.insert(*message) {
                                for neighbour in node_topology.get(&my_node_id).unwrap() {
                                    node_handlers.get_mut(neighbour).unwrap().send_message(*message);
                                }
                            }
                        }
                        dispatch_message(&env.reply(Message::SyncOk { messages: incoming_messages.clone() }));
                    }

                    Message::SyncOk { messages: acked_messages } => {
                        eprintln!("sync_ok from {}", env.src);
                        node_handlers.get_mut(&env.src).unwrap().sync_ok(acked_messages);
                    }

                    Message::Read => {
                        dispatch_message(&env.reply(Message::ReadOk { messages: messages.iter().copied().collect() }));
                    }

                    _ => unimplemented!()
                }
            }

            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {}
        }

        if Instant::now() >= deadline {
            for (remote_node, handler) in node_handlers.iter() {
                if !handler.unacked_messages.is_empty() {
                    eprintln!("to {}: {:?}", remote_node, handler.unacked_messages);
                    dispatch_message(&Envelope::new(my_node_id.clone(), remote_node.clone(), None,
                                                           Message::Sync { messages: handler.unacked_messages.clone() }));
                }
            }
            deadline = deadline + SYNC_INTERVAL;
        }
    }
}
