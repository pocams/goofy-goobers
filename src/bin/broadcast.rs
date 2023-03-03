use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io::{BufRead, BufWriter, Stderr, Write};
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::{io, mem, thread};
use once_cell::sync::OnceCell;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use goofy_goobers::message::Envelope;

const RESEND_AFTER: Duration = Duration::from_millis(200);

fn ms() -> u64 {
    static START: OnceCell<Instant> = OnceCell::new();
    START.get_or_init(|| Instant::now()).elapsed().as_millis() as u64
}

fn log() -> Arc<Mutex<impl Write>> {
    static STDERR: OnceCell<Arc<Mutex<BufWriter<Stderr>>>> = OnceCell::new();
    STDERR.get_or_init(|| Arc::new(Mutex::new(BufWriter::with_capacity(1024*1024, io::stderr())))).clone()
}

#[derive(Debug)]
struct MessageInFlight<B> where B: Debug {
    message: Envelope<B>,
    sent_at: Instant
}

impl<B: Debug> MessageInFlight<B> {
    pub fn from_message(message: Envelope<B>) -> MessageInFlight<B> {
        MessageInFlight {
            message,
            sent_at: Instant::now()
        }
    }

    pub fn is_ready_for_resend(&self) -> bool {
        Instant::now() - self.sent_at > RESEND_AFTER
    }
}

struct NodeHandler<B: Debug> {
    node: String,
    messages_in_flight: Vec<MessageInFlight<B>>,
    last_message_received: Instant,
    dispatch_message: fn(&Envelope<B>) -> ()
}

impl<B: Debug> NodeHandler<B> {
    fn new(node: String, dispatch_message: fn(&Envelope<B>) -> ()) -> NodeHandler<B> {
        NodeHandler {
            node,
            messages_in_flight: Default::default(),
            last_message_received: Instant::now(),
            dispatch_message
        }
    }

    fn send_message(&mut self, message: Envelope<B>) {
        if message.dest != self.node {
            panic!("Message {:?} not for node {:?}", message, self.node);
        }
        (self.dispatch_message)(&message);
        self.messages_in_flight.push(MessageInFlight::from_message(message));
    }

    fn handle_incoming_message(&mut self, message: &Envelope<B>) where B: Debug {
        if message.src != self.node {
            panic!("Message {:?} not from node {:?}", message, self.node);
        }

        self.last_message_received = Instant::now();

        if let Some(in_reply_to) = message.in_reply_to() {
            // In-flight messages always have a message ID, but we might have already
            // received this ack and removed the message from our list
            if let Some(mif) = self.messages_in_flight.iter().position(|m| m.message.msg_id().unwrap() == in_reply_to) {
                self.messages_in_flight.remove(mif);
            }
        }
    }

    pub fn process_resends(&mut self) {
        let Some(newest) = self.messages_in_flight.iter().max_by_key(|m| m.sent_at) else { return };

        if newest.is_ready_for_resend() {
            if self.last_message_received > newest.sent_at {
                // The node is responding now, resend all our messages
                let mif = mem::take(&mut self.messages_in_flight);
                for m in mif {
                    writeln!(log().lock().unwrap(), "[{}] (resend, flushing) {:?}", ms(), m).unwrap();
                    self.send_message(m.message)
                };
            } else {
                // If the most recent message we sent hasn't yet been acknowledged,
                // resend the oldest outstanding message.
                let m = self.messages_in_flight.remove(0);
                writeln!(log().lock().unwrap(), "[{}] (resend, testing) {:?}", ms(), m).unwrap();
                self.send_message(m.message);
            }
        }
    }

}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Broadcast {
        message: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        seen_at_nodes: Option<Vec<String>>,
    },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<u64> },
    Topology {
        topology: HashMap<String, Vec<String>>
    },
    TopologyOk
}

fn dispatch_message(message: &Envelope<Message>) {
    writeln!(log().lock().unwrap(), "[{}] >> {:?}", ms(), message).unwrap();
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, message).unwrap();
    stdout.write(b"\n").unwrap();
    stdout.flush().unwrap();
}

fn read_stdin<B: Debug + DeserializeOwned>(incoming_messages: Sender<Envelope<B>>) {
    for line in std::io::stdin().lock().lines().map(Result::unwrap) {
        writeln!(log().lock().unwrap(), "[stdin] {}", line).unwrap();
        let env = serde_json::from_str(&line).unwrap();
        incoming_messages.send(env).unwrap();
    }
}

fn main() {
    let mut my_node_id = Default::default();
    let mut all_node_ids = Default::default();
    let mut node_topology = Default::default();

    let mut messages = HashSet::new();

    let mut node_handlers: HashMap<String, NodeHandler<Message>> = HashMap::new();

    let (incoming_sender, incoming_receiver) = mpsc::channel();
    thread::spawn(move || read_stdin(incoming_sender));

    loop {
        match incoming_receiver.recv_timeout(RESEND_AFTER / 2) {
            Ok(env) => {
                writeln!(log().lock().unwrap(), "[{}] << {:?}", ms(), env).unwrap();
                if env.is_from_node() {
                    node_handlers.get_mut(&env.src).unwrap().handle_incoming_message(&env);
                }

                match env.message() {
                    Message::Init { node_id, node_ids } => {
                        my_node_id = node_id.clone();
                        all_node_ids = node_ids.clone();
                        for node_id in all_node_ids {
                            node_handlers.insert(node_id.clone(), NodeHandler::new(node_id.clone(), dispatch_message));
                        }

                        dispatch_message(&env.reply(Message::InitOk));
                    }

                    Message::Topology { topology } => {
                        node_topology = topology.clone();
                        dispatch_message(&env.reply(Message::TopologyOk));
                    }

                    Message::Broadcast { message, seen_at_nodes } => {
                        messages.insert(*message);

                        let mut seen_at_nodes = seen_at_nodes.as_ref().map(|v| v.clone()).unwrap_or_default();
                        if !seen_at_nodes.contains(&my_node_id) {
                            seen_at_nodes.push(my_node_id.clone());
                        }

                        for neighbour in node_topology.get(&my_node_id).unwrap() {
                            if !seen_at_nodes.contains(neighbour) {
                                let e = Envelope::new(my_node_id.clone(), neighbour.clone(), None,
                                                      Message::Broadcast { message: *message, seen_at_nodes: Some(seen_at_nodes.clone()) });
                                node_handlers.get_mut(neighbour).unwrap().send_message(e);
                            }
                        }
                        dispatch_message(&env.reply(Message::BroadcastOk));
                    }

                    Message::BroadcastOk => {}

                    Message::Read => {
                        dispatch_message(&env.reply(Message::ReadOk { messages: messages.iter().copied().collect() }));
                    }

                    _ => unimplemented!()
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                writeln!(log().lock().unwrap(), "@@@ timeout").unwrap();
            }
            Err(RecvTimeoutError::Disconnected) => {
                // Just spin in the loop while we process the rest of our resends
                writeln!(log().lock().unwrap(), "@@@ disconnected").unwrap();
                thread::sleep(RESEND_AFTER / 2);
            }
        }

        for h in node_handlers.values_mut() {
            h.process_resends();
        }
    }
}
