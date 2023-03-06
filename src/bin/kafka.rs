use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::{panic, process, thread};
use std::thread::JoinHandle;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use goofy_goobers::error::{Error, ErrorCode};
use goofy_goobers::message::Envelope;

const KV_ADDRESS: &str = "seq-kv";
const XID_KEY: &str = "xid";

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Topology { topology: HashMap<String, Vec<String>> },
    TopologyOk,

    // KV store messages
    Read {
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<String>
    },
    ReadOk { value: u64 },
    Write { key: String, value: u64 },
    WriteOk,
    Cas {
        key: String,
        from: u64,
        to: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        create_if_not_exists: Option<bool>,
    },
    CasOk,

    // Workload messages
    Send { key: String, msg: u64 },
    SendOk { offset: u64 },
    Poll { offsets: HashMap<String, u64> },
    PollOk { msgs: HashMap<String, Vec<(u64, u64)>> },
    CommitOffsets { offsets: HashMap<String, u64> },
    CommitOffsetsOk,
    ListCommittedOffsets { keys: Vec<String> },
    ListCommittedOffsetsOk { offsets: HashMap<String, u64> },

    Error {
        code: u64,
        text: String
    },
}

struct InputHandler;

struct InputHandlerHandle<B: Clone + Debug + Send> {
    new_subscriber_sender: Sender<Sender<Envelope<B>>>
}

impl<B: Clone + Debug + Send> InputHandlerHandle<B> {
    fn new_receiver(&self) -> Receiver<Envelope<B>> {
        let (sender, receiver) = channel();
        self.new_subscriber_sender.send(sender).unwrap();
        receiver
    }
}

impl InputHandler {
    pub fn start<B: Clone + Debug + Send + DeserializeOwned + 'static>(mut subscribers: Vec<Sender<Envelope<B>>>) -> InputHandlerHandle<B> {
        let (new_subscriber_sender, new_subscriber_receiver) = channel();

        thread::spawn(move || {
            loop {
                for line in std::io::stdin().lock().lines().map(Result::unwrap) {
                    while let Ok(r) = new_subscriber_receiver.try_recv() {
                        subscribers.push(r);
                    };

                    let env: Envelope<B> = serde_json::from_str(&line).unwrap();
                    for subscriber in subscribers.iter() {
                        let _ = subscriber.send(env.clone());
                    }
                }
            }
        });

        InputHandlerHandle { new_subscriber_sender }
    }
}

struct OutputHandler;

impl OutputHandler {
    fn start<B: Debug + Serialize + Send + 'static>() -> Sender<Envelope<B>> {
        let (sender, receiver) = channel();

        thread::spawn(move || {
            let mut stdout = std::io::stdout().lock();
            for envelope in receiver {
                serde_json::to_writer(&mut stdout, &envelope).unwrap();
                stdout.write(b"\n").unwrap();
                stdout.flush().unwrap();
            }
        });

        sender
    }
}

#[derive(Clone)]
struct XidRequester {
    request_sender: Sender<Sender<usize>>
}

impl XidRequester {
    fn get_xid(&mut self) -> usize {
        let (sender, receiver) = channel();
        self.request_sender.send(sender).unwrap();
        receiver.recv().unwrap()
    }
}

struct XidAssigner {
    local_node: String,
    incoming: Receiver<Envelope<Message>>,
    outgoing: Sender<Envelope<Message>>,
    request_receiver: Receiver<Sender<usize>>,
    last_seen_xid: usize,
}

impl XidAssigner {
    // This only allows a single outstanding request at a time - that may need
    // to be optimized later to handle high latency
    pub fn start(local_node: String, incoming: Receiver<Envelope<Message>>, outgoing: Sender<Envelope<Message>>) -> XidRequester {
        let (request_sender, request_receiver) = channel();
        let mut assigner = XidAssigner {
            local_node,
            incoming,
            outgoing,
            request_receiver,
            last_seen_xid: 0
        };

        thread::spawn(move || {
            assigner.initialize_xid();
            loop {
                let response_channel = assigner.request_receiver.recv().unwrap();
                response_channel.send(assigner.generate_xid()).unwrap()
            }
        });

        XidRequester { request_sender }
    }

    fn initialize_xid(&mut self) {
        let e = Envelope::new(self.local_node.clone(), KV_ADDRESS.to_string(), None,
        Message::Cas { key: XID_KEY.to_string(), from: 0, to: 0, create_if_not_exists: Some(true) });
        self.outgoing.send(e).unwrap();
        for env in self.incoming.iter() {
            if env.src == KV_ADDRESS {
                match env.message() {
                    Message::CasOk => {
                        self.last_seen_xid = 0;
                        return;
                    },
                    Message::Error { code, text} if *code == ErrorCode::PreconditionFailed as u64 => {
                        // If we can't initialize it to 0, it must already have been initialized (and incremented)
                        eprintln!("initialize_xid: {text}");
                        self.last_seen_xid = self.fetch_last_xid();
                        return;
                    },
                    _ => panic!("initialize_xid: unexpected message {env:?}"),
                }
            }
        }
    }

    fn try_cas(&mut self) -> Option<usize> {
        let possible_xid = self.last_seen_xid + 1;
        let e = Envelope::new(self.local_node.clone(), KV_ADDRESS.to_string(), None,
        Message::Cas { key: XID_KEY.to_string(), from: self.last_seen_xid as u64, to: possible_xid as u64, create_if_not_exists: None });
        self.outgoing.send(e).unwrap();
        for env in self.incoming.iter() {
            if env.src == KV_ADDRESS {
                return match env.message() {
                    Message::CasOk => {
                        self.last_seen_xid = possible_xid;
                        Some(possible_xid)
                    },
                    Message::Error { code, text} if *code == ErrorCode::PreconditionFailed as u64 => {
                        eprintln!("try_cas: {text}");
                        None
                    },
                    _ => panic!("Expected cas_ok but got {env:?}"),
                }
            }
        }
        panic!("Incoming channel closed while waiting for cas_ok");
    }

    fn fetch_last_xid(&mut self) -> usize {
        let e = Envelope::new(self.local_node.clone(), KV_ADDRESS.to_string(), None,
        Message::Read { key: Some(XID_KEY.to_string()) });
        self.outgoing.send(e).unwrap();
        for env in self.incoming.iter() {
            if env.src == KV_ADDRESS {
                return match env.message() {
                    Message::ReadOk { value } => *value as usize,
                    _ => panic!("Expected read_ok but got {env:?}"),
                }
            }
        }
        panic!("Incoming channel closed while waiting for read_ok");
    }

    fn generate_xid(&mut self) -> usize {
        loop {
            if let Some(xid) = self.try_cas() {
                return xid
            } else {
                eprintln!("generate_xid: got error, retrying");
                self.last_seen_xid = self.fetch_last_xid()
            }
        }
    }
}

fn main() {
    // https://stackoverflow.com/questions/35988775/how-can-i-cause-a-panic-on-a-thread-to-immediately-end-the-main-thread
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        process::exit(1);
    }));


    let output_sender = OutputHandler::start::<Message>();
    let (main_sender, main_receiver) = channel();
    let input_handler: InputHandlerHandle<Message> = InputHandler::start::<Message>(vec![main_sender]);
    let mut local_node = Default::default();

    for envelope in main_receiver.iter() {
        match envelope.message() {
            Message::Init { node_id, node_ids } => {
                eprintln!("init: {:?}", envelope);
                local_node = node_id.clone();
                output_sender.send(envelope.reply(Message::InitOk)).unwrap();
                break;
            }
            // Message::Topology { .. } => {
            //     eprintln!("topology: {:?}", envelope);
            //     output_sender.send(envelope.reply(Message::TopologyOk)).unwrap();
            // },
            _ => panic!("Unexpected message at init time: {envelope:?}")
        }
    }

    let mut xid = XidAssigner::start(local_node, input_handler.new_receiver(), output_sender.clone());

    eprintln!("x1 {:?}", xid.get_xid());
    eprintln!("x2 {:?}", xid.get_xid());
    eprintln!("x3 {:?}", xid.get_xid());
    eprintln!("x4 {:?}", xid.get_xid());

    for envelope in main_receiver.iter() {
        if envelope.src == KV_ADDRESS { continue }
        match envelope.message() {
            Message::Topology { .. } => {
                eprintln!("topology: {:?}", envelope);
                output_sender.send(envelope.reply(Message::TopologyOk)).unwrap();
            },
            Message::Send { key, msg } => {}
            Message::Poll { offsets } => {}
            Message::CommitOffsets { offsets } => {}
            Message::ListCommittedOffsets { keys } => {}
            _ => panic!("Unexpected message at runtime: {envelope:?}")
        }
    }
}
