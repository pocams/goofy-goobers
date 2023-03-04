use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use goofy_goobers::error::{Error, ErrorCode};

use goofy_goobers::message::Envelope;

const SEQ_KV: &str = "seq-kv";
const KV_KEY: &str = "total";

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Topology { topology: HashMap<String, Vec<String>> },
    TopologyOk,
    Add { delta: u64 },
    AddOk,
    // read and read_ok are used by both the workload and the seq-kv store, but key is only used by seq-kv
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

    Error {
        code: u64,
        text: String
    },
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
    let mut my_node_id: String = Default::default();
    let mut all_node_ids: Vec<String> = Default::default();
    let mut to_add: u64 = 0;
    let mut value: u64 = 0;
    let mut last_cas_to: u64 = 0;
    let mut last_cas_id: usize = 0;
    let mut cas_outstanding: bool = false;

    let (incoming_sender, incoming_receiver) = mpsc::channel();
    thread::spawn(move || read_stdin(incoming_sender));

    loop {
        match incoming_receiver.recv_timeout(Duration::from_millis(1000)) {
            Ok(env) => {
                match env.message() {
                    Message::Init { node_id, node_ids } => {
                        my_node_id = node_id.clone();
                        all_node_ids = node_ids.clone();
                        dispatch_message(&env.reply(Message::InitOk));

                        // Initialize the counter in the kv store
                        let e = Envelope::new(my_node_id.clone(), SEQ_KV.to_string(), None,
                                                   Message::Cas { key: KV_KEY.to_string(), from: 0, to: 0, create_if_not_exists: Some(true) });
                        dispatch_message(&e);
                        cas_outstanding = true;
                        last_cas_id = e.msg_id().unwrap();
                    }

                    Message::Topology { .. } => {
                        dispatch_message(&env.reply(Message::TopologyOk));
                    }

                    Message::Add { delta } => {
                        to_add += *delta;
                        eprintln!("delta {}; to-add {}", delta, to_add);
                        dispatch_message(&env.reply(Message::AddOk));
                    }

                    Message::Read { .. } => {
                        dispatch_message(&env.reply(Message::ReadOk { value }));
                    }

                    Message::ReadOk { value: new_value } => {
                        if env.is_from_node() {
                            eprintln!("read (from {}) ok: {}", env.src, new_value);
                            if *new_value > value { value = *new_value }
                        } else {
                            eprintln!("read ok: {}", new_value);
                            value = *new_value
                        }
                    }

                    Message::CasOk => {
                        if env.in_reply_to().unwrap() == last_cas_id {
                            eprintln!("cas ok: {env:?} ({value} + {to_add})");
                            to_add = 0;
                            value = last_cas_to;
                            last_cas_id = 0;
                            cas_outstanding = false;
                        } else {
                            eprintln!("unexpected cas ok: {env:?} ({value} + {to_add})");
                        }
                    }

                    Message::Error { code, text } => {
                        let e = Error { code: ErrorCode::from(*code), text: text.clone() };
                        eprintln!("error: {e:?}");
                        if e.code == ErrorCode::PreconditionFailed {
                            // Our last CAS failed because the "from" value was out of date
                            cas_outstanding = false;
                            let e = Envelope::new(my_node_id.clone(), SEQ_KV.to_string(), None,
                                                         Message::Read { key: Some(KV_KEY.to_string()) });
                            eprintln!("read: {e:?}");
                            dispatch_message(&e);
                        } else {
                            panic!("Unexpected error {e:?}");
                        }
                    }

                    _ => unimplemented!()
                }
            }

            Err(RecvTimeoutError::Timeout) => {
                if to_add == 0 {
                    for node in &all_node_ids {
                        if node != &my_node_id {
                            let e = Envelope::new(my_node_id.clone(), node.to_string(), None,
                                                         Message::Read { key: None });
                            eprintln!("node read: {e:?}");
                            dispatch_message(&e);
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => {}
        }

        if to_add != 0 && !cas_outstanding {
            last_cas_to = value + to_add;
            let e = Envelope::new(my_node_id.clone(), SEQ_KV.to_string(), None,
                                         Message::Cas { key: KV_KEY.to_string(), from: value, to: last_cas_to, create_if_not_exists: None });
            eprintln!("cas: {e:?}");
            dispatch_message(&e);
            last_cas_id = e.msg_id().unwrap();
            cas_outstanding = true;
        }
    }
}
