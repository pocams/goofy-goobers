use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::io::{BufRead, Write};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::{panic, process, thread};
use std::char::ParseCharError;
use std::cmp::Ordering;
use std::sync::{Arc, atomic, Mutex};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeSeq;
use goofy_goobers::error::ErrorCode;
use goofy_goobers::message::Envelope;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(try_from="char", into="char")]
enum OpType {
    Read,
    Write,
}

struct ParseError;

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "optype parse error")
    }
}

impl TryFrom<char> for OpType {
    type Error = ParseError;

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            'r' => Ok(OpType::Read),
            'w' => Ok(OpType::Write),
            _ => Err(ParseError)
        }
    }
}

impl Into<char> for OpType {
    fn into(self) -> char {
        match self {
            OpType::Read => 'r',
            OpType::Write => 'w',
        }
    }
}

#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
struct Operation {
    optype: OpType,
    key: u64,
    value: Option<u64>,
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element(&self.optype)?;
        seq.serialize_element(&self.key)?;
        seq.serialize_element(&self.value)?;
        seq.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct Transaction {
    node: String,
    transaction_id: usize,
    operations: Vec<Operation>,
}

impl PartialOrd<Self> for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.transaction_id.partial_cmp(&other.transaction_id)
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Topology { topology: HashMap<String, Vec<String>> },
    TopologyOk,

    Txn {
        #[serde(rename="txn")]
        operations: Vec<Operation>
    },
    TxnOk {
        #[serde(rename="txn")]
        operations: Vec<Operation>
    },

    // Node to node messages
    Transactions { transactions: Vec<Transaction>},
    PollTransactions { first_xid: usize },

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
                    // eprintln!("{}", line);
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

fn roll_up_transactions(transactions: &Vec<Transaction>) -> HashMap<u64, u64> {
    let mut values = HashMap::new();
    for txn in transactions {
        for operation in &txn.operations {
            if operation.optype == OpType::Write {
                values.insert(operation.key, operation.value.unwrap());
            }
        }
    }
    values
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
    let mut other_nodes = Vec::new();
    // Doesn't actually need to be atomic but what the heck
    let mut local_xid = AtomicUsize::new(0);

    for envelope in main_receiver.iter() {
        match envelope.message() {
            Message::Init { node_id, node_ids } => {
                eprintln!("init: {:?}", envelope);
                local_node = node_id.clone();
                other_nodes.extend(node_ids.into_iter().filter(|n| **n != local_node).cloned());
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

    let mut node_transactions: Arc<Mutex<HashMap<String, Vec<Transaction>>>> = Default::default();

    if !other_nodes.is_empty() {
        let local_node = local_node.clone();
        let other_nodes = other_nodes.clone();
        let node_transactions = node_transactions.clone();
        let sender = output_sender.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(1000));
            for other_node in &other_nodes {
                let poll = Envelope::new(local_node.clone(), other_node.clone(), None, Message::PollTransactions {
                    first_xid: node_transactions.lock().unwrap()
                        .get(other_node)
                        .map(|txns| txns.iter().max_by_key(|txn| txn.transaction_id).map(|txn| txn.transaction_id).unwrap_or(0))
                        .unwrap_or(0),
                });
                sender.send(poll).unwrap();
            }
        });
    }

    for envelope in main_receiver.iter() {
        match envelope.message() {
            Message::Topology { .. } => {
                eprintln!("topology: {:?}", envelope);
                output_sender.send(envelope.reply(Message::TopologyOk)).unwrap();
            },

            Message::Txn { operations } => {
                let mut node_transactions = node_transactions.lock().unwrap();

                // TODO: do we have to merge in our own txns last?
                let mut state = node_transactions.values()
                    .map(|v| roll_up_transactions(v))
                    .reduce(|rollup, elem| rollup.into_iter().chain(elem).collect())
                    .unwrap_or_default();

                // Fill in the reads
                let mut filled_in_operations: Vec<Operation> = Default::default();
                for op in operations {
                    filled_in_operations.push(match op.optype {
                        OpType::Read => {
                            Operation {
                                optype: OpType::Read,
                                key: op.key,
                                value: state.get(&op.key).copied(),
                            }
                        }
                        OpType::Write => {
                            state.insert(op.key, op.value.unwrap());
                            op.to_owned()
                        }
                    });
                }

                let txn = Transaction {
                    node: local_node.clone(),
                    transaction_id: local_xid.fetch_add(1, atomic::Ordering::SeqCst),
                    operations: filled_in_operations.clone(),
                };

                node_transactions.entry(local_node.to_string()).or_default().push(txn.clone());

                // Broadcast the transaction to other nodes
                let transactions = vec![txn];
                for other_node in &other_nodes {
                    output_sender.send(Envelope::new(local_node.clone(), (*other_node).clone(), None, Message::Transactions { transactions: transactions.clone() })).unwrap();
                }

                output_sender.send(envelope.reply(Message::TxnOk { operations: filled_in_operations })).unwrap();
            }

            Message::Transactions { transactions } => {
                // FIXME: optimize
                // eprintln!("incoming txns: {transactions:?}");
                let mut node_transactions = node_transactions.lock().unwrap();
                for new_txn in transactions {
                    let txn_is_known = node_transactions.get(&new_txn.node)
                        .map(|txns| txns.iter().any(|committed_txn| committed_txn.transaction_id == new_txn.transaction_id))
                        .unwrap_or(false);
                    if !txn_is_known {
                        let node_txns = node_transactions.entry(new_txn.node.to_string()).or_default();
                        node_txns.push(new_txn.to_owned());
                        node_txns.sort_unstable();
                    }
                }
            }

            Message::PollTransactions { first_xid } => {
                let transactions = if let Some(node_txns) = node_transactions.lock().unwrap().get(&local_node) {
                    node_txns.iter().filter(|txn| txn.transaction_id >= *first_xid).cloned().collect()
                } else {
                    vec![]
                };
                output_sender.send(envelope.reply(Message::Transactions { transactions })).unwrap();
            }

            _ => panic!("Unexpected message at runtime: {envelope:?}")
        }
    }
}
