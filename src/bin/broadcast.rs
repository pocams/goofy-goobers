use std::collections::{HashMap, HashSet};


use serde::{Deserialize, Serialize};

use goofy_goobers::message::Envelope;

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Broadcast { message: u64 },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<u64> },
    Topology {
        topology: HashMap<String, Vec<String>>
    },
    TopologyOk
}

fn main() {
    let mut stdout = std::io::stdout();
    let mut my_node_id = "".to_string();
    let mut all_node_ids = vec![];
    let mut messages = HashSet::new();
    let mut node_topology = Default::default();

    for line in std::io::stdin().lines() {
        eprintln!("{:?}", line);
        let env: Envelope<Message> = serde_json::from_str(&line.unwrap()).unwrap();

        match env.message() {
            Message::Init { node_id, node_ids } => {
                my_node_id = node_id.clone();
                all_node_ids = node_ids.clone();
                eprintln!("init: {} of {:?}", node_id, node_ids);
                let r = env.reply(Message::InitOk);
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            },

            Message::Broadcast { message } => {
                messages.insert(*message);
                let r = env.reply(Message::BroadcastOk);
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            }

            Message::Read => {
                let r = env.reply(Message::ReadOk { messages: messages.iter().copied().collect() });
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            }

            Message::Topology { topology } => {
                node_topology = topology.clone();
                let r = env.reply(Message::TopologyOk);
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            }

            _ => unimplemented!()
        }
    }
}
