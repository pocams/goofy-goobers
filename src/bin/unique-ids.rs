
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use goofy_goobers::message::Envelope;

static ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Generate,
    GenerateOk { id: String },
}

fn main() {
    let mut stdout = std::io::stdout();
    let mut my_node_id = "".to_string();
    for line in std::io::stdin().lines() {
        let env: Envelope<Message> = serde_json::from_str(&line.unwrap()).unwrap();
        match env.message() {
            Message::Init { node_id, node_ids } => {
                eprintln!("init: {} of {:?}", node_id, node_ids);
                my_node_id = node_id.clone();
                let r = env.reply(Message::InitOk);
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            },

            Message::Generate => {
                let next_id = ID.fetch_add(1, Ordering::SeqCst);
                let r = env.reply(Message::GenerateOk { id: format!("{}.{}", my_node_id, next_id) });
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            }

            _ => unimplemented!()
        }
    }
}
