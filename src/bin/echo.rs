


use serde::{Deserialize, Serialize};

use goofy_goobers::message::Envelope;


#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Message {
    Init { node_id: String, node_ids: Vec<String> },
    InitOk,
    Echo { echo: String },
    EchoOk { echo: String },
}

fn main() {
    let mut stdout = std::io::stdout();
    for line in std::io::stdin().lines() {
        let env: Envelope<Message> = serde_json::from_str(&line.unwrap()).unwrap();
        match env.message() {
            Message::Init { node_id, node_ids } => {
                eprintln!("init: {} of {:?}", node_id, node_ids);
                let r = env.reply(Message::InitOk);
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            },
            Message::Echo { echo  } => {
                let r = env.reply(Message::EchoOk { echo: echo.clone() });
                serde_json::to_writer(&mut stdout, &r).unwrap();
                println!();
            }
            _ => unimplemented!()
        }
    }
}
