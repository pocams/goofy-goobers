use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

static MESSAGE_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize, Debug)]
pub struct Body<B> {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    message: B
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Envelope<B> {
    src: String,
    dest: String,
    body: Body<B>
}

impl<B> Envelope<B> {
    pub fn message(&self) -> &B {
        &self.body.message
    }

    pub fn reply(&self, message: B) -> Envelope<B> {
        Envelope {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: Body {
                msg_id: Some(MESSAGE_ID.fetch_add(1, Ordering::SeqCst)),
                in_reply_to: self.body.msg_id,
                message
            }
        }
    }
}
