use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

static MESSAGE_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize, Debug)]
pub struct Body<B: Debug> {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    message: B
}

impl<B: Clone + Debug> Clone for Body<B> {
    fn clone(&self) -> Self {
        Body {
            msg_id: self.msg_id.clone(),
            in_reply_to: self.in_reply_to.clone(),
            message: self.message.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Envelope<B: Debug> {
    pub src: String,
    pub dest: String,
    body: Body<B>
}

impl<B: Clone + Debug> Clone for Envelope<B> {
    fn clone(&self) -> Self {
        Envelope {
            src: self.src.clone(),
            dest: self.dest.clone(),
            body: self.body.clone(),
        }
    }
}

impl<B: Debug> Envelope<B> {
    pub fn new(src: String, dest: String, in_reply_to: Option<usize>, message: B) -> Envelope<B> {
        Envelope {
            src,
            dest,
            body: Body {
                msg_id: Some(MESSAGE_ID.fetch_add(1, Ordering::SeqCst)),
                in_reply_to,
                message
            }
        }
    }

    pub fn is_from_node(&self) -> bool {
        self.src.starts_with('n')
    }

    pub fn message(&self) -> &B {
        &self.body.message
    }

    pub fn msg_id(&self) -> Option<usize> {
        self.body.msg_id
    }

    pub fn in_reply_to(&self) -> Option<usize> {
        self.body.in_reply_to
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
