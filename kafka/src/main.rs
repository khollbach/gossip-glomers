#![allow(unused)]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use maelstrom::{
    done,
    protocol::{Message, MessageBody},
    Node, Result, Runtime,
};
use serde_json::Value;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    messages: Mutex<HashMap<String, Vec<Value>>>,
    committed_offsets: Mutex<HashMap<String, usize>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "send" {
            let key = req.body.extra["key"].as_str().unwrap().to_string();
            let value = req.body.extra["msg"].clone();
            let offset = {
                let mut unlocked = self.messages.lock().unwrap();
                let key_messages = unlocked.entry(key).or_default();
                key_messages.push(value);
                key_messages.len() - 1
            };
            let mut response_body = MessageBody::new().with_type("send_ok");
            response_body
                .extra
                .insert("offset".to_string(), offset.into());
            return runtime.reply(req, response_body).await;
        } else if req.get_type() == "poll" {
            let mut replies = serde_json::Map::new();

            for (key, offset) in req.body.extra["offsets"].as_object().unwrap().iter() {
                let idx = offset.as_u64().unwrap() as usize;
                match &self.messages.lock().unwrap().get(key) {
                    Some(message_list) if message_list.len() > idx => {
                        let message = message_list[idx].clone();

                        let value = Value::Array(vec![vec![offset.clone(), message].into()]);
                        replies.insert(key.clone(), value);
                    }
                    _ => {
                        replies.insert(key.clone(), Value::Array(vec![]));
                    }
                }
            }
            let mut body = MessageBody::new().with_type("poll_ok");
            body.extra.insert("msgs".to_string(), replies.into());

            return runtime.reply(req, body).await;
        } else if req.get_type() == "commit_offsets" {
            for (key, offset) in req.body.extra["offsets"].as_object().unwrap().iter() {
                let new_offset = offset.as_u64().unwrap() as usize;
                let prev_offset = *self
                    .committed_offsets
                    .lock()
                    .unwrap()
                    .entry(key.clone())
                    .or_default();
                if new_offset > prev_offset {
                    self.committed_offsets
                        .lock()
                        .unwrap()
                        .insert(key.clone(), new_offset);
                }
            }

            let mut body = MessageBody::new().with_type("commit_offsets_ok");

            return runtime.reply(req, body).await;
        } else if req.get_type() == "list_committed_offsets" {
            let mut response = serde_json::Map::new();
            for key in req.body.extra["keys"].as_array().unwrap().iter() {
                let key_str = key.as_str().unwrap().to_string();
                if let Some(&offset) = self.committed_offsets.lock().unwrap().get(&key_str) {
                    response.insert(key_str, offset.into());
                }
            }
            let mut body = MessageBody::new().with_type("list_committed_offsets_ok");
            body.extra.insert("offsets".to_string(), response.into());
            return runtime.reply(req, body).await;
        }
        done(runtime, req)
    }

    // if req.get_type() == "broadcast" {
    // let value = req.body.extra["message"].as_i64().expect("not an integer");
    // self.seen_messages
    //     .lock()
    //     .expect("could not lock")
    //     .insert(value);

    //     if runtime.is_client(&req.src) && runtime.node_id() != LEADER_ID {
    //         let mut forward_body = MessageBody::new().with_type("broadcast");
    //         forward_body
    //             .extra
    //             .insert("message".to_string(), req.body.extra["message"].clone());
    //     return runtime.reply(req, response).await;

    //         // send the value to the leader
    //         let mut rpc_handle = runtime.rpc(LEADER_ID, forward_body.clone()).await?;

    //         // wait for a reply; but if it times out, ignore that
    //         let (ctx, _handle) = Context::with_timeout(Duration::from_millis(50));
    //         rpc_handle.done_with(ctx).await?;

    //         // how to handle timeouts?
    //         // ignore for now, see if there's a problem?

    //         // let mut send_futures = VecDeque::<(String, RPCResult)>::new();
    //         // for node_id in runtime.nodes() {
    //         //     send_futures.push_back((node_id.to_string(), runtime.rpc(node_id, forward_body.clone()).await?));
    //         // }
    //         // while !send_futures.is_empty() {
    //         //     let (node_id, mut first_future) = send_futures.pop_front().unwrap();
    //         //     let (ctx, _handle) = Context::with_timeout(Duration::from_millis(1000));
    //         //     let rpc_result = first_future.done_with(ctx).await;
    //         //     if rpc_result.is_err() {
    //         //         eprintln!("failed to send with error {:?}, retrying", rpc_result);
    //         //         send_futures.push_back((node_id.to_string(), runtime.rpc(node_id, forward_body.clone()).await?));
    //         //     }
    //         // }
    //     }

    //     let mut response_body = req.body.clone().with_type("broadcast_ok");
    //     response_body.extra.remove("message");
    //     runtime.reply(req.clone(), response_body).await?;

    //     return Ok(());
    // } else if req.get_type() == "read" {
    //     // if we're a follower, ask the leader for the latest set; update ours
    //     // (allow this to time out)
    //     if runtime.node_id() != LEADER_ID {
    //         let forward_body = MessageBody::new().with_type("read");
    //         let mut rpc_handle = runtime.rpc(LEADER_ID, forward_body.clone()).await?;

    //         // wait for a reply (todo: timeout=?)
    //         let (ctx, _handle) = Context::with_timeout(Duration::from_millis(1000));
    //         let resp = rpc_handle.done_with(ctx).await?;

    //         let mut seen = self.seen_messages.lock().unwrap();
    //         for value in resp.body.extra["messages"].as_array().unwrap() {
    //             seen.insert(value.as_i64().unwrap());
    //         }
    //     }

    //     // reply with our freshest data
    //     let mut response = req.body.clone().with_type("read_ok");
    //     let messages: Vec<_> = self.seen_messages.lock().unwrap().iter().copied().collect();
    //     response.extra.insert("messages".into(), messages.into());
    //     return runtime.reply(req, response).await;
    // } else if req.get_type() == "topology" {
    //     let mut response = req.body.clone().with_type("topology_ok");
    //     response.extra.remove("topology");
    //     return runtime.reply(req, response).await;
    // }

    // done(runtime, req)
}
