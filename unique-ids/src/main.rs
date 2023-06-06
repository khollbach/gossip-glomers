use std::{fmt::Write, io};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct InitMessage {
    src: String,
    dest: String,
    body: InitBody,
}

#[derive(Debug, Serialize, Deserialize)]
struct InitBody {
    #[serde(rename = "type")]
    type_: String,
    msg_id: u32,

    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateMessage {
    src: String,
    dest: String,
    body: GenerateBody,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateBody {
    #[serde(rename = "type")]
    type_: String,
    msg_id: u32,
}

fn main() -> Result<()> {
    // An incrementing sequence number of how many messages I've sent so far.
    let mut next_msg_id = 0;

    let mut stdin = io::stdin().lines();
    let init = stdin
        .by_ref()
        .next()
        .context("expected init message; got EOF")??;
    let message: InitMessage = serde_json::from_str(&init)?;

    let client = message.src;
    ensure!(message.body.type_ == "init");
    let req_id = message.body.msg_id;
    let my_id = message.body.node_id;

    let resp = json!({
        "src": my_id,
        "dest": client,
        "body": {
            "type": "init_ok",
            "msg_id": next_msg_id,
            "in_reply_to": req_id,
        },
    });
    println!("{}", resp);
    next_msg_id += 1;

    for line in stdin {
        let message: GenerateMessage = serde_json::from_str(&line?)?;

        let client = message.src;
        ensure!(message.dest == my_id);
        ensure!(message.body.type_ == "generate");
        let req_id = message.body.msg_id;

        let mut id = my_id.clone();
        write!(id, "-{next_msg_id}")?;

        let resp = json!({
            "src": my_id,
            "dest": client,
            "body": {
                "type": "generate_ok",
                "msg_id": next_msg_id,
                "in_reply_to": req_id,
                "id": id,
            },
        });
        println!("{}", resp);
        next_msg_id += 1;
    }

    Ok(())
}
