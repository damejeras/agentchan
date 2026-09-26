// Drives the agentchan binary over MCP. Fake `claude`, `codex` and `grok` scripts on PATH
// print what the real CLIs print, so these tests do not use any agent's plan limits.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::transport::TokioChildProcess;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::process::Command;

const FAKE_CLAUDE: &str = r#"#!/bin/sh
[ "$1" = auth ] && exit 0
resume=new
while [ $# -gt 0 ]; do [ "$1" = --resume ] && resume=$2; shift; done
[ -e "$FAKE_DIR/noise" ] && head -c 300000 /dev/zero | tr '\0' x >&2
prompt=$(cat)
if [ "$prompt" = sleep ]; then
  sleep 300 &
  echo $! > "$FAKE_DIR/grandchild.tmp"
  mv "$FAKE_DIR/grandchild.tmp" "$FAKE_DIR/grandchild"
  wait
fi
printf '{"type":"result","is_error":false,"session_id":"claude-session","result":"resume=%s prompt=%s"}\n' \
  "$resume" "$(printf %s "$prompt" | head -c 20) bytes=$(printf %s "$prompt" | wc -c)"
"#;

const FAKE_CODEX: &str = r#"#!/bin/sh
[ "$1" = login ] && exit 0
thread=codex-thread
resume=new
while [ $# -gt 0 ]; do [ "$1" = resume ] && resume=$2; shift; done
prompt=$(cat)
printf '{"type":"thread.started","thread_id":"%s"}\n' "$thread"
if [ "$prompt" = fail ]; then
  echo "boom: not signed in" >&2
  exit 1
fi
printf '{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"resume=%s prompt=%s"}}\n' "$resume" "$prompt"
printf '{"type":"turn.completed","usage":{}}\n'
"#;

const FAKE_GROK: &str = r#"#!/bin/sh
if [ "$1" = models ]; then
  [ -e "$FAKE_DIR/grok-signed-out" ] && echo "You are not authenticated."
  echo "Default model: grok-4.6"
  exit 0
fi
resume=new
prompt=
for arg; do
  case "$arg" in --single=*) prompt=${arg#--single=} ;; esac
done
while [ $# -gt 0 ]; do [ "$1" = --resume ] && resume=$2; shift; done
printf '{\n  "text": "resume=%s prompt=%s",\n  "stopReason": "end_turn",\n  "sessionId": "grok-session"\n}\n' "$resume" "$prompt"
"#;

struct Fakes {
    dir: TempDir,
}

impl Fakes {
    fn new() -> Fakes {
        let dir = tempfile::tempdir().unwrap();
        for (name, script) in [
            ("claude", FAKE_CLAUDE),
            ("codex", FAKE_CODEX),
            ("grok", FAKE_GROK),
        ] {
            let path = dir.path().join(name);
            fs::write(&path, script).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Fakes { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    async fn start(&self) -> RunningService<RoleClient, ()> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agentchan"));
        let path = format!(
            "{}:{}",
            self.path().display(),
            std::env::var("PATH").unwrap()
        );
        command
            .env("PATH", path)
            .env("FAKE_DIR", self.path())
            .current_dir(self.path());
        ().serve(TokioChildProcess::new(command).unwrap())
            .await
            .unwrap()
    }
}

// Returns the tool's text, or Err with it when the server marked the result as an error.
async fn call(
    client: &RunningService<RoleClient, ()>,
    tool: &'static str,
    args: Value,
) -> Result<Value, String> {
    let params = CallToolRequestParams::new(tool).with_arguments(args.as_object().unwrap().clone());
    let result = client.call_tool(params).await.unwrap();
    let text = result.content[0].as_text().unwrap().text.clone();
    if result.is_error == Some(true) {
        Err(text)
    } else {
        Ok(serde_json::from_str(&text).unwrap())
    }
}

async fn open(
    client: &RunningService<RoleClient, ()>,
    tool: &'static str,
    message: &str,
) -> String {
    let sent = call(client, tool, json!({ "message": message }))
        .await
        .unwrap();
    sent["channel"].as_str().unwrap().to_string()
}

async fn tool_names(client: &RunningService<RoleClient, ()>) -> Vec<String> {
    let mut names: Vec<_> = client
        .list_all_tools()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn offers_a_tool_for_each_signed_in_agent() {
    let fakes = Fakes::new();
    let client = fakes.start().await;
    assert_eq!(
        tool_names(&client).await,
        ["claude", "close", "codex", "grok", "receive"]
    );

    fs::write(fakes.path().join("grok-signed-out"), "").unwrap();
    let client = fakes.start().await;
    assert_eq!(
        tool_names(&client).await,
        ["claude", "close", "codex", "receive"]
    );
}

#[tokio::test]
async fn receive_returns_replies_from_any_listed_channel() {
    let fakes = Fakes::new();
    let client = fakes.start().await;
    let a = open(&client, "claude", "apple").await;
    let b = open(&client, "codex", "banana").await;
    let c = open(&client, "grok", "cherry").await;
    assert_eq!(a.len(), 6);

    let mut replies = Vec::new();
    for _ in 0..3 {
        let reply = call(&client, "receive", json!({ "channels": [a, b, c] }))
            .await
            .unwrap();
        assert_eq!(reply["ok"], true);
        replies.push((
            reply["channel"].as_str().unwrap().to_string(),
            reply["text"].as_str().unwrap().to_string(),
        ));
    }
    replies.sort();
    let mut expected = vec![
        (a, "resume=new prompt=apple bytes=5".to_string()),
        (b, "resume=new prompt=banana".to_string()),
        (c, "resume=new prompt=cherry".to_string()),
    ];
    expected.sort();
    assert_eq!(replies, expected);
}

#[tokio::test]
async fn follow_ups_run_in_order_in_the_same_session() {
    let fakes = Fakes::new();
    let client = fakes.start().await;
    for (tool, session) in [
        ("claude", "claude-session"),
        ("codex", "codex-thread"),
        ("grok", "grok-session"),
    ] {
        let channel = open(&client, tool, "one").await;
        let sent = call(
            &client,
            tool,
            json!({ "message": "two", "channel": channel }),
        )
        .await
        .unwrap();
        assert_eq!(sent["channel"], channel.as_str());

        let first = call(&client, "receive", json!({ "channels": [channel] }))
            .await
            .unwrap();
        let second = call(&client, "receive", json!({ "channels": [channel] }))
            .await
            .unwrap();
        assert!(
            first["text"]
                .as_str()
                .unwrap()
                .starts_with("resume=new prompt=one"),
            "{tool}: {first}"
        );
        let resumed = format!("resume={session} prompt=two");
        assert!(
            second["text"].as_str().unwrap().starts_with(&resumed),
            "{tool}: {second}"
        );
    }
}

#[tokio::test]
async fn wrong_calls_are_tool_errors() {
    let fakes = Fakes::new();
    let client = fakes.start().await;
    let codex = open(&client, "codex", "x").await;
    call(&client, "receive", json!({ "channels": [codex] }))
        .await
        .unwrap();

    let wrong_agent = call(
        &client,
        "claude",
        json!({ "message": "x", "channel": codex }),
    )
    .await;
    assert_eq!(
        wrong_agent,
        Err(format!("channel {codex} is a codex channel"))
    );
    let unknown = call(
        &client,
        "codex",
        json!({ "message": "x", "channel": "000000" }),
    )
    .await;
    assert_eq!(unknown, Err("unknown channel: 000000".to_string()));
    let unknown = call(&client, "receive", json!({ "channels": ["000000"] })).await;
    assert_eq!(unknown, Err("unknown channel: 000000".to_string()));
    let drained = call(&client, "receive", json!({ "channels": [codex] })).await;
    assert_eq!(
        drained,
        Err("nothing was sent on these channels, so no reply can arrive".to_string())
    );
    let unknown = call(&client, "close", json!({ "channel": "000000" })).await;
    assert_eq!(unknown, Err("unknown channel: 000000".to_string()));
}

#[tokio::test]
async fn a_failed_agent_reports_its_stderr_and_keeps_its_session() {
    let fakes = Fakes::new();
    let client = fakes.start().await;
    let channel = open(&client, "codex", "fail").await;
    let reply = call(&client, "receive", json!({ "channels": [channel] }))
        .await
        .unwrap();
    assert_eq!(reply["ok"], false);
    assert!(
        reply["text"]
            .as_str()
            .unwrap()
            .contains("boom: not signed in"),
        "{reply}"
    );

    call(
        &client,
        "codex",
        json!({ "message": "again", "channel": channel }),
    )
    .await
    .unwrap();
    let reply = call(&client, "receive", json!({ "channels": [channel] }))
        .await
        .unwrap();
    assert_eq!(reply["text"], "resume=codex-thread prompt=again");
}

#[tokio::test]
async fn close_stops_the_agent_and_its_commands_and_wakes_receive() {
    let fakes = Fakes::new();
    let client = fakes.start().await;
    let channel = open(&client, "claude", "sleep").await;
    let grandchild = fakes.path().join("grandchild");
    while !grandchild.exists() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let pid: u32 = fs::read_to_string(&grandchild)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    let started = Instant::now();
    let waiting = call(&client, "receive", json!({ "channels": [channel.clone()] }));
    let closing = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        call(&client, "close", json!({ "channel": channel.clone() })).await
    };
    let (received, closed) = tokio::join!(waiting, closing);
    assert_eq!(closed.unwrap()["closed"], channel.as_str());
    assert_eq!(received, Err(format!("unknown channel: {channel}")));
    assert!(started.elapsed() < Duration::from_secs(5));

    // A killed process stays in the process table as a zombie until it is reaped, so look at its
    // state, and give the reaper time.
    let running = || {
        fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| !rest.starts_with('Z'))
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while running() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!running(), "the command the agent started is still running");
}

#[tokio::test]
async fn a_large_prompt_to_a_noisy_agent_does_not_block() {
    let fakes = Fakes::new();
    fs::write(fakes.path().join("noise"), "").unwrap();
    let client = fakes.start().await;
    let prompt = "p".repeat(300_000);
    let channel = open(&client, "claude", &prompt).await;
    let reply = tokio::time::timeout(
        Duration::from_secs(20),
        call(&client, "receive", json!({ "channels": [channel] })),
    )
    .await
    .expect("the agent and the server block each other on full pipes")
    .unwrap();
    assert!(
        reply["text"].as_str().unwrap().ends_with("bytes=300000"),
        "{reply}"
    );
}

// Runs the real CLIs, so it uses their plan limits. Run it with `cargo test -- --ignored`.
#[tokio::test]
#[ignore]
async fn real_agents_answer() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_agentchan"));
    command.current_dir(dir.path());
    let client = ().serve(TokioChildProcess::new(command).unwrap()).await.unwrap();
    let tools = tool_names(&client).await;
    for tool in ["claude", "codex", "grok"] {
        if !tools.iter().any(|name| name == tool) {
            continue;
        }
        let channel = open(&client, tool, "Reply with exactly the word: apple").await;
        let mut reply = call(&client, "receive", json!({ "channels": [channel] }))
            .await
            .unwrap();
        while reply["timeout"] == true {
            reply = call(&client, "receive", json!({ "channels": [channel] }))
                .await
                .unwrap();
        }
        assert_eq!(reply["ok"], true, "{tool}: {reply}");
        assert!(
            reply["text"]
                .as_str()
                .unwrap()
                .to_lowercase()
                .contains("apple"),
            "{tool}: {reply}"
        );
    }
}
