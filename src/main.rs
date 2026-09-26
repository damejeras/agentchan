use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{ServerCapabilities, ServerConfig};
use rmcp::transport::stdio;
use rmcp::{ServerHandler, ServiceExt, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

// A receive must return before the client's MCP tool timeout, which is 60 s by default.
const RECV_WAIT: Duration = Duration::from_secs(50);

#[derive(Clone, Copy, PartialEq)]
enum Agent {
    Claude,
    Codex,
    Grok,
}

const AGENTS: [Agent; 3] = [Agent::Claude, Agent::Codex, Agent::Grok];

impl Agent {
    fn name(self) -> &'static str {
        match self {
            Agent::Claude => "claude",
            Agent::Codex => "codex",
            Agent::Grok => "grok",
        }
    }

    // True when the agent's CLI is installed and signed in.
    async fn ready(self) -> bool {
        let (program, args): (_, &[_]) = match self {
            Agent::Claude => ("claude", &["auth", "status"]),
            Agent::Codex => ("codex", &["login", "status"]),
            Agent::Grok => ("grok", &["models"]),
        };
        let Ok(output) = Command::new(program).args(args).output().await else {
            return false;
        };
        match self {
            // grok has no status command, and `grok models` exits 0 when signed out too.
            Agent::Grok => !String::from_utf8_lossy(&[output.stdout, output.stderr].concat())
                .contains("not authenticated"),
            _ => output.status.success(),
        }
    }
}

#[derive(Clone, Serialize)]
struct Reply {
    channel: String,
    ok: bool,
    text: String,
}

struct Turn {
    session: Option<String>,
    ok: bool,
    text: String,
}

struct Channel {
    agent: Agent,
    prompts: mpsc::UnboundedSender<String>,
    worker: JoinHandle<()>,
    // Turns sent and not yet replied to. With `replies`, it tells receive that nothing can arrive.
    pending: usize,
    replies: VecDeque<Reply>,
}

type Channels = Arc<Mutex<HashMap<String, Channel>>>;

#[derive(Clone)]
struct Server {
    channels: Channels,
    arrived: Arc<Notify>,
    tool_router: ToolRouter<Server>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct MessageArgs {
    /// The message. The agent sees only what is on its channel, so give it the context it needs.
    message: String,
    /// Leave out to start a new session. Give a channel id from an earlier call to send a follow-up.
    channel: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReceiveArgs {
    /// Channel ids to wait on. The first reply on any of them is returned.
    channels: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CloseArgs {
    channel: String,
}

#[tool_router]
impl Server {
    #[tool(
        description = "Send a message to Claude Code. Returns at once with the channel id; read the reply with receive. Messages on one channel run in order, one at a time, in the same session."
    )]
    fn claude(&self, Parameters(args): Parameters<MessageArgs>) -> Result<String, String> {
        self.send(Agent::Claude, args)
    }

    #[tool(
        description = "Send a message to Codex. Returns at once with the channel id; read the reply with receive. Messages on one channel run in order, one at a time, in the same session."
    )]
    fn codex(&self, Parameters(args): Parameters<MessageArgs>) -> Result<String, String> {
        self.send(Agent::Codex, args)
    }

    #[tool(
        description = "Send a message to Grok. Returns at once with the channel id; read the reply with receive. Messages on one channel run in order, one at a time, in the same session."
    )]
    fn grok(&self, Parameters(args): Parameters<MessageArgs>) -> Result<String, String> {
        self.send(Agent::Grok, args)
    }

    #[tool(
        description = "Wait for the next reply on any of the given channels, like a Go select. Returns {channel, ok, text} or {timeout: true} after 50 s; on timeout, call receive again."
    )]
    async fn receive(&self, Parameters(args): Parameters<ReceiveArgs>) -> Result<String, String> {
        let deadline = tokio::time::Instant::now() + RECV_WAIT;
        loop {
            // Register for the wake-up before looking, so a reply that lands between the look
            // and the wait is not missed.
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            {
                let mut channels = self.channels.lock().unwrap();
                let mut waiting = false;
                for id in &args.channels {
                    let Some(channel) = channels.get_mut(id) else {
                        return Err(format!("unknown channel: {id}"));
                    };
                    if let Some(reply) = channel.replies.pop_front() {
                        return Ok(serde_json::to_string(&reply).unwrap());
                    }
                    waiting |= channel.pending > 0;
                }
                if !waiting {
                    return Err("nothing was sent on these channels, so no reply can arrive".into());
                }
            }
            if tokio::time::timeout_at(deadline, arrived).await.is_err() {
                return Ok(json!({ "timeout": true }).to_string());
            }
        }
    }

    #[tool(
        description = "Close a channel. Stops the agent if it is running and drops replies not yet received."
    )]
    fn close(&self, Parameters(args): Parameters<CloseArgs>) -> Result<String, String> {
        let channel = self
            .channels
            .lock()
            .unwrap()
            .remove(&args.channel)
            .ok_or_else(|| format!("unknown channel: {}", args.channel))?;
        // Aborting the worker drops the running turn, which kills the agent's process group.
        channel.worker.abort();
        // A receive that waits on this channel must wake up and see that it is gone.
        self.arrived.notify_waiters();
        Ok(json!({ "closed": args.channel }).to_string())
    }
}

impl Server {
    fn send(&self, agent: Agent, args: MessageArgs) -> Result<String, String> {
        let mut channels = self.channels.lock().unwrap();
        if let Some(id) = args.channel {
            return match channels.get_mut(&id) {
                Some(channel) if channel.agent == agent => {
                    channel.pending += 1;
                    let _ = channel.prompts.send(args.message);
                    Ok(json!({ "channel": id }).to_string())
                }
                Some(channel) => Err(format!(
                    "channel {id} is a {} channel",
                    channel.agent.name()
                )),
                None => Err(format!("unknown channel: {id}")),
            };
        }
        let id = loop {
            let id = format!("{:06x}", RandomState::new().hash_one(()) & 0xff_ffff);
            if !channels.contains_key(&id) {
                break id;
            }
        };
        let (prompts, rx) = mpsc::unbounded_channel();
        let _ = prompts.send(args.message);
        let worker = tokio::spawn(work(
            agent,
            id.clone(),
            rx,
            self.channels.clone(),
            self.arrived.clone(),
        ));
        channels.insert(
            id.clone(),
            Channel {
                agent,
                prompts,
                worker,
                pending: 1,
                replies: VecDeque::new(),
            },
        );
        Ok(json!({ "channel": id }).to_string())
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Talk to other coding agents. Each agent tool sends a message on a channel, which is one session with that agent; receive waits for replies; close ends a channel.",
        )
    }
}

async fn work(
    agent: Agent,
    id: String,
    mut prompts: mpsc::UnboundedReceiver<String>,
    channels: Channels,
    arrived: Arc<Notify>,
) {
    let mut session = None;
    while let Some(prompt) = prompts.recv().await {
        let turn = run(agent, &prompt, session.as_deref()).await;
        if turn.session.is_some() {
            session = turn.session;
        }
        let reply = Reply {
            channel: id.clone(),
            ok: turn.ok,
            text: turn.text,
        };
        if let Some(channel) = channels.lock().unwrap().get_mut(&id) {
            channel.pending -= 1;
            channel.replies.push_back(reply);
        }
        arrived.notify_waiters();
    }
}

async fn run(agent: Agent, prompt: &str, session: Option<&str>) -> Turn {
    // Claude and Codex read the prompt from stdin, so a prompt that starts with "-" is not read
    // as a flag and its length is not limited by the argument size limit.
    let (mut command, stdin) = match agent {
        Agent::Claude => {
            let mut command = Command::new("claude");
            command.args([
                "-p",
                "--output-format",
                "json",
                "--dangerously-skip-permissions",
            ]);
            if let Some(session) = session {
                command.args(["--resume", session]);
            }
            (command, Some(prompt))
        }
        Agent::Codex => {
            let mut command = Command::new("codex");
            command.args([
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--dangerously-bypass-approvals-and-sandbox",
            ]);
            if let Some(session) = session {
                command.args(["resume", session]);
            }
            command.arg("-");
            (command, Some(prompt))
        }
        Agent::Grok => {
            let mut command = Command::new("grok");
            command.arg(format!("--single={prompt}")).args([
                "--output-format",
                "json",
                "--always-approve",
            ]);
            if let Some(session) = session {
                command.args(["--resume", session]);
            }
            (command, None)
        }
    };
    // The server's own stdin carries the MCP protocol, so the child must never inherit it.
    command
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Its own process group, so that a stopped turn also stops the commands the agent started.
        .process_group(0)
        .kill_on_drop(true);

    let output = async {
        let mut child = command.spawn()?;
        // Declared after `child`, so on cancellation the group is killed before the child is dropped.
        let mut group = KillGroupOnDrop(child.id());
        let (pipe, mut out, mut err) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take());
        // Write the prompt while reading the output. When the agent writes a lot before it reads
        // its stdin, a write that must finish first fills both pipes and blocks both processes.
        let write = async {
            if let (Some(mut pipe), Some(prompt)) = (pipe, stdin) {
                // An agent that exits without reading its prompt breaks the pipe. Its output then
                // says what went wrong, so the write error is not needed.
                let _ = pipe.write_all(prompt.as_bytes()).await;
            }
        };
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let ((), read_out, read_err) = tokio::join!(
            write,
            out.as_mut().unwrap().read_to_end(&mut stdout),
            err.as_mut().unwrap().read_to_end(&mut stderr),
        );
        read_out?;
        read_err?;
        // Reap the agent only after its output is closed. Until it is reaped, the agent keeps its
        // id, so no other process group can take that id while the kill guard is armed.
        let status = child.wait().await?;
        group.0 = None;
        Ok::<_, std::io::Error>(std::process::Output {
            status,
            stdout,
            stderr,
        })
    }
    .await;
    let output = match output {
        Ok(output) => output,
        Err(error) => return failed(format!("could not run {}: {error}", agent.name())),
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let turn = match agent {
        Agent::Claude => serde_json::from_str::<Value>(&stdout).ok().map(|v| Turn {
            session: v["session_id"].as_str().map(String::from),
            ok: !v["is_error"].as_bool().unwrap_or(false),
            text: v["result"].as_str().unwrap_or_default().to_string(),
        }),
        Agent::Codex => {
            let mut turn = Turn {
                session: None,
                ok: true,
                text: String::new(),
            };
            for v in stdout
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            {
                match v["type"].as_str() {
                    Some("thread.started") => {
                        turn.session = v["thread_id"].as_str().map(String::from)
                    }
                    Some("item.completed") if v["item"]["type"] == "agent_message" => {
                        turn.text = v["item"]["text"].as_str().unwrap_or_default().to_string()
                    }
                    Some("turn.failed") => {
                        turn.ok = false;
                        turn.text = v["error"]["message"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                    }
                    Some("error") => {
                        turn.ok = false;
                        turn.text = v["message"].as_str().unwrap_or_default().to_string();
                    }
                    _ => {}
                }
            }
            Some(turn)
        }
        // One JSON document, printed over many lines.
        Agent::Grok => {
            serde_json::from_str::<Value>(&stdout)
                .ok()
                .map(|v| match v["text"].as_str() {
                    Some(text) => Turn {
                        session: v["sessionId"].as_str().map(String::from),
                        ok: true,
                        text: text.to_string(),
                    },
                    None => Turn {
                        session: None,
                        ok: false,
                        text: v["message"].as_str().unwrap_or_default().to_string(),
                    },
                })
        }
    };
    match turn {
        Some(turn) if turn.ok && output.status.success() => turn,
        Some(turn) if !turn.text.is_empty() => Turn { ok: false, ..turn },
        // Keep the session the agent reported, so the next message still resumes it.
        turn => Turn {
            ok: false,
            text: format!(
                "{} exited with {}: {}",
                agent.name(),
                output.status,
                stderr.trim()
            ),
            session: turn.and_then(|turn| turn.session),
        },
    }
}

struct KillGroupOnDrop(Option<u32>);

impl Drop for KillGroupOnDrop {
    fn drop(&mut self) {
        if let Some(pgid) = self.0 {
            // SAFETY: killpg only sends a signal; the group is the one this turn's agent leads.
            unsafe { libc::killpg(pgid as libc::pid_t, libc::SIGKILL) };
        }
    }
}

fn failed(text: String) -> Turn {
    Turn {
        session: None,
        ok: false,
        text,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Offer a tool only for the agents that are installed and signed in.
    let mut tool_router = Server::tool_router();
    for agent in AGENTS {
        if !agent.ready().await {
            tool_router.remove_route(agent.name());
        }
    }

    let server = Server {
        channels: Arc::default(),
        arrived: Arc::new(Notify::new()),
        tool_router,
    };
    server.serve(stdio()).await?.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_wakes_a_waiting_receive() {
        let server = Server {
            channels: Arc::default(),
            arrived: Arc::new(Notify::new()),
            tool_router: Server::tool_router(),
        };
        let (prompts, _rx) = mpsc::unbounded_channel();
        let channel = Channel {
            agent: Agent::Claude,
            prompts,
            worker: tokio::spawn(std::future::pending()),
            pending: 1,
            replies: VecDeque::new(),
        };
        server
            .channels
            .lock()
            .unwrap()
            .insert("abc123".into(), channel);

        let receive = server.receive(Parameters(ReceiveArgs {
            channels: vec!["abc123".into()],
        }));
        tokio::pin!(receive);
        // A zero timeout polls receive once, so it is waiting before close runs.
        assert!(
            tokio::time::timeout(Duration::ZERO, receive.as_mut())
                .await
                .is_err()
        );
        server
            .close(Parameters(CloseArgs {
                channel: "abc123".into(),
            }))
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(1), receive)
            .await
            .expect("receive was not woken by close");
        assert_eq!(received, Err("unknown channel: abc123".to_string()));
    }
}
