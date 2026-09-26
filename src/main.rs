use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::pin::pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::stdio;
use rmcp::{ServerHandler, ServiceExt, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// A receive must return before the client's MCP tool timeout, which is 60 s by default.
const RECV_WAIT: Duration = Duration::from_secs(50);
// How long a stopped agent has to exit before it is killed, and how long the output of an agent
// that exited can stay open.
const GRACE: Duration = Duration::from_secs(5);
// How long the check that an agent is installed and signed in can take.
const READY_WAIT: Duration = Duration::from_secs(10);
// The most output kept from one stream of one turn.
const OUTPUT_LIMIT: u64 = 16 << 20;

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
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own process group, so that a check that times out also stops what it started.
            .process_group(0)
            .kill_on_drop(true);
        let Ok(mut child) = command.spawn() else {
            return false;
        };
        // Declared after `child`, so on timeout the group is killed before the child is dropped.
        let mut group = Group(child.id());
        let (mut out, mut err) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let checked = tokio::time::timeout(READY_WAIT, async {
            tokio::try_join!(out.read_to_end(&mut stdout), err.read_to_end(&mut stderr))?;
            child.wait().await
        })
        .await;
        let Ok(Ok(status)) = checked else {
            return false;
        };
        // Reaped, so the id can now name another process.
        group.0 = None;
        match self {
            // grok has no status command, and `grok models` exits 0 when signed out too.
            Agent::Grok => {
                !String::from_utf8_lossy(&[stdout, stderr].concat()).contains("not authenticated")
            }
            _ => status.success(),
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
    stop: CancellationToken,
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
    // Stops every channel. Each channel's own stop token is a child of it.
    stop: CancellationToken,
    // Every channel's worker, also one that close removed and is still stopping.
    workers: TaskTracker,
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
    async fn receive(
        &self,
        cancelled: CancellationToken,
        Parameters(args): Parameters<ReceiveArgs>,
    ) -> Result<String, String> {
        let deadline = Instant::now() + RECV_WAIT;
        loop {
            // Register for the wake-up before looking, so a reply that lands between the look
            // and the wait is not missed.
            let mut arrived = pin!(self.arrived.notified());
            arrived.as_mut().enable();
            {
                let mut channels = self.channels.lock().unwrap();
                // The client drops the answer to a cancelled call, so a reply taken now is lost.
                if cancelled.is_cancelled() {
                    return Err("cancelled".into());
                }
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
            tokio::select! {
                () = arrived => {}
                () = cancelled.cancelled() => {}
                () = tokio::time::sleep_until(deadline) => {
                    return Ok(json!({ "timeout": true }).to_string());
                }
            }
        }
    }

    #[tool(
        description = "Close a channel. Stops the agent if it is running and drops replies not yet received."
    )]
    async fn close(&self, Parameters(args): Parameters<CloseArgs>) -> Result<String, String> {
        let channel = self
            .channels
            .lock()
            .unwrap()
            .remove(&args.channel)
            .ok_or_else(|| format!("unknown channel: {}", args.channel))?;
        // A receive that waits on this channel must wake up and see that it is gone.
        self.arrived.notify_waiters();
        channel.stop.cancel();
        let _ = channel.worker.await;
        Ok(json!({ "closed": args.channel }).to_string())
    }
}

impl Server {
    fn send(&self, agent: Agent, args: MessageArgs) -> Result<String, String> {
        let mut channels = self.channels.lock().unwrap();
        // Checked under the lock that shutdown holds when it stops the server, so no worker
        // starts after shutdown looked for workers.
        if self.stop.is_cancelled() {
            return Err("the server is stopping".into());
        }
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
        let stop = self.stop.child_token();
        let worker = self.workers.spawn(work(
            agent,
            id.clone(),
            rx,
            stop.clone(),
            self.channels.clone(),
            self.arrived.clone(),
        ));
        channels.insert(
            id.clone(),
            Channel {
                agent,
                prompts,
                stop,
                worker,
                pending: 1,
                replies: VecDeque::new(),
            },
        );
        Ok(json!({ "channel": id }).to_string())
    }

    // Stops every agent, and returns when all of them have exited.
    async fn shutdown(&self) {
        {
            let _channels = self.channels.lock().unwrap();
            self.stop.cancel();
        }
        self.workers.close();
        self.workers.wait().await;
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Talk to other coding agents. Each agent tool sends a message on a channel, which is one session with that agent; receive waits for replies; close ends a channel.",
            )
    }
}

async fn work(
    agent: Agent,
    id: String,
    mut prompts: mpsc::UnboundedReceiver<String>,
    stop: CancellationToken,
    channels: Channels,
    arrived: Arc<Notify>,
) {
    let mut session = None;
    let mut started = false;
    loop {
        // Stop first, so a stopped channel starts no queued turn.
        let prompt = tokio::select! {
            biased;
            () = stop.cancelled() => None,
            prompt = prompts.recv() => prompt,
        };
        let Some(prompt) = prompt else { return };
        // Without the session, the agent would start a new conversation and not see the
        // messages before this one.
        let turn = if started && session.is_none() {
            failed(format!(
                "the first message on this channel ended without a {} session, so this message cannot continue it; open a new channel",
                agent.name()
            ))
        } else {
            let Some(turn) = run(agent, &prompt, session.as_deref(), &stop).await else {
                return;
            };
            turn
        };
        started = true;
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

// Runs one turn. Returns None when `stop` ends it.
async fn run(
    agent: Agent,
    prompt: &str,
    session: Option<&str>,
    stop: &CancellationToken,
) -> Option<Turn> {
    // Each agent reads the prompt from stdin, so a prompt that starts with "-" is not read as a
    // flag, other users cannot see it in the process list, and its length is not limited by the
    // argument size limit.
    let mut command = match agent {
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
            command
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
            command
        }
        Agent::Grok => {
            let mut command = Command::new("grok");
            command.args([
                "--prompt-file",
                "/dev/stdin",
                "--output-format",
                "json",
                "--always-approve",
            ]);
            if let Some(session) = session {
                command.args(["--resume", session]);
            }
            command
        }
    };
    // The server's own stdin carries the MCP protocol, so the child must never inherit it.
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Its own process group, so that a stopped turn also stops the commands the agent started.
        .process_group(0)
        .kill_on_drop(true);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return Some(failed(format!("could not run {}: {error}", agent.name()))),
    };
    // Declared after `child`, so on cancellation the group is killed before the child is dropped.
    let mut group = Group(child.id());
    let mut exited = exited(child.id());
    let (pipe, out, err) = (
        child.stdin.take().unwrap(),
        child.stdout.take().unwrap(),
        child.stderr.take().unwrap(),
    );
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    // The agent's exit ends the turn, not the end of its output: a command that the agent leaves
    // running can hold the output open.
    let (stopped, read_result, grace_end) = {
        let mut io = pin!(async {
            // Write the prompt while reading the output. When the agent writes a lot before it reads
            // its stdin, a write that must finish first fills both pipes and blocks both processes.
            let write = async move {
                let mut pipe = pipe;
                // An agent that exits without reading its prompt breaks the pipe. Its output then
                // says what went wrong, so the write error is not needed.
                let _ = pipe.write_all(prompt.as_bytes()).await;
            };
            let ((), out, err) =
                tokio::join!(write, read(out, &mut stdout), read(err, &mut stderr));
            Ok::<_, std::io::Error>((out?, err?))
        });
        let mut read_result = None;
        let mut grace_end = None;
        let stopped = loop {
            tokio::select! {
                result = &mut io, if read_result.is_none() => read_result = Some(result),
                _ = &mut exited, if grace_end.is_none() => {
                    // The agent is not reaped yet, so its group id still names only its own group.
                    group.signal(libc::SIGKILL);
                    grace_end = Some(Instant::now() + GRACE);
                }
                () = tokio::time::sleep_until(grace_end.unwrap_or_else(Instant::now)),
                    if grace_end.is_some() && read_result.is_none() => break false,
                () = stop.cancelled() => break true,
            }
            if read_result.is_some() && grace_end.is_some() {
                break false;
            }
        };
        (stopped, read_result, grace_end)
    };
    if stopped {
        // Give the agent time to stop the commands it started, then kill what is left.
        group.signal(libc::SIGTERM);
        if grace_end.is_none() {
            let _ = tokio::time::timeout(GRACE, &mut exited).await;
        }
        group.signal(libc::SIGKILL);
        let _ = child.wait().await;
        group.0 = None;
        return None;
    }
    let status = child.wait().await;
    // Reaped, so the id can now name another process.
    group.0 = None;
    let status = match status {
        Ok(status) => status,
        Err(error) => return Some(failed(format!("could not run {}: {error}", agent.name()))),
    };
    let cut = match read_result {
        Some(Ok((cut, _))) => cut,
        Some(Err(error)) => {
            return Some(failed(format!("could not run {}: {error}", agent.name())));
        }
        // The output stayed open after the agent exited; use what it printed.
        None => false,
    };
    let stdout = String::from_utf8_lossy(&stdout);
    let stderr = String::from_utf8_lossy(&stderr);

    let turn = match agent {
        Agent::Claude => serde_json::from_str::<Value>(&stdout).ok().and_then(|v| {
            let ok = !v["is_error"].as_bool()?;
            let text = match v["result"].as_str() {
                Some(text) => text,
                // A failed run can have no result; its subtype says what went wrong.
                None if !ok => v["subtype"].as_str()?,
                None => return None,
            };
            Some(Turn {
                session: v["session_id"].as_str().map(String::from),
                ok,
                text: text.to_string(),
            })
        }),
        Agent::Codex => {
            let mut session = None;
            let mut message = String::new();
            // Only turn.completed or turn.failed ends a turn. An error event before them can be
            // one that Codex recovered from.
            let mut end = None;
            let mut error = String::new();
            for v in stdout
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            {
                match v["type"].as_str() {
                    Some("thread.started") => session = v["thread_id"].as_str().map(String::from),
                    Some("item.completed") if v["item"]["type"] == "agent_message" => {
                        message = v["item"]["text"].as_str().unwrap_or_default().to_string()
                    }
                    Some("turn.completed") => end = Some(true),
                    Some("turn.failed") => {
                        end = Some(false);
                        error = v["error"]["message"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                    }
                    Some("error") => error = v["message"].as_str().unwrap_or_default().to_string(),
                    _ => {}
                }
            }
            Some(match end {
                Some(true) => Turn {
                    session,
                    ok: true,
                    text: message,
                },
                _ => Turn {
                    session,
                    ok: false,
                    text: error,
                },
            })
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
    // A reply without a session is not a success: the next message could not resume it.
    // Each failure keeps the session the agent reported, so the next message still resumes it.
    Some(match turn {
        turn if cut => Turn {
            ok: false,
            text: format!(
                "{} printed more than {} MiB",
                agent.name(),
                OUTPUT_LIMIT >> 20
            ),
            session: turn.and_then(|turn| turn.session),
        },
        Some(turn) if turn.ok && turn.session.is_some() && status.success() => turn,
        Some(turn) if !turn.ok && !turn.text.is_empty() => turn,
        turn => Turn {
            ok: false,
            text: format!(
                "{} exited with {} and printed no reply: {}",
                agent.name(),
                status,
                stderr.trim()
            ),
            session: turn.and_then(|turn| turn.session),
        },
    })
}

// Reads a pipe to its end and keeps the first OUTPUT_LIMIT bytes. Returns true when it dropped
// bytes after them.
async fn read(mut pipe: impl AsyncRead + Unpin, buf: &mut Vec<u8>) -> std::io::Result<bool> {
    (&mut pipe).take(OUTPUT_LIMIT).read_to_end(buf).await?;
    // Read the rest too, so the agent does not block on a full pipe.
    Ok(tokio::io::copy(&mut pipe, &mut tokio::io::sink()).await? > 0)
}

// Finishes when the process has exited, and leaves it for `Child::wait` to reap.
fn exited(pid: Option<u32>) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let Some(pid) = pid else { return };
        loop {
            // SAFETY: an all-zero siginfo_t is valid, and waitid only writes to it.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // SAFETY: WNOWAIT leaves the process to be reaped by its Child.
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result == 0
                || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
            {
                return;
            }
        }
    })
}

// A process group that a child of the server leads. Dropping it kills the group.
struct Group(Option<u32>);

impl Group {
    fn signal(&self, signal: libc::c_int) {
        if let Some(pgid) = self.0 {
            // SAFETY: killpg only sends a signal; the group leader is not reaped yet, so the id
            // still names that group.
            unsafe { libc::killpg(pgid as libc::pid_t, signal) };
        }
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        self.signal(libc::SIGKILL);
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
async fn main() {
    // By default a signal ends the process without running destructors, so the agents would
    // keep running.
    let signals = [
        SignalKind::terminate(),
        SignalKind::interrupt(),
        SignalKind::hangup(),
    ]
    .map(|kind| signal(kind).expect("could not handle signals"));
    let [mut terminate, mut interrupt, mut hangup] = signals;

    let server = Server {
        channels: Arc::default(),
        arrived: Arc::new(Notify::new()),
        stop: CancellationToken::new(),
        workers: TaskTracker::new(),
        tool_router: Server::tool_router(),
    };
    let serve = async {
        let mut server = server.clone();
        // Offer a tool only for the agents that are installed and signed in.
        let (claude, codex, grok) = tokio::join!(
            Agent::Claude.ready(),
            Agent::Codex.ready(),
            Agent::Grok.ready()
        );
        for (agent, ready) in AGENTS.into_iter().zip([claude, codex, grok]) {
            if !ready {
                server.tool_router.remove_route(agent.name());
            }
        }
        server.serve(stdio()).await?.waiting().await?;
        anyhow::Ok(())
    };
    let result = tokio::select! {
        result = serve => result,
        _ = terminate.recv() => Ok(()),
        _ = interrupt.recv() => Ok(()),
        _ = hangup.recv() => Ok(()),
    };
    server.shutdown().await;
    if let Err(error) = &result {
        eprintln!("Error: {error:?}");
    }
    // Returning from main waits for the thread that reads stdin, and nothing can stop that read.
    std::process::exit(if result.is_ok() { 0 } else { 1 });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> Server {
        Server {
            channels: Arc::default(),
            arrived: Arc::new(Notify::new()),
            stop: CancellationToken::new(),
            workers: TaskTracker::new(),
            tool_router: Server::tool_router(),
        }
    }

    #[tokio::test]
    async fn a_stopped_server_takes_no_message() {
        let server = server();
        server.shutdown().await;
        let args = MessageArgs {
            message: "x".into(),
            channel: None,
        };
        assert_eq!(
            server.send(Agent::Claude, args),
            Err("the server is stopping".to_string())
        );
        assert!(server.workers.is_empty());
    }

    #[tokio::test]
    async fn close_wakes_a_waiting_receive() {
        let server = server();
        let (prompts, _rx) = mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let channel = Channel {
            agent: Agent::Claude,
            prompts,
            stop,
            worker: tokio::spawn(async move { stopped.cancelled().await }),
            pending: 1,
            replies: VecDeque::new(),
        };
        server
            .channels
            .lock()
            .unwrap()
            .insert("abc123".into(), channel);

        let receive = server.receive(
            CancellationToken::new(),
            Parameters(ReceiveArgs {
                channels: vec!["abc123".into()],
            }),
        );
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
            .await
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(1), receive)
            .await
            .expect("receive was not woken by close");
        assert_eq!(received, Err("unknown channel: abc123".to_string()));
    }
}
