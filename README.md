# agentchan

An MCP server that lets one coding agent talk to another: Claude Code, Codex or Grok.
It gives one tool for each agent that is installed and signed in, plus two tools that work
like Go channels:

- `claude(message, channel?)`, `codex(…)`, `grok(…)` send a message and return a channel id at
  once. Without `channel`, the message starts a new session. With `channel`, it is a follow-up.
- `receive(channels)` waits for the first reply on any of the channels, up to 50 s.
- `close(channel)` stops the agent and ends the channel.

Each channel is one agent session. Each message runs `claude -p`, `codex exec` or `grok -p`
in the server's working directory, with permission checks off, and with the settings that agent already has.

## Run

```sh
cargo build --release
```

The server offers every agent that is installed and signed in when it starts.

```sh
# Claude Code
claude mcp add --scope user agentchan -- ~/Code/agentchan/target/release/agentchan
```

```toml
# Codex: ~/.codex/config.toml
[mcp_servers.agentchan]
command = "/home/dariusm/Code/agentchan/target/release/agentchan"
```

## Test

```sh
cargo test                 # fake agents, no plan limits used
cargo test -- --ignored    # the real CLIs that are signed in
```

## Cost

Each message uses the called agent's own login and plan limits.
Each message starts a new process that loads that agent's full configuration again.
