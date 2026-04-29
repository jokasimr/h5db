# SFTP SSH-Agent Authentication

> Status: Implemented. This note records the review that guided the feature and
> summarizes the implementation shape now present in `h5db`.

## Scope

This note captures:

- what `duckdb-sshfs` supports today
- how its ssh-agent authentication is implemented
- what parts of that design are worth copying
- what the simple implemented ssh-agent design looks like in `h5db`

The intent is to align with sensible DuckDB extension practice where it is
actually good, without depending on `sshfs` or inheriting avoidable complexity.

## Reviewed Implementations

### Current `h5db`

Relevant files:

- [src/h5_sftp_secrets.cpp](../../src/h5_sftp_secrets.cpp)
- [src/h5_remote_backend.cpp](../../src/h5_remote_backend.cpp)

Current `h5db` SFTP auth modes:

- password
- key file with optional passphrase
- SSH agent via `USE_AGENT`

Current `h5db` strengths that should be preserved:

- explicit secret validation
- explicit host verification via `KNOWN_HOSTS_PATH` or
  `HOST_KEY_FINGERPRINT`
- clear config resolution and connection caching

### `duckdb-sshfs`

Reviewed source at commit `8c4216e8f33d7f7c3d7c3a8f171856eb7a5ac8e6`:

- `src/ssh_secrets.cpp`
- `src/include/ssh_client.hpp`
- `src/ssh_client.cpp`
- `src/sshfs_filesystem.cpp`
- `src/ssh_config.cpp`

The extension also has a README that documents its auth model, but the source
code is the authoritative reference here because some README claims do not match
the implementation.

### libssh2 agent API

Reviewed local libssh2 headers/docs/source:

- `libssh2_agent_init`
- `libssh2_agent_connect`
- `libssh2_agent_list_identities`
- `libssh2_agent_get_identity`
- `libssh2_agent_userauth`
- `libssh2_agent_set_identity_path`

Important behavior:

- on Unix, libssh2 agent support uses `SSH_AUTH_SOCK`
- agent auth means authenticating with a key already loaded into the agent
- it does not mean unlocking `KEY_PATH` through the OS keychain

## What `sshfs` Supports

`sshfs` secret/config parameters include:

- `username`
- `password`
- `key_path`
- `use_agent`
- `port`
- `host`
- `hostname`

Notably, `sshfs` does not expose:

- `key_passphrase`
- `known_hosts_path`
- host fingerprint verification parameters like `h5db` does

It also reads SSH config files and can pick up values like `IdentityFile` from
`~/.ssh/config`.

## How `sshfs` Implements Authentication

In practice it has these auth branches:

- password
- key file
- ssh-agent
- an implicit fallback that tries ssh-agent if `SSH_AUTH_SOCK` exists

Its agent flow is straightforward libssh2 usage:

1. `libssh2_agent_init(session)`
2. `libssh2_agent_connect(agent)`
3. `libssh2_agent_list_identities(agent)`
4. iterate identities with `libssh2_agent_get_identity(...)`
5. try each identity with `libssh2_agent_userauth(...)`
6. stop on first success

So yes: `sshfs` tries all loaded agent identities. It does not appear to select
one by fingerprint/comment, and it does not use
`libssh2_agent_set_identity_path(...)` for a custom socket path.

## Conclusions From Reviewing `sshfs`

### What is worth copying

- explicit ssh-agent support as a real auth mode
- direct use of libssh2's agent API
- agent auth meaning "use a key already loaded into the agent"

### What is not worth copying

- implicit fallback to ssh-agent whenever `SSH_AUTH_SOCK` is set
- SSH config parsing
- weaker host verification semantics
- loose secret validation

There is also a concrete inconsistency in `sshfs`: parts of the implementation
support `use_agent`, but higher-level config validation still appears oriented
toward "password or key path". That is exactly the kind of partial feature
integration `h5db` should avoid.

## Implemented `h5db` Design

### Principle

Keep the existing `h5db` model and add one explicit third auth mode.

Do not redesign SFTP auth around agent support.

### Auth modes

`h5db` supports exactly one of:

- `PASSWORD`
- `KEY_PATH`
- `USE_AGENT`

That means:

- `KEY_PASSPHRASE` remains meaningful only with `KEY_PATH`
- agent mode is separate from key-file mode
- there is no implicit fallback to agent auth

This is the simplest model that stays explicit and easy to validate.

### Secret shape

Keep the existing host verification requirements:

- `KNOWN_HOSTS_PATH` or `HOST_KEY_FINGERPRINT`

Keep:

- `USERNAME`
- `PORT`
- `HOST_KEY_ALGORITHMS`

Add:

- `USE_AGENT` as a boolean named parameter

For the minimal version, do **not** add:

- SSH config parsing
- a custom agent socket path parameter
- an identity selector

### Runtime behavior

In agent mode:

- `h5db` uses libssh2's agent support
- on Unix-like systems, libssh2 resolves the agent via `SSH_AUTH_SOCK`
- on Windows, libssh2 uses its supported agent backends
- `h5db` iterates loaded identities and tries them until one succeeds
- remote SSH authentication is run with the session in nonblocking mode, using
  the same interruptible socket-wait helper as handshake and SFTP setup
- local SSH-agent setup and identity enumeration are ordinary local calls; only
  the remote `libssh2_agent_userauth(...)` step uses the nonblocking session loop

This remains a reasonable shape because:

- it matches the practical approach in `sshfs`
- it keeps the implementation small
- remote network waits remain interruptible without making local agent IPC more
  complicated than needed

### Why this is acceptable

The main downside of trying all identities is that some servers enforce a low
authentication-attempt limit. If a user has many keys loaded, the right key may
be tried too late.

That is a real limitation, but it is acceptable for the simplest first
implementation. If this becomes a practical problem, the next feature to add is
an identity selector, not SSH config parsing or implicit fallback behavior.

## Implemented Shape

### 1. Secret validation

`src/h5_sftp_secrets.cpp` validates:

- `USERNAME` is required
- exactly one of `PASSWORD`, `KEY_PATH`, or `USE_AGENT`
- `KEY_PASSPHRASE` is only allowed with `KEY_PATH`
- `KNOWN_HOSTS_PATH` or `HOST_KEY_FINGERPRINT` is still required

This keeps the feature coherent.

### 2. Resolved config

`src/h5_remote_backend.cpp` uses an explicit auth method enum:

- `PASSWORD`
- `KEY_FILE`
- `SSH_AGENT`

This is cleaner than inferring auth mode from optional fields everywhere.

### 3. Authentication

`H5SftpConnection::Authenticate()` is split into small branches:

- password auth
- key-file auth
- agent auth

The agent branch:

1. initialize the agent
2. connect to it
3. list identities
4. try identities one by one with nonblocking remote SSH userauth
5. disconnect/free the agent
6. throw a clear error if no identity works

### 4. Connection cache key

The connection cache key includes the auth mode, so password, key-file, and
agent-based configs for the same host/user do not collide incorrectly.

### 5. Testing

The implemented coverage includes:

- agent-auth success
- missing `SSH_AUTH_SOCK`
- no identities loaded in the agent
- wrong agent-loaded key
- first identity fails, later identity succeeds
- secret validation for invalid auth-mode combinations

This proves that agent mode works without storing a key passphrase in the
secret.

Runtime coverage note:

- the repo's real agent interaction test is Unix-oriented because the harness
  uses `ssh-agent -s` and `ssh-add`
- Windows agent support is implemented through libssh2's supported backends,
  but is not currently exercised by this repository's runtime interaction tests

## Final Notes

The implemented `h5db` design intentionally does **not** include:

- SSH config parsing
- implicit fallback to agent auth
- a custom agent socket path parameter
- identity selection

That keeps the feature small, coherent, and maintainable without inheriting the
looser parts of `sshfs`'s design.
