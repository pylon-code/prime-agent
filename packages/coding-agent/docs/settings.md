# Settings

Prime Agent stores its settings in `settings.json`. By default this file is at:
- macOS/Linux: `~/.prime-agent/settings.json`
- Windows: `%USERPROFILE%\.prime-agent\settings.json`

You can also specify a project-level settings file in your project root:
`.prime-agent/settings.json`

Project settings override global settings, except for lists and objects which are merged.

## Configuration Options

### Models & Thinking

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `defaultProvider` | string | none | Default AI provider to use |
| `defaultModel` | string | none | Default model ID |
| `defaultThinkingLevel` | string | `"low"` | Default thinking level: `"off"`, `"minimal"`, `"low"`, `"medium"`, `"high"`, `"xhigh"`, or `"max"` |
| `defaultServiceTier` | string | none | Default service tier for providers that support it (e.g. Anthropic's `"auto"` or `"priority"`) |
| `thinkingBudgets` | object | none | Custom token budgets for thinking levels: `minimal`, `low`, `medium`, `high`, `xhigh` |
| `enabledModels` | string[] | none | Whitelist of model patterns for cycling/selection |

```json
{
  "defaultProvider": "anthropic",
  "defaultModel": "claude-3-7-sonnet-20250219",
  "defaultThinkingLevel": "low",
  "thinkingBudgets": {
    "low": 4096,
    "medium": 8192
  }
}
```

### Context & Compaction

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `compaction.enabled` | boolean | `true` | Enable automatic context compaction |
| `compaction.reserveTokens` | number | `16384` | Tokens reserved for output and tool use |
| `compaction.keepRecentTokens` | number | `20000` | Tokens of recent messages to preserve during compaction |
| `compaction.customInstructions` | string | none | Additional instructions for the summarizer |

```json
{
  "compaction": {
    "enabled": true,
    "reserveTokens": 16384,
    "keepRecentTokens": 20000
  }
}
```

### Auto Refinement

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `autoRefine.enabled` | boolean | `false` | Enable automatic prompt refinement |
| `autoRefine.minIntervalMs` | number | `60000` | Minimum interval between automated refinement checks (1 minute) |
| `autoRefine.cooldownMs` | number | `1200000` | Cooldown period after refinement triggers (20 minutes) |

```json
{
  "autoRefine": {
    "enabled": true,
    "minIntervalMs": 60000,
    "cooldownMs": 1200000
  }
}
```

### Subagents (RLM)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `rlmMaxDepth` | number | `2` | Maximum nesting depth for subagents (0 = disabled, max = 5) |

```json
{
  "rlmMaxDepth": 2
}
```

### Auto Retry

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `retry.enabled` | boolean | `true` | Enable automatic agent-level retry on transient errors |
| `retry.maxRetries` | number | `3` | Maximum agent-level retry attempts |
| `retry.baseDelayMs` | number | `2000` | Base delay for agent-level exponential backoff (2s, 4s, 8s) |
| `retry.provider.timeoutMs` | number | SDK default | Provider/SDK request timeout in milliseconds |
| `retry.provider.maxRetryDelayMs` | number | `60000` | Max server-requested retry delay before failing (60s) |

When a provider requests a retry delay longer than `retry.provider.maxRetryDelayMs` (e.g. a usage-limit reset hours away), auto-retry stops immediately with an informative error instead of waiting. Set to `0` to disable the cap.

### Wait-for-usage and provider recovery

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `retry.provider.waitForUsage.enabled` | boolean | `true` | Bounded wait-for-recovery loop for quota exhaustion and provider unavailability |
| `retry.provider.waitForUsage.baseDelayMs` | number | `1000` | First ping delay (doubles per ping) |
| `retry.provider.waitForUsage.maxDelayMs` | number | `300000` | Per-ping ceiling (5m) |
| `retry.provider.waitForUsage.maxAttempts` | number | `30` | Abort bound: maximum recovery pings |
| `retry.provider.waitForUsage.maxWaitMs` | number | `900000` | Abort bound: maximum total wait (15m) |
| `retry.provider.waitForUsage.pauseUntilReset` | boolean | `true` | Park quota-blocked sessions until the provider-reported reset instead of dying mid-task |
| `retry.provider.waitForUsage.maxPauseMs` | number | `86400000` | Abort bound: maximum single park (24h; clamped to 7d) |
| `retry.provider.waitForUsage.maxParks` | number | `8` | Abort bound: maximum parks per quota episode |
| `providerBackupModel` | string | none | Backup model ("provider/model-id" or bare id) used while the primary is quota-blocked or unavailable |

The wait loop runs under the `retry.enabled` master switch: with retries
disabled, no waits run either.

When a request fails with quota/subscription exhaustion (429s, usage limits), the
session waits for usage to come back: it pings the provider with exponential
backoff and jitter (1s doubling to a 5m ceiling) and resumes automatically when
the provider recovers. If the provider reports a reset time (Retry-After header
or "Try again in ~90 min" style text), the resume is scheduled exactly then
instead of pinging. Quick retries still run first for transient errors (5xx,
overload, network, and 404 routing blips); the wait loop takes over when they
are exhausted. Every wait shows attempts and the next check countdown in the
status line, and both abort bounds (`maxAttempts`, `maxWaitMs`) are hard stops:
waits never hang. When a reported reset time exceeds `maxWaitMs`, the wait gives
up immediately with an informative error instead of pinging pointlessly — raise
`maxWaitMs` to wait out long subscription windows.

When `pauseUntilReset` is on (the default) and such a reset is reported — e.g.
the ChatGPT-plan "Try again in ~7272 min" 429 — the session does not die
mid-task: it parks. The turn ends cleanly with a "parked until ..." status, the
park/resume transitions are recorded in the session log, and one durable
one-shot scheduled job (visible via `/cron`) wakes the session at the reset
time — or sooner when `maxPauseMs` caps the park. While parked the session
itself makes no model calls. The wake delivers an
in-context marker telling the model the pause happened and to continue the
interrupted task; that turn's single model call probes the quota. If the quota
is back, the task resumes with its context. If not, the session re-parks with
the newly reported reset, bounded by `maxPauseMs` per park and `maxParks` per
quota episode; when the budget is spent, it aborts exactly like the bounded
wait it replaced. Parks apply at the session level (subagents included), only
for quota failures with a provider-reported reset, and only when no backup
model took over; a `maxPauseMs` above 7 days is clamped. Set
`pauseUntilReset: false` to keep the pre-park behavior of failing immediately.

`providerBackupModel` routes failed turns to a user-defined backup model
instead of waiting while the primary is quota-blocked or unavailable. It is
disabled by default: with no setting, behavior is unchanged and requests never
silently switch models. When set, the retry status line shows an explicit
"retrying on backup model X" indicator, the switch is recorded in the session
log, and the session returns to the primary model automatically (the next turn
probes the primary again). If the backup reference cannot be resolved to an
available, authenticated model, the bounded wait runs instead.

```json
{
  "retry": {
    "enabled": true,
    "maxRetries": 3,
    "baseDelayMs": 2000,
    "provider": {
      "timeoutMs": 3600000,
      "maxRetryDelayMs": 60000,
      "waitForUsage": {
        "enabled": true,
        "baseDelayMs": 1000,
        "maxDelayMs": 300000,
        "maxAttempts": 30,
        "maxWaitMs": 900000,
        "pauseUntilReset": true,
        "maxPauseMs": 86400000,
        "maxParks": 8
      }
    }
  },
  "providerBackupModel": "anthropic/claude-opus-4-7"
}
```

### Message Delivery

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `steeringMode` | string | `"one-at-a-time"` | How steering messages are sent: `"all"` or `"one-at-a-time"` |
| `followUpMode` | string | `"one-at-a-time"` | How follow-up messages are sent: `"all"` or `"one-at-a-time"` |
| `transport` | string | `"sse"` | Preferred transport for providers that support multiple transports: `"sse"`, `"websocket"`, or `"auto"` |

### Terminal & Images

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `terminal.showImages` | boolean | `true` | Show image type and dimensions in terminal |
| `terminal.clearOnShrink` | boolean | `false` | Clear empty rows when content shrinks (can cause flicker) |
| `images.autoResize` | boolean | `true` | Resize images to 2000x2000 max |
| `images.blockImages` | boolean | `false` | Block all images from being sent to LLM |

### Shell

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `shellPath` | string | - | Custom shell path (e.g., for Cygwin on Windows) |
| `shellCommandPrefix` | string | - | Prefix for every bash command (e.g., `"shopt -s expand_aliases"`) |
| `npmCommand` | string[] | - | Command argv used for npm package lookup/install operations (e.g., `["mise", "exec", "node@20", "--", "npm"]`) |

```json
{
  "shellPath": "/bin/zsh",
  "shellCommandPrefix": "shopt -s expand_aliases",
  "npmCommand": ["pnpm"]
}
```

### UI & Themes

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `theme` | string | `"dark"` | Color theme name |
| `themes` | string[] | none | Paths to custom theme files/directories |
| `editorPaddingX` | number | `0` | Horizontal padding for input editor |
| `autocompleteMaxVisible` | number | `5` | Max visible items in autocomplete dropdown |
| `showHardwareCursor` | boolean | `false` | Show hardware terminal cursor |
| `quietStartup` | boolean | `false` | Suppress startup banner |

### Skills & Prompts

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `skills` | string[] | none | Paths to skill files/directories |
| `enableSkillCommands` | boolean | `true` | Register skills as `/skill:name` commands |
| `bundledSkills` | object | none | Configure bundled skills |
| `enableBuiltinSkills` | boolean | `true` | Enable built-in skills |
| `prompts` | string[] | none | Paths to prompt template files/directories |

### Extensions & MCP

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `extensions` | string[] | none | Paths to extension files/directories |
| `packages` | object[] | none | NPM/git package sources |
| `mcpServers` | object | none | MCP server configurations (see [MCP documentation](mcp.md)) |

### Session Management

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `sessionDir` | string | none | Custom directory for session storage |
| `idleEvictionMinutes` | number | `90` | Minutes of inactivity before evicting a session from memory (0 or `"off"` disables) |
