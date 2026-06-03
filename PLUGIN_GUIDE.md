# reyn-broker Plugin Guide

reyn-broker のプラグインシステムは、broker のコアを変更せずにドメイン固有のゲートウェイや自動化ロジックを追加するための仕組みです。

Telegram ブリッジや GitHub CI ウォッチャーのように、**外部サービス ↔ broker セッション**を仲介するコンポーネントをプラグインとして実装できます。

---

## 目次

1. [アーキテクチャ](#アーキテクチャ)
2. [最小プラグインの実装](#最小プラグインの実装)
3. [コマンドの宣言（@command）](#コマンドの宣言command)
4. [BrokerClient リファレンス](#brokerclient-リファレンス)
5. [BrokerPlugin リファレンス](#brokerplugin-リファレンス)
6. [セッションイベント購読](#セッションイベント購読)
7. [プラグインのパッケージング](#プラグインのパッケージング)
8. [broker へのプラグイン登録・起動](#broker-へのプラグイン登録起動)
9. [バンドルプラグイン](#バンドルプラグイン)
10. [設計原則](#設計原則)

---

## アーキテクチャ

```
reyn-broker (core)              ← domain-agnostic メッセージルーター
      ↕ MCP
 BrokerPlugin (plugin_base.py)  ← 接続・登録・inbox drain・再接続を共通化
      ↕ (継承)
 plugins/telegram.py            ← Telegram ↔ broker ゲートウェイ
 plugins/github_ci_watcher.py   ← GitHub Actions CI poll → broker リレー
 my_plugin.py                   ← あなたのプラグイン
```

各プラグインは broker に **MCP セッションとして接続**します。broker はプラグインが何をするかを知りません。

---

## 最小プラグインの実装

```python
# my_plugin.py
import asyncio
from plugin_base import BrokerClient, BrokerPlugin, command


class EchoPlugin(BrokerPlugin):
    session_id = "echo"           # broker 上のセッション ID
    role = "echo back every message"

    @command(description="Echo a message back to the sender")
    async def echo(self, text: str, broker: BrokerClient) -> str:
        return f"echo: {text}"


def main() -> None:
    asyncio.run(EchoPlugin().run())


if __name__ == "__main__":
    main()
```

他セッションからの呼び出し:
```python
post_message(to="echo", from_session="alice", message="echo:hello world")
# → alice に "echo: hello world" が届く
```

実行:
```bash
/path/to/broker/.venv/bin/python my_plugin.py
```

---

## コマンドの宣言（@command）

`@command` デコレータでメソッドをプラグインの公開コマンドとして宣言します。

```python
@command(description="短い説明文")
async def command_name(self, arg1: str, arg2: str, broker: BrokerClient) -> str:
    # 処理
    return "返信テキスト（None または "" で返信抑制）"
```

### メッセージフォーマット

呼び出し元は以下の形式でメッセージを送ります:

```
"command_name:arg1 arg2 ..."
```

`arg1`, `arg2` は位置引数として順番に渡されます。メソッドの引数より少ない場合は `""` で補完されます。

```python
# 引数1つ
@command(description="Watch a PR")
async def watch(self, pr_number: str, broker: BrokerClient) -> str: ...
# → message="watch:#1268"

# 引数なし
@command(description="List watched PRs")
async def list(self, broker: BrokerClient) -> str: ...
# → message="list"  (引数なしコマンドは "command_name" のみでOK)

# 引数2つ
@command(description="Set threshold for a session")
async def threshold(self, session_id: str, seconds: str, broker: BrokerClient) -> str: ...
# → message="threshold:lead-coder 900"
```

### コマンドの発見

他セッションはコマンド体系を問い合わせできます:

```python
get_plugin_commands("echo")
# → [{"name": "echo", "description": "Echo a message back", "args": ["text"]}]
```

### `broker` 引数の規約

`@command` で宣言するメソッドは必ず `broker: BrokerClient` を受け取ってください。これはフレームワークが自動的に注入します。省略すると `TypeError` が発生し、呼び出し元にエラー返信が届きます。

```python
# ✅ 正しい
async def watch(self, pr_number: str, broker: BrokerClient) -> str: ...

# ❌ broker 引数が無い
async def watch(self, pr_number: str) -> str: ...
```

### 呼び出し元 session id を受け取る（`sender`）

コマンドの中で「誰が呼んだか」を知る必要がある場合（サブスクリプション管理など）は、`sender: str = ""` を追加してください。フレームワークが呼び出し元の session id を自動で注入します。`sender` はユーザー供給の引数ではないためコマンドスキーマには現れません。

```python
@command(description="Subscribe to events for a repo")
async def watch(self, repo: str, broker: BrokerClient, sender: str = "") -> str:
    self._subscribers[repo].add(sender)
    return f"subscribed {sender} to {repo}"
```

### on_broker_message へのフォールバック

どのコマンドにもマッチしないメッセージは `on_broker_message` に渡されます。自由形式のメッセージや help レスポンスの実装に使います。

---

## BrokerClient リファレンス

すべてのライフサイクルフックに `broker: BrokerClient` として渡されます。`from_session` は自動設定されます。

### メッセージング

```python
await broker.post(to="lead-coder", message="done")
await broker.broadcast(message="all hands")
sessions = await broker.list_sessions(compact=True)
msgs = await broker.peek(limit=5)
```

### セッションイベント購読

```python
await broker.subscribe_events(
    event_types=["posted", "registered", "unregistered"],
    session_filter=None,  # None = 全セッション
)
```

`list_sessions` ポーリングの代わりにイベント購読を使うことでブローカー負荷を下げられます。イベントは `on_broker_message` に届きます（`msg.get("event")` でイベント種別を確認）。

### ポーリング制御

```python
broker.start_poll(interval=60)  # on_poll を N秒毎に有効化（間隔変更も可）
broker.stop_poll()               # on_poll を一時停止
```

---

## BrokerPlugin リファレンス

### クラス属性（必須）

| 属性 | 説明 |
|---|---|
| `session_id` | broker 上のセッション ID |
| `role` | `list_sessions` に表示される短い説明 |

### クラス属性（任意）

| 属性 | デフォルト | 説明 |
|---|---|---|
| `broker_url` | `http://127.0.0.1:8765/mcp` | `BROKER_URL` 環境変数で上書き可 |
| `inbox_interval` | `30` | inbox drain 間隔（秒） |
| `poll_interval` | `None` | 設定すると起動時から `on_poll` が有効になる |
| `reconnect_seconds` | `10` | 接続失敗後の再接続待ち時間（秒） |

### ライフサイクルフック

#### `on_start(broker: BrokerClient) -> None`
broker への接続・登録完了後に一度だけ呼ばれます。

```python
async def on_start(self, broker: BrokerClient) -> None:
    await broker.post(to="lead-coder", message="plugin started")
    broker.start_poll(60)  # ポーリングを有効化
```

#### `on_broker_message(msg: dict, broker: BrokerClient) -> None`
`@command` にマッチしないメッセージに対するフォールバック。`msg` は `{"from": "...", "message": "..."}` を含みます。

```python
async def on_broker_message(self, msg, broker):
    await broker.post(to=msg["from"], message="unknown command")
```

#### `on_poll(broker: BrokerClient) -> None`
ポーリングが有効な間、`poll_interval` 秒毎に呼ばれます。ループを書かないこと（基底クラスが繰り返し呼ぶ）。

```python
async def on_poll(self, broker: BrokerClient) -> None:
    status = await asyncio.to_thread(check_external_api)
    if status_changed(status):
        await broker.broadcast(f"status: {status}")
```

---

## セッションイベント購読

broker の `subscribe_session_events` を使うと、セッションの活動変化を inbox 経由で受け取れます。`list_sessions` のポーリングが不要になります。

```python
# 購読（broker MCP ツールを直接呼ぶ）
subscribe_session_events(
    subscriber_id="my-plugin",
    event_types=["registered", "unregistered", "posted"],
    session_filter=None,  # None = 全セッション
)
```

届くメッセージ形式:
```json
{"from": "broker", "event": "posted", "session_id": "lead-coder", "at": "2026-06-03T..."}
```

`session_filter` で監視対象を絞れます。除外したいセッション（broker/telegram 等）は subscriber 側でフィルタします。

イベントタイプ:

| イベント | トリガー |
|---|---|
| `registered` | `register_session` / `startup_summary` が呼ばれた |
| `unregistered` | `unregister_session` が呼ばれた / TTL 期限切れ |
| `posted` | `post_message` / `broadcast_message` が呼ばれた |

### peer stall watcher での活用例

```python
class StallWatcherPlugin(BrokerPlugin):
    session_id = "stall-watcher"
    role = "peer stall detection"

    EXCLUDE = {"broker", "telegram", "stall-watcher"}
    STALL_SECONDS = 900

    def __init__(self):
        self._last_seen: dict[str, datetime] = {}

    async def on_start(self, broker: BrokerClient) -> None:
        # list_sessions polling の代わりにイベント購読
        await broker._cs.call_tool("subscribe_session_events", {
            "subscriber_id": self.session_id,
            "event_types": ["posted"],
        })
        broker.start_poll(60)

    async def on_broker_message(self, msg, broker):
        if msg.get("event") == "posted":
            sid = msg.get("session_id", "")
            if sid not in self.EXCLUDE:
                self._last_seen[sid] = datetime.fromisoformat(msg["at"])

    async def on_poll(self, broker: BrokerClient) -> None:
        now = datetime.now(timezone.utc)
        for sid, last in self._last_seen.items():
            silent = (now - last).total_seconds()
            if silent > self.STALL_SECONDS:
                await broker.post(
                    to="backlog-watcher",
                    message=f"PEER_STALL: {sid} silent={int(silent//60)}min",
                )
```

---

## プラグインのパッケージング

### スタンドアロンスクリプト（最シンプル）

```python
# my_plugin.py
...
if __name__ == "__main__":
    asyncio.run(MyPlugin().run())
```

### pyproject.toml に entry point を追加

reyn-broker のバンドルプラグインとして追加する場合:

```toml
[project.scripts]
reyn-broker-myplugin = "plugins.my_plugin:main"
```

独立パッケージとして配布する場合:

```toml
[project.entry-points."reyn_broker.plugins"]
my-plugin = "my_package.plugin:MyPlugin"
```

---

## broker へのプラグイン登録・起動

broker MCP 経由でプラグインを動的に管理できます（再起動不要）。

```python
# 登録
plugin_add(
    name="my-plugin",
    command="/path/to/.venv/bin/python /path/to/my_plugin.py",
    session_id="my-plugin",
    env={"MY_API_KEY": "xxx"},
    auto_start=True,      # broker 起動時に自動起動
)

# 起動・停止・再起動・削除
plugin_start("my-plugin")
plugin_stop("my-plugin")
plugin_restart("my-plugin")
plugin_remove("my-plugin")

# 状態確認
plugin_list()
# → [{"name": "my-plugin", "pid": 12345, "running": True, "connected": True, ...}]
```

登録情報は state ファイルに永続化されます。`auto_start=True` のプラグインは broker 起動時に自動起動します。

### クラッシュ時の自動再起動

`auto_start=True` のプラグインは broker が 10 秒ごとに死活確認し、クラッシュしていれば自動再起動します。監視プラグイン（stall watcher、CI watcher 等）が静かに死んだままになることを防ぎます。

### ログ

プラグインの stderr は以下に追記されます:

```
~/.local/state/reyn-broker/plugins/<name>.log
```

クラッシュ時の診断はここを確認してください。

---

## バンドルプラグイン

### `plugins/telegram.py` — Telegram ブリッジ

スマートフォンから broker セッションとやり取りするための双方向ゲートウェイ。

必要な環境変数: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

```python
plugin_add(
    name="telegram",
    command="/path/to/.venv/bin/reyn-broker-telegram",
    session_id="telegram",
    auto_start=True,
)
```

### `plugins/github_ci_watcher.py` — GitHub Actions CI ウォッチャー

GitHub PR のチェック結果（GitHub Actions）を監視し、状態変化を broker メッセージとして通知します。

必要条件: `gh` CLI がインストールされ認証済み (`gh auth login`)

```python
plugin_add(
    name="github-ci",
    command="/path/to/.venv/bin/reyn-broker-github-ci",
    session_id="github-ci",
    auto_start=True,
)
```

コマンド:
```python
post_message(to="github-ci", from_session="me", message="watch:#1268")
# → "✅ CI #1268: SUCCESS" が届く
post_message(to="github-ci", from_session="me", message="unwatch:#1268")
post_message(to="github-ci", from_session="me", message="list")
```

複数セッションが同じ PR を watch した場合、全員に通知が届きます。

### `peer_stall_watcher.py` — Peer Stall 検出

セッションの活動を `subscribe_session_events` でリアルタイム追跡し、一定時間沈黙しているセッションを検出して通知します。`list_sessions` ポーリング不要。

環境変数: `PEER_STALL_THRESHOLD_S`（デフォルト 900s）、`PEER_STALL_NOTIFY`（デフォルト `backlog-watcher`）、`PEER_STALL_EXCLUDE`（除外セッション、カンマ区切り）

```python
plugin_add(
    name="peer-stall-watcher",
    command="/path/to/.venv/bin/reyn-broker-peer-stall",
    session_id="peer-stall-watcher",
    auto_start=True,
)
```

### `plugins/github_pr_watcher.py` — GitHub PR 状態監視

GitHub PR の open/merged/closed と mergeStateStatus (CLEAN/DIRTY) の変化を監視し、サブスクライバーに通知します。

必要条件: `gh` CLI がインストールされ認証済み

```python
plugin_add(
    name="github-pr-watcher",
    command="/path/to/.venv/bin/reyn-broker-github-pr",
    session_id="github-pr-watcher",
    auto_start=True,
)
```

コマンド:
```python
post_message(to="github-pr-watcher", from_session="me", message="watch:tya5/reyn")
# → pr_opened / pr_merged / pr_clean 等のイベントが届く
post_message(to="github-pr-watcher", from_session="me", message="unwatch:tya5/reyn")
post_message(to="github-pr-watcher", from_session="me", message="list")
```

---

## 設計原則

1. **broker はドメインを知らない** — コマンドの意味を broker は解釈しない。broker は保存・配信するだけ。
2. **プラグインは独立プロセス** — broker と別プロセスで動作。broker の再起動はプラグインに影響しない（自動再接続）。
3. **LLM コストゼロの処理は Python ループに** — 監視・ポーリング・判定は Python で完結。LLM は confirmed な結果のみ受け取る。
4. **`session_id` は一意に** — 同じ `session_id` のプラグインを複数起動すると inbox が競合する。
5. **`@command` でインターフェースを宣言する** — `on_broker_message` で手書きパーサーを書かない。発見可能性と保守性のために `@command` を使う。
