# reyn-broker Plugin Guide

reyn-broker のプラグインシステムは、broker のコアを変更せずにドメイン固有のゲートウェイや自動化ロジックを追加するための仕組みです。

Telegram ブリッジや GitHub CI ウォッチャーのように、**外部サービス ↔ broker セッション**を仲介するコンポーネントをプラグインとして実装できます。

---

## 目次

1. [アーキテクチャ](#アーキテクチャ)
2. [最小プラグインの実装](#最小プラグインの実装)
3. [BrokerPlugin リファレンス](#brokerplugin-リファレンス)
4. [プラグインのパッケージング](#プラグインのパッケージング)
5. [broker へのプラグイン登録・起動](#broker-へのプラグイン登録起動)
6. [バンドルプラグイン](#バンドルプラグイン)
7. [設計原則](#設計原則)

---

## アーキテクチャ

```
reyn-broker (core)        ← domain-agnostic メッセージルーター
      ↕ MCP
 BrokerPlugin (base)      ← 接続・登録・inbox drain・再接続を共通化
      ↕ (継承)
 plugins/telegram.py      ← Telegram ↔ broker ゲートウェイ
 plugins/ci_watcher.py    ← GitHub CI poll → broker リレー
 my_plugin/               ← あなたのプラグイン
```

各プラグインは broker に **MCP セッションとして接続**します。プラグインは通常の Claude Code セッションと区別がなく、broker はプラグインが何をするかを知りません。

---

## 最小プラグインの実装

```python
# my_plugin.py
import asyncio
from mcp import ClientSession
from plugin_base import BrokerPlugin


class EchoPlugin(BrokerPlugin):
    session_id = "echo"                        # broker 上のセッション ID
    role = "echo back every message"           # list_sessions で表示される説明

    async def on_message(self, msg: dict, cs: ClientSession) -> None:
        """受信メッセージを送信元に返す"""
        await cs.call_tool("post_message", {
            "to": msg["from"],
            "from_session": self.session_id,
            "message": f"echo: {msg['message']}",
        })


def main() -> None:
    asyncio.run(EchoPlugin().run())


if __name__ == "__main__":
    main()
```

実行:
```bash
/path/to/broker/.venv/bin/python my_plugin.py
```

---

## BrokerPlugin リファレンス

### クラス属性（オーバーライド必須）

| 属性 | 型 | 説明 |
|---|---|---|
| `session_id` | `str` | broker 上のセッション ID（他セッションがメッセージを送る宛先） |
| `role` | `str` | `list_sessions` に表示される短い説明 |

### クラス属性（オーバーライド任意）

| 属性 | デフォルト | 説明 |
|---|---|---|
| `broker_url` | `http://127.0.0.1:8765/mcp` | 接続先 broker URL（`BROKER_URL` 環境変数で上書き可） |
| `poll_seconds` | `30` | inbox drain 間隔（秒） |
| `reconnect_seconds` | `10` | 接続失敗後の再接続待ち時間（秒） |

### オーバーライドするメソッド

#### `on_connected(cs: ClientSession) -> None`
broker への接続・登録完了後に一度だけ呼ばれます。初期化処理（ウェルカムメッセージ送信、初期状態読み込み等）に使います。

```python
async def on_connected(self, cs: ClientSession) -> None:
    await cs.call_tool("post_message", {
        "to": "lead-coder",
        "from_session": self.session_id,
        "message": "MyPlugin started and ready.",
    })
```

#### `on_message(msg: dict, cs: ClientSession) -> None`
inbox にメッセージが届くたびに呼ばれます。`msg` には最低限 `"from"` と `"message"` キーが含まれます。

```python
async def on_message(self, msg: dict, cs: ClientSession) -> None:
    text = msg.get("message", "")
    sender = msg.get("from", "?")
    if text == "ping":
        await cs.call_tool("post_message", {
            "to": sender,
            "from_session": self.session_id,
            "message": "pong",
        })
```

#### `run_tasks(cs: ClientSession) -> list[asyncio.Task]`
inbox loop と並行して実行する追加タスクを返します。外部 API のポーリング、WebSocket 接続の維持などに使います。broker 接続が切れると全タスクがキャンセルされます。

```python
async def run_tasks(self, cs: ClientSession) -> list[asyncio.Task]:
    return [
        asyncio.create_task(self._poll_external_api(cs)),
        asyncio.create_task(self._heartbeat_loop(cs)),
    ]

async def _poll_external_api(self, cs: ClientSession) -> None:
    while True:
        await asyncio.sleep(60)
        data = await fetch_something()
        if data_changed(data):
            await cs.call_tool("broadcast_message", {
                "from_session": self.session_id,
                "message": f"Update: {data}",
            })
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

reyn-broker の `pyproject.toml` に追加する場合（バンドルプラグイン）:

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

プラグインを broker MCP 経由で管理できます（再起動不要）。

### 登録

```python
plugin_add(
    name="my-plugin",
    command="/path/to/.venv/bin/python /path/to/my_plugin.py",
    session_id="my-plugin",        # プラグインが register_session で使う session_id
    env={"MY_API_KEY": "xxx"},     # 追加の環境変数（broker の env とマージ）
    auto_start=True,               # broker 起動時に自動起動
)
```

### 起動・停止・再起動

```python
plugin_start("my-plugin")      # サブプロセス起動
plugin_stop("my-plugin")       # SIGTERM 送信
plugin_restart("my-plugin")    # stop + start
```

### 状態確認

```python
plugin_list()
# → [{
#     "name": "my-plugin",
#     "session_id": "my-plugin",
#     "pid": 12345,
#     "running": True,       # プロセスが生きているか
#     "connected": True,     # broker session に登録済みか
#     "auto_start": True,
#   }, ...]
```

### 削除

```python
plugin_remove("my-plugin")    # 停止 + レジストリから削除
```

登録情報は broker の state ファイルに永続化されます。broker を再起動しても登録は維持されます。`auto_start=True` のプラグインは broker 起動時に自動で起動します。

---

## バンドルプラグイン

reyn-broker には以下のプラグインが同梱されています。

### `plugins/telegram.py` — Telegram ブリッジ

スマートフォンから broker セッションとやり取りするための双方向ゲートウェイ。

必要な環境変数:
- `TELEGRAM_BOT_TOKEN` — @BotFather から取得したトークン
- `TELEGRAM_CHAT_ID` — あなたの Telegram ユーザー ID

登録例:
```python
plugin_add(
    name="telegram",
    command="/path/to/.venv/bin/reyn-broker-telegram",
    session_id="telegram",
    auto_start=True,
)
```

詳細は `telegram_bridge.py` のドキュメントを参照してください。

### `plugins/ci_watcher.py` — GitHub CI ウォッチャー

`gh` CLI 経由で GitHub PR のチェック状態を監視し、変化があった場合に broker メッセージとして通知します。

必要条件:
- `gh` CLI がインストールされ認証済みであること

登録例:
```python
plugin_add(
    name="ci-watcher",
    command="/path/to/.venv/bin/reyn-broker-ci",
    session_id="ci-watcher",
    auto_start=True,
)
```

使用方法（PR の監視開始）:
```python
post_message(
    to="ci-watcher",
    from_session="my-session",
    message="watch:#1268",
)
# 状態変化時に "✅ CI #1268: SUCCESS" がmy-session に届く

post_message(to="ci-watcher", from_session="my-session", message="unwatch:#1268")
post_message(to="ci-watcher", from_session="my-session", message="list")
```

---

## 設計原則

1. **broker はドメインを知らない** — `waiting_for: ci:#1268` の意味を broker は解釈しない。broker は保存・配信するだけ。
2. **プラグインは独立プロセス** — broker と別プロセスで動作し、MCP セッションとして接続。broker の再起動はプラグインに影響しない（自動再接続）。
3. **トークンコスト 0 の処理は Python ループに** — LLM は confirmed な結果のみ受け取る。監視・ポーリング・判定は Python で完結させる。
4. **`session_id` は一意に** — 同じ `session_id` のプラグインを複数起動すると inbox が競合する。1 session につき 1 プロセス。
5. **broker_url を環境変数で上書き可能に** — `BROKER_URL` 環境変数を使えば、異なる broker インスタンスに接続できる。
