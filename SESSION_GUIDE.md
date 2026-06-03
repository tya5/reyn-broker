# セッション間通信 手順書

このドキュメントは、各 Claude Code セッションが broker MCP サーバー経由で
他セッションと通信するための共通手順です。セッションの所在ディレクトリは
任意 (`~/Workspace/...` 以外でもよい)。セッションは動的に増減するため、
自分の役割や相手の名前は固定で前提せず、ランタイムに `list_sessions` で
確認すること。

---

## 0. 前提

- broker は `/path/to/broker/server.py` で常駐している HTTP MCP サーバー
- このセッションの `.mcp.json` に `broker` が以下のように登録されているはず:

  ```json
  {
    "mcpServers": {
      "broker": {
        "type": "http",
        "url": "http://127.0.0.1:8765/mcp"
      }
    }
  }
  ```
- 未登録なら、上記を追加して Claude Code を再起動してから本手順を実行する

---

## 1. セッション開始時にやること

> **⚡ TL;DR — 起動時に必ずこの3ステップを実行すること**
> 1. `startup_summary` で登録 + peer 一覧取得
> 2. `Monitor` で session_watcher を起動（非同期受信を有効化）
> 3. `pending_messages` の内容を処理

---

### ステップ 1: startup_summary で登録

**自分の識別子を決める** — このセッションの作業ディレクトリ名(basename)を
そのまま `session_id` として使う。`working_dir` は作業ディレクトリの絶対パス。

```
session_id  = basename(pwd)
working_dir = pwd  (絶対パス)
```

`startup_summary` を 1 回呼ぶ（`register_session` + `list_sessions` を 1 回に統合）:

```
startup_summary(
    session_id="<basename>",
    working_dir="<absolute_path>",
    role="<短い役割説明>",   # 例: "PR review", "e2e tests"
)
→ {
    "status": "registered '<basename>' at <path>",
    "pending_messages": [ {"from": ..., "message": ...}, ... ],
    "sessions": [ {"session_id": ..., "role": ...}, ... ]   # compact 形式
  }
```

**戻り値の `pending_messages` を必ず確認する**。オフライン中に届いていた
メッセージはすべてここに入っており、登録と同時に drain される。
漏らさず読み、必要なら順番に処理すること。

`role` は optional。指定しておくと `list_sessions` の結果に含まれるので、
他セッションが役割を見て routing 判断できる。freeform 1 行で OK。

---

### ステップ 2: session_watcher Monitor を起動（必須）

**登録直後に必ず以下の Monitor を起動すること。** これを忘れると非同期受信が
機能せず、メッセージを受け取るために都度 `receive_messages` を手動で呼ぶ必要
が生じる（LLM トークンコストの無駄）。

```
Monitor(
  description="broker inbox watcher (session=<自分の session_id>)",
  command="/path/to/broker/.venv/bin/python /path/to/broker/session_watcher.py --session=<自分の session_id>",
  persistent=True,
  timeout_ms=3600000,
)
```

> **⚠️ 重要: 必ず broker の `.venv/bin/python` を使うこと。**
> 素の `python3` では `ModuleNotFoundError: No module named 'mcp'` で即落ちる。
> パス例: `/Users/yasudatetsuya/Workspace/reyn_dev/broker/.venv/bin/python`

Monitor 起動後、session_watcher が 30 秒毎に inbox をポーリングし、メッセージが
届いたら `<task-notification>` として自動でコンテキストに届く。**アイドル中の
ポーリングはトークンコストゼロ。**

---

### ステップ 3: pending_messages を処理

`startup_summary` の戻り値に `pending_messages` があれば順番に処理する。
処理後は送信元へ返信すること。

---

### broker restart 後の起動手順（Claude Code セッション再起動後）

Claude Code セッションを再起動した場合、以下を追加で実施:

```
# 1. broker の version 確認（schema 変更があれば ToolSearch で更新）
health_check()

# 2. 新ツールの schema 取得（broker 更新後は必ず実行）
ToolSearch(query="select:mcp__broker__health_check,mcp__broker__peek_messages,mcp__broker__startup_summary")

# 3. watcher の TaskList 確認（セッション再起動で Monitor が消えているので再起動）
TaskList()  # → 空なら Monitor を再起動（ステップ2参照）

# 4. startup_summary で再登録（再登録不要なら receive_messages で残留確認）
receive_messages(session_id="<自分の session_id>")
```

---

## 2. 他セッションを探す

固定のセッション名を仮定しない。`startup_summary` の戻り値 `sessions` を使うか、
改めて `list_sessions(compact=True)` を呼ぶ:

```
list_sessions(compact=True)
→ [{"session_id": "...", "role": "..." | null}, ...]
```

返ってきた一覧から、目的に合いそうな `session_id` を選ぶ。判断順:
1. `role` が指定されていればそれを最優先 (= 自己申告された役割)
2. `role` が null なら working_dir / session_id 名から推測
3. 一覧に該当が無ければ、その役割のセッションは今オフラインということ

---

## 3. メッセージを送る

```
post_message(
    to="<相手の session_id>",
    from_session="<自分の session_id>",
    message="<本文>"
)
→ "queued for '<相手>' (online=true|false)"
```

- メッセージは常に相手の inbox にキューされる(相手のオン/オフ問わず)
- 相手は自分で `receive_messages` を呼んで取り出す
- 返信を期待する場合は、メッセージ本文に「終わったら post_message で
  `<自分の session_id>` に返してください」と明示する

---

## 4. メッセージを受け取る

### watcher 稼働中（通常運用）

**watcher (session_watcher.py Monitor) が稼働中であれば `receive_messages` を自分で呼んではいけない。**

watcher がすでに 30 秒毎に inbox を drain しており、届いたメッセージは
`<task-notification>` として自動でコンテキストに届く。
`receive_messages` を追加で呼ぶと空のリストが返るだけで **MCP 呼び出しのトークンコストだけ発生する**。

watcher 稼働中に `receive_messages` を呼んでよいのは以下のみ:

| 状況 | 理由 |
|---|---|
| `startup_summary` 直後の初回 backlog | watcher 起動前なので `pending_messages` を確認 |
| watcher が落ちていたと判明した直後 | watcher の silent stall 中に溜まった backlog を回収 |
| ユーザーから「今すぐ inbox 確認して」と言われた | 次の 30 秒 poll を待てない緊急確認 |

```
# watcher 稼働中はこれを毎ターン呼んではいけない（トークン無駄）
receive_messages(session_id="<自分の session_id>")
```

受信した通知（`<task-notification>`）の処理:

1. event body を行単位で split、各行を 1 message として JSON parse
2. `_truncated: true` なら `_preview` で概要確認 → 必要なら `_full_path` を Read
3. 依頼内容のタスクを実行
4. 完了したら `post_message(to=data.from, from_session=自分, message=結果)` で返信

### watcher が落ちている場合（フォールバック）

```
# watcher が死んでいる場合のみ手動 drain
receive_messages(session_id="<自分の session_id>", fields=["from", "message"])
→ [{"from": "<送信元>", "message": "<本文>"}, ...]
```

返ってきた配列は inbox から削除済み。空配列なら未読なし。
`fields=["from","message"]` で metadata を省いてトークン削減すること。

依頼が曖昧なら、エラー扱いせず `post_message` で質問を返す。

---

## 4.5. MANDATORY status protocol（必須プロトコル）

> **⚠️ silent stop（status なしで turn 終了）= protocol 違反**

### 大原則: stall/idle は session だけが知る決定論的事実

broker は message router にすぎず、知れるのは post/drain の timestamp だけ。
「無 post N 分 = idle」は**非決定論的な当て推量**で、長い正計算で静かなだけ /
真に stuck / 人間待ち を外部から区別できない。**自分の状態を決定論的に知るのは
session 自身だけ**。だから status の自己申告は「便利」ではなく**必然**であり、
監視側（peer-idle-notifier 等）はそれに反応するだけで推論しない。

### 2つの直交する軸

状態は**2軸**に分かれる。混ぜないこと:

| 軸 | API / CLI | 設定者 | 性質 |
|---|---|---|---|
| **active**（機械的） | `set_active` / `reyn-broker-active` | hook | 決定論・stall 判定の authority |
| **status**（意味的） | `update_session_status` / `reyn-broker-status` | LLM | best-effort・enrichment |

両者は独立フィールド。`set_active(false)` は status に触れず、`update_session_status`
は active に触れない。だから Stop hook の機械的 idle が LLM の `waiting` 申告を
**clobber しない**。

### active は両エッジを撃つ義務（hook）

`active_changed` は**フリップ時のみ**発火する。両エッジを hook で撃つ。
**推奨は `PreToolUse` + `Stop` の2つ**:

| エッジ | hook | コマンド |
|---|---|---|
| 作業中 | **PreToolUse** | `reyn-broker-active <id> true` |
| 作業終了 | **Stop** | `reyn-broker-active <id> false` |

**なぜ作業開始に `UserPromptSubmit` でなく `PreToolUse` か**:
broker の task-notification / wakeup 経由の再開は `UserPromptSubmit` を**通らない**。
そのため UserPromptSubmit で true を撃つ設計だと、wakeup で再開した session が
idle のまま取り残される（= 一番 dormancy しやすい session を捕り逃す）。
`PreToolUse` なら「作業すれば必ず何かツールを使う」ので、**user-prompt 起点でも
wakeup 起点でも確実に true になる**。1つの hook で両経路をカバーできる。

**毎ツール呼ばれても安全**: `set_active` は値が変わらなければ no-op（`active_changed`
を発火しない）。PreToolUse が毎回 true を撃っても、active→true のフリップ1回だけが
イベント化され、購読側に spam は飛ばない（CLI 起動コストのみ）。

**⚠️ active=true を撃たないと sticky-idle トラップ**: 一度 false にしたきり再開時に
true へ戻さないと、watcher は「idle のまま」と「再開した」を区別できず永遠に idle に見える。

```jsonc
// settings.json
{
  "hooks": {
    "PreToolUse": [{ "matcher": "", "hooks": [{
      "type": "command",
      "command": "/path/to/.venv/bin/reyn-broker-active <session_id> true"
    }]}],
    "Stop": [{ "matcher": "", "hooks": [{
      "type": "command",
      "command": "/path/to/.venv/bin/reyn-broker-active <session_id> false"
    }]}]
  }
}
```

### status は任意の enrichment（LLM）

「何を待っているか」を伝えたい時だけ撃つ。無くても active で stall 検出は回る。

```python
update_session_status(
    session_id="<self>",
    status="waiting",
    detail="ci:#1268 / blocked-on-<X> / pausing-resume-when-<Y>",
)
```

これは **best-effort な付帯情報**であり stall 判定の authority ではない。watcher は
`active` のエッジで判定し、status は「なぜ idle か」の表示にのみ使う。

### ステップ 2: backlog-watcher に stop-status を post する

```
post_message(
    to="backlog-watcher",
    from_session="<自分の session_id>",
    message="""
(a) 完了したこと: <直前のタスク完了状態>
(b) 停止理由 + 次: done-need-review / blocked-on-<X> / pausing-resume-when-<Y> / handed-off-to-<Z>
(c) WIP 状態: branch push 済? PR#? blocker?
"""
)
```

### 通知先の役割分担

| 通知先 | 用途 |
|---|---|
| **backlog-watcher** | 停止 status（全ての stop-status はここへ）|
| **lead-coder** | 緊急 review 依頼 / design decision / PR merge 依頼 |

backlog-watcher が peer-status を集約し、actionable な stall / blocker のみ
lead-coder に escalate する（lead-coder の noise 削減）。routine な status は
backlog-watcher で留まる。

### なぜ必須か

broker は POST 時しか lead-coder / backlog-watcher に通知しない。無言停止 =
監視側が盲目化し、silent stall を外部から検知できなくなる。
mid-task で止まらざるを得ない時も「ここで一旦止まる、再開は〜」の一言で足りる。

---

## 5. セッション終了時

明示的にセッションを終わる前に呼ぶ:

```
unregister_session(session_id="<自分の session_id>")
```

呼ばずに落としても broker は次回の送信失敗時にキューへ退避するが、
`list_sessions` の見た目が綺麗になるので呼ぶ習慣をつけること。

---

## 5.5. broker restart 時の振る舞い

broker (v0.3.0+) は sessions メタデータ + pending キューを disk に永続化する。 結果:

- broker process restart があっても登録は維持される (= `list_sessions` から消えない)
- 自分宛 pending message も維持される
- ただし **`mcp_session` ref は ephemeral** なので push 通知 (`notifications/message`) は復活直後だけ届かない期間がある。 docs 上「push は best-effort」 を維持
- watcher (Monitor task) は broker 接続が一時切断 → 自動 retry → 復活するので透過。 fallback として手動 `receive_messages` も使える
- 「broker restart した」 と user / 他 session から通知されても、 **再 `register_session` は不要** (= 必要なら自分の判断で role 更新したい時だけ呼ぶ)

### ⚠️ watcher の health check 義務

broker restart announce を受けたら、**必ず自 session の watcher (Monitor task) が生きているかを確認すること**。

broker 接続切断中に Monitor task が何らかの理由で kill されていた場合、自動 retry は起きず inbox が蓄積し続ける silent stall になる。

**確認手順**:
```
TaskList()  # watcher task が running か確認
```
- running でない場合 → 即 watcher 再起動:
  ```
  Monitor(
    command="/path/to/broker/.venv/bin/python /path/to/broker/session_watcher.py --session=<id>",
    persistent=True,
    timeout_ms=3600000,
  )
  ```
- running でも念のため `receive_messages` を手動 drain して蓄積確認

broker からの restart 完了 announce には必ず「watcher health check をしてください」という文言が含まれます。

### ⚠️ ツールスキーマキャッシュ問題

Claude Code は MCP サーバーのツール一覧を **セッション起動時に一度だけ取得してキャッシュ** する。broker が新バージョンで再起動してツールのシグネチャが変わった場合、接続中のセッションは古いスキーマを持ち続ける。

**症状**: `receive_messages` は動くのに `post_message` など変更されたツールが失敗する。

**対処手順**:
1. まず ToolSearch でスキーマを再取得:
   ```
   ToolSearch(query="select:mcp__broker__post_message")
   ```
   取得したスキーマで再度ツールを呼び出す。
2. それでも失敗する場合 → Claude Code **セッション再起動**（MCP 接続がリセットされ最新スキーマを取得）

**予防**: CHANGELOG でツールシグネチャの変更がある release は「セッション側スキーマ更新が必要」と明記する。
再起動後は `health_check()` で version を確認し、schema 変更リリースなら ToolSearch でスキーマを更新すること。

---

## 5.9. トークン節約ガイドライン

broker MCP の呼び出し結果は LLM context に入るため、不要な呼び出しや大きなペイロードはトークンコストに直結する。以下を原則とすること。

### startup_summary を使う（register + list を 1 回にまとめる）

```
# ❌ 2 回呼ぶ
register_session(session_id=..., working_dir=..., role=...)
list_sessions()

# ✅ 1 回で済む
startup_summary(session_id=..., working_dir=..., role=...)
# → {status, pending_messages, sessions: [...compact...]} を一括取得
```

### list_sessions は compact=True を原則使用

```
# ❌ フルシェイプ（8セッション × 6フィールド ≈ 1600 chars）
list_sessions()

# ✅ コンパクト（session_id + role のみ、約 60% 削減）
list_sessions(compact=True)

# full shape が必要なのは activity timestamp や inbox_unread_count を見る時のみ
```

### list_sessions は必要な時だけ呼ぶ

- 起動時（startup_summary で同時取得）
- 新しい送信先を探す時
- **ターンごとに毎回呼ばない**（セッション一覧は頻繁に変わらない）

### receive_messages は fields で絞り込む

```
# ❌ 全フィールド（sent_at_iso, is_broadcast, recipient_count 等を含む）
receive_messages(session_id="self")

# ✅ 必要なフィールドのみ（大半のケースはこれで十分）
receive_messages(session_id="self", fields=["from", "message"])
```

### inbox_stats は「相手の確認」専用

`inbox_stats` は自分の inbox 確認に使わない。drain するつもりなら `receive_messages` を直接呼ぶ（2回呼ぶのは無駄）。`inbox_stats` の用途は「相手がまだ読んでいないか確認する」。

### peek_messages — 割り込み判断に使う

`receive_messages` を呼ぶと inbox が drain されて read-ack も発火する。
まだ今のタスクを続けるべきか中断すべきか判断したい場合は、
先に `peek_messages` で内容だけ確認してから drain するかを決める:

```
# ❌ drain してから判断（read-ack 発火・キュー消去が先行してしまう）
msgs = receive_messages(session_id="self")

# ✅ 内容を確認してから drain を判断
preview = peek_messages(session_id="self", fields=["from", "message"])
if <割り込み不要と判断>:
    # 後で drain する
else:
    msgs = receive_messages(session_id="self")
```

ただし triage 後に必ず `receive_messages` を呼ぶこと。`peek` は消費しないので読み飛ばし防止にならない。

### health_check — broker 再起動後に必ず確認

broker 再起動後は最初に `health_check()` を呼んで version を確認し、
CHANGELOG に schema 変更がある version への更新であれば `ToolSearch` でスキーマを更新する。

```
health = health_check()
# → {"version": "0.10.0", "uptime_seconds": 5, ...}
```

### session TTL — 短命タスクには ttl_hours を設定

明示的に `unregister_session` を呼ばずに終了する可能性があるセッション
（単発タスク、CI bot 等）は `ttl_hours` を設定しておく。
broker の background purge (5 分毎) が自動削除するので registry が汚染されない。

```
# 永続セッション (omit ttl_hours)
startup_summary(session_id="lead-coder", working_dir=..., role="lead")

# 短命タスク (auto-expire in 4 hours)
startup_summary(session_id="temp-reviewer", working_dir=..., role="PR review", ttl_hours=4.0)
```

---

## 6. 利用可能なツール早見表

| ツール | 引数 | 用途 |
|---|---|---|
| `startup_summary` | `session_id`, `working_dir`, `role?`, `compact?` (default true), `ttl_hours?` | **起動時推奨** — register + list を 1 回に統合。`pending_messages` + `sessions` を同時取得 |
| `register_session` | `session_id`, `working_dir`, `role?`, `ttl_hours?` | register のみ（list 不要な場合）。`ttl_hours` で自動期限切れ。戻り値の `pending_messages` を必ず処理 |
| `list_sessions` | `compact?` (default false) | 相手を探す。**原則 `compact=True` を使う**（session_id + role のみ） |
| `post_message` | `to`, `from_session`, `message`, `request_read_ack?`, `recipients?`, `ttl_seconds?` | 依頼/返信を送る。`recipients=[...]` で複数宛先、`ttl_seconds` で自動期限切れ |
| `broadcast_message` | `from_session`, `message`, `exclude_self?` (default true), `recipients?` | 全 registered session に一斉送信。`recipients=[...]` で対象を絞れる |
| `receive_messages` | `session_id`, `fields?` | 自分の inbox を drain。**`fields=["from","message"]` で不要な metadata を除いてトークン削減** |
| `peek_messages` | `session_id`, `limit?` (default 10), `fields?` | **非破壊コンテンツ確認** — inbox を drain せず内容を確認。割り込み要否の triage に |
| `inbox_stats` | `session_id` | 非破壊 peek: `{pending_count, senders}`。自分の確認より**相手の確認**に使う |
| `update_session_status` | `session_id`, `status`, `detail?` | 自分の状態を宣言（`"active"` / `"idle"` / `"waiting"` など）。変化時のみ `status_changed` イベントを発火。`reyn-broker-status` CLI で stop hook からも呼べる |
| `get_session_status` | `session_id` | 1 セッションの現在状態を broker に問い合わせ。`{registered, status, status_detail}` を返す |
| `subscribe_session_events` | `subscriber_id`, `event_types`, `session_filter?` | セッションイベント（`registered` / `unregistered` / `posted` / `status_changed`）を inbox 経由で受け取る。プラグインは `BrokerClient.subscribe_events()` を使用 |
| `unsubscribe_session_events` | `subscriber_id` | イベント購読を解除 |
| `tool_stats` | *(なし)* | ツール呼び出し回数の統計。token コスト分析に |
| `health_check` | *(なし)* | broker の version / uptime / session_count / total_pending を返す。再起動後の確認に |
| `unregister_session` | `session_id` | 終了前に 1 回 |

---

## 7. ふるまいの約束

- **session_id は basename(pwd) で固定**。途中で変えない
- **`from_session` には必ず自分の session_id** を入れる(嘘の名前で送らない)
- **相手のセッション名を勝手に仮定しない**。`list_sessions` で確認するか、
  ユーザーから明示的に指定された名前だけ使う
- **broker からの通知を受けたら最優先で対応**。完了したら必ず返信する
- **エラー時** (相手が居ない、ツール呼び出し失敗等) はユーザーに報告する

---

## 8. 典型フロー

```
[このセッション]
  result = register_session(session_id="<self>", working_dir="<cwd>")
  # result.pending_messages を順に処理(あれば即返信)

  list_sessions()                       # 相手の候補を確認
  post_message(to="<peer>",
               from_session="<self>",
               message="X をレビューしてほしい。終わったら post_message で返して")

  # 待ち時間や次の区切りで:
  msgs = receive_messages(session_id="<self>")
  # msgs を順に処理 → post_message で返信

  unregister_session(session_id="<self>")
```

---

## 9. 非同期受信 (= Monitor + session_watcher.py)

`receive_messages` を Claude Code のターン内でしか呼べないため、 ユーザの
prompt を待たずに新着 message を pickup したい場合は **Monitor タスク** で
inbox watcher を背後で動かす。

### Watcher の仕組み

`/path/to/broker/session_watcher.py` が、 broker MCP の
`receive_messages` を 30 秒毎にポーリングし、 message が来たら **1 件 = 1 行
JSON** を stdout に吐く。 Monitor タスクの stdout 1 行は Claude Code 上で
`<task-notification>` として LLM context に inject されるので、 LLM
セッションがアイドルでも 「届いた瞬間に反応」 出来る。

idle (= 0 件) の間は何も出力しないので、 LLM のトークン消費はゼロ。

### 起動手順

Claude Code セッション側で `register_session` を呼んだ直後に、 以下の
Monitor を起動:

```
Monitor(
  description="broker inbox watcher",
  command="/Users/yasudatetsuya/Workspace/reyn_dev/broker/.venv/bin/python /Users/yasudatetsuya/Workspace/reyn_dev/broker/session_watcher.py --session=<自分の session_id>",
  persistent=true,
  timeout_ms=3600000,  # 1 時間 (必要に応じて延長)
)
```

**⚠️ 重要: 必ず broker の `.venv/bin/python` を使うこと。**
素の `python` / `python3` で実行すると `ModuleNotFoundError: No module named 'mcp'` で即落ちる。
broker の venv パスは `/Users/yasudatetsuya/Workspace/reyn_dev/broker/.venv/bin/python`。

`<自分の session_id>` は `register_session` で使ったのと同じ値 (= basename(pwd))。

### 受信した notification の形

watcher の stdout 1 行はそのまま `<task-notification>` の event 本文として
LLM に届く。 1 message ごとの形:

```json
{"from": "<送信元 session_id>", "message": "<本文>"}
```

通知を受けたら通常の MCP `receive_messages` で取り直さずに **その JSON を
そのまま処理する**。 watcher が既に inbox から drain 済みなので、
`receive_messages` を後で呼んでも同じ message は再 push されない。

**batch 挙動**: 1 回の poll で N 件の message が到着すると watcher は
N 行 stdout に書き出す。 Monitor は ~200ms 以内の stdout を 1 notification
にまとめるため、 event body に **複数 JSON 行が改行区切りで連結** されて
届くことがある。 受信側は **event body を行単位で split し、 各行を 1
message として parse** すること (= JSON 1 行 = 1 message が不変条件)。

**長 message の truncation 対策 (0.5.0+)**: Monitor の event body サイズ上限
は実測で ~500 chars 程度 (0.5.0 で 1500 chars を default にしていたが、
2026-05-19 観測で 529 chars message が truncate → 0.5.1 で **default 400 chars** に
下方修正)。 threshold 超え message について、 watcher は full body を
per-session journal file (`/tmp/reyn-broker-inbox/<session_id>/msg-<ts>-<sender>.json`)
に書き出した上で、 emit する line を **summary 形式** に切替える:

```json
{
  "from": "<sender>",
  "message": "[long message from <sender>, N chars — full text at /tmp/...]",
  "_truncated": true,
  "_full_path": "/tmp/reyn-broker-inbox/<self>/msg-<ts>-<sender>.json",
  "_body_chars": N,
  "_preview": "<先頭 ~100 chars>"
}
```

受信側の処理:
1. event body 各行を JSON parse
2. **`_truncated` field が true の場合**:
   - まず `_preview` で内容の概要を確認 (routing 判断に十分なら `Read` 不要)
   - full body が必要な場合は `_full_path` を Read tool で開いて原 JSON を取得 → `"message"` field が本来の本文
3. `_truncated` が無い / false なら `message` field をそのまま使う

journal file は auto cleanup されない (= `/tmp` 配下なので再起動で消える)。

### 注意

- **1 session につき watcher 1 つ**。 同じ session_id の watcher を複数起動
  すると inbox を奪い合って message の取りこぼし / 二重処理が起きる
- **watcher が落ちたら手動で `receive_messages`**: Monitor タスクが何らかの
  理由で死んだ場合 (= 5 連続 broker poll 失敗で `_watcher_error` が 1 行
  出力されてから quiet モード)、 fallback として `receive_messages` を
  自分で呼んで取りこぼし回収
- **セッション終了時**: `unregister_session` + Monitor は session 終了で
  自動 kill。 明示的に止めたければ `TaskStop(task_id=<watcher task id>)`
