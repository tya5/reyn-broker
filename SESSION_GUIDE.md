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

## 4. メッセージを受け取る (ポーリング方式)

**重要:** broker は logging notification (`notifications/message`) も
ベストエフォートで投げるが、Claude Code の MCP クライアントは log 通知を
LLM のコンテキストに自動投入しない。つまり通知だけでは届かない。
**必ず `receive_messages` を能動的に呼んで inbox を回収すること。**

```
receive_messages(session_id="<自分の session_id>")
→ [{"from": "<送信元>", "message": "<本文>"}, ...]
```

返ってきた配列は inbox から削除済み(一度きりの引き渡し)。空配列なら
未読なし。

呼び出すタイミング:

1. `register_session` 直後の `pending_messages` 確認(初回 backlog)
2. ユーザーから「inbox 見て」「メッセージ来てない?」と頼まれたとき
3. 長めのタスクが一段落したとき(自分が手元の作業を区切ったタイミング)
4. 自分が `post_message` で誰かに依頼を投げた直後、相手の返信待ち時

受信したら:

1. 配列の各要素について `data.from` と `data.message` を読む
2. 依頼内容のタスクを実行する
3. 完了したら結果を `post_message(to=data.from, from_session=自分,
   message=結果)` で返す
4. 必要なら成果物パスや作業ログも本文に含める

依頼が曖昧なら、エラー扱いせず `post_message` で質問を返す。

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
