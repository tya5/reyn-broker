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

**自分の識別子を決める** — このセッションの作業ディレクトリ名(basename)を
そのまま `session_id` として使う。`working_dir` は作業ディレクトリの絶対パス。

```
session_id  = basename(pwd)
working_dir = pwd  (絶対パス)
```

その値で `register_session` を 1 回呼ぶ:

```
register_session(
    session_id="<basename>",
    working_dir="<absolute_path>",
    role="<短い役割説明>",   # optional, 例: "PR review", "e2e tests"
)
→ {
    "status": "registered '<basename>' at <path>",
    "pending_messages": [ {"from": ..., "message": ...}, ... ]
  }
```

**戻り値の `pending_messages` を必ず確認する**。オフライン中に届いていた
メッセージはすべてここに入っており、登録と同時に drain される
(broker 側からは消える)。漏らさず読み、必要なら順番に処理すること。

`role` は optional で、未指定なら `null` 扱い。 指定しておくと
`list_sessions` の結果に含まれるので、 他セッションが session_id の
命名規約を知らなくても役割を見て routing 判断できる。 freeform 1 行で OK
(例: "PR review", "e2e tests", "TUI fixes")。

---

## 2. 他セッションを探す

固定のセッション名を仮定しない。実行時に必ず `list_sessions` で取得する:

```
list_sessions()
→ [{"session_id": "...", "working_dir": "...", "role": "..." | null}, ...]
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

---

## 6. 利用可能なツール早見表

| ツール | 引数 | 用途 |
|---|---|---|
| `register_session` | `session_id`, `working_dir`, `role` (optional) | 起動時に 1 回。戻り値の `pending_messages` を必ず処理 |
| `list_sessions` | (なし) | 相手を探す |
| `post_message` | `to`, `from_session`, `message`, `request_read_ack` (optional) | 依頼/返信を送る (常にキュー)。 `request_read_ack=True` で相手 drain 時に sender 宛に `read-ack` を自動投函 |
| `broadcast_message` | `from_session`, `message`, `exclude_self` (default true) | 全 registered session に同一 message を送る (= announce / anyone-help 用途) |
| `receive_messages` | `session_id` | 自分の inbox を drain して取得 (ポーリング) |
| `inbox_stats` | `session_id` | 非破壊 peek: `{pending_count, senders}` 取得、 drain しない |
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
  command="python /path/to/broker/session_watcher.py --session=<自分の session_id>",
  persistent=true,
  timeout_ms=3600000,  # 1 時間 (必要に応じて延長)
)
```

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
  "_body_chars": N
}
```

受信側の処理:
1. event body 各行を JSON parse
2. **`_truncated` field が true の場合**: `_full_path` を Read tool で開いて
   原 JSON を取得 → 中の `"message"` field が本来の本文
3. `_truncated` が無い / false なら従来通り `message` field をそのまま使う

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
