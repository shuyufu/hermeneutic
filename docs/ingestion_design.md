# 行情 Ingestion 設計文件

狀態：見第 11 節「下一步」的最新狀態摘要，這裡不重複維護一份會漂移的計數。

本文件目的：把設計討論過程中反覆修正、目前只存在對話 scrollback 裡的決策跟理由固定下來，避免之後被 context 摘要掉、或被下一個 session 遺忘。**特別保留「曾經想錯、後來怎麼修正」的部分**，不只是最終乾淨版本——因為那些修正本身就是之後容易重蹈覆轍的地方。

## 1. 目標與範圍

從多個交易所（目前做 Binance USDⓈ-M Futures + Binance Spot + Bybit linear/spot）訂閱多個 symbol 的 L2 order book diff，維護正確的本地 order book 狀態，餵進已經存在的 `SymbolBook`（`service/aggregator_service.hpp`，`AggregatorService::book(symbol)`）。目標是非同步、多執行緒（Boost.Asio + Beast），且核心邏輯採 **sans-io** 設計：resync/序號校驗這類最容易出錯的邏輯完全不碰 socket，可以純用假資料序列做單元測試。

## 2. 分層總覽

```
┌──────────────────────────────────────────────────────────┐
│  I/O driver 層（薄，asio/Beast 都關在這層）                  │
│  VenueSession<Feed, Policy>（per-connection strand）        │
│    擁有一代又一代的 WebSocketConnection（每次重連換一個新的） │
│    一次性 http_get() 函式（不是常駐的 HttpConnection 物件）  │
│    reconnect/backoff（帶 jitter）                           │
└───────────────────────┬────────────────────────────────────┘
                         │ 餵事件進去 / 執行回傳的 Action
                         ▼
┌──────────────────────────────────────────────────────────┐
│  sans-io 核心（純狀態機，零 I/O，unit test 主戰場）           │
│  VenueFeed（parse/encode，per 交易所，例：BinanceFuturesFeed）│
│  SymbolSync<SequencePolicy>（per venue+symbol 的 resync）    │
│  SymbolRegistry（symbol → SymbolBook*，純查表）              │
└───────────────────────┬────────────────────────────────────┘
                         │ 呼叫（同 process，直接 C++ call，無 IPC）
                         ▼
                    SymbolBook（已存在）
```

## 3. `SymbolBook`（已實作：`service/aggregator_service.hpp`）

背景：原本 `AggregatorService` 隱含假設一個 symbol 一個 process/port（兩個 `grpc::Service` 實例不能註冊在同一個 `grpc::Server` 上，RPC method path 會撞名）。拆成：

- **`SymbolBook`**：純 C++ 型別（非 gRPC），擁有一個 `AggregateOrderBook`、自己的 `mutex_`、`seq_`、subscriber `SubscriberQueue` 集合。`apply_delta`/`apply_snapshot`/`invalidate_venue`/`send_heartbeat`/`subscribe`/`unsubscribe` 都在這裡。
- **`AggregatorService`**：唯一的 gRPC 型別，內部 `map<string, SymbolBook, less<>> books_`（建構時建好，不動態增減），`Subscribe(SubscribeRequest{symbol})` 路由到對應 book，未知 symbol 回 `NOT_FOUND`。`AggregatorService::book(symbol)` 是 ingestion 層要呼叫的入口。

`.proto` 的 `SubscribeRequest` 加了 `string symbol = 1`（安全,因為這個 proto 從沒服務過真正的 consumer）。詳細設計理由跟測試涵蓋範圍見 memory `hermeneutic-aggregator-service-design`。

## 4. `SymbolSync<SequencePolicy>`（已實作：`include/bobby/hermeneutic/symbol_sync.hpp` / `tests/symbol_sync_test.cpp`）

每個 (venue, symbol) 一個實例，管理「buffer → snapshot → drain → 穩態 → 偵測到 gap 就整個重來」這套流程。**完全不碰 socket**，靠 driver 餵事件進來、讀它吐出來的 action 列表去執行實際 I/O。

### 4.1 資料型別

```cpp
struct DepthUpdate {
    SymbolId symbol;
    std::uint64_t first_id;       // Binance: U
    std::uint64_t final_id;       // Binance: u
    std::uint64_t prev_final_id;  // Binance Futures: pu（Spot 沒有這欄位）
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
};

struct SnapshotMessage {
    SymbolId symbol;
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
    std::uint64_t last_update_id;
};

enum class Action { RequestSnapshot, ApplySnapshot, ApplyDelta, InvalidateVenue };
struct SyncOutput { Action action; /* 對應 payload，依 action 而定 */ };
```

**修正記錄（重要，別重蹈覆轍）**：`DepthUpdate` 一開始被設計成「一個 struct = 一筆 (side, price, size)」，多一個 `side` 欄位。這是錯的——交易所的一則 depth diff 訊息本身就是**一批** bid/ask 變化共用同一組 `U`/`u`（例：`{"U":157,"u":160,"b":[[...]],"a":[[...]]}`），不是一個 level 一則。改成跟 `SnapshotMessage` 同形狀（`bids`/`asks` 都是 vector），拿掉 `side`。這樣 gap 檢查（`U`/`u`/`pu`）本來就是**整則訊息層級**的事，不會被誤解成個別 level 各自有序號。

**衍生的未決事項**：`SymbolBook::apply_delta` 目前一次只吃一個 `(side, price, size)`，套用一個 `DepthUpdate`（一批 level）會變成好幾次 `apply_delta` 呼叫、各自 bump 一次 `seq_`——交易所一則訊息在我們自己的 wire protocol 上會變成好幾個 `L2Diff`。可能要幫 `SymbolBook` 加一個類似 `apply_batch`（比照 `apply_snapshot` 已經在做的「一批變化只 bump 一次 seq」）。**尚未決定，列在第 10 節。**

### 4.2 狀態機

狀態：`Buffering`（WS 已連上/已訂閱，snapshot 還沒套用，或正在重抓）、`Live`（穩態）。

```cpp
class SymbolSync {  // 實際上是 template<typename SequencePolicy>
  public:
    std::vector<SyncOutput> on_connected();        // driver 重連+重新訂閱成功後呼叫
    std::vector<SyncOutput> on_depth_update(DepthUpdate update);
    std::vector<SyncOutput> on_snapshot(SnapshotMessage snapshot);
    std::vector<SyncOutput> on_disconnected();
  private:
    enum class State { Buffering, Live } state_ = State::Buffering;
    std::vector<DepthUpdate> buffer_;
    std::uint64_t last_u_ = 0;
    bool snapshot_requested_ = false;
};
```

演算法（以 Binance USDⓈ-M Futures 官方文件的 9 個步驟為準，見第 5 節引用）：

```
on_connected():
    // 這裡才是該 emit RequestSnapshot 的時機點，不是斷線當下
    if state == Buffering && !snapshot_requested_:
        snapshot_requested_ = true
        emit RequestSnapshot

on_depth_update(update):
    if state == Buffering:
        buffer_.push_back(update)
    else:  // Live
        if !Policy::is_contiguous(update, last_u_):     // gap
            emit InvalidateVenue
            reset (清 buffer_/last_u_, snapshot_requested_ = false)
            state = Buffering
            buffer_.push_back(update)   // 這筆可能是下次銜接的候選，別丟掉
            // 注意：這裡不馬上 emit RequestSnapshot，等下一次 on_connected()
            //（連線通常還活著，但如果 driver 認為這代表連線該重建，
            //  也可以選擇讓 driver 自己觸發重連，讓 on_connected() 自然發生）
        else:
            emit ApplyDelta(update.bids, update.asks)
            last_u_ = update.final_id

on_snapshot(snapshot):
    erase buffer_ 中 Policy::should_drop_buffered(event, snapshot) 為真的
    找第一筆 Policy::bridges_snapshot(event, snapshot) 為真的
    if 找不到:
        emit RequestSnapshot   // 重試，留在 Buffering，buffer_ 不清空、繼續累積
    else:
        emit ApplySnapshot(snapshot.bids, snapshot.asks)
        從那筆（含）開始依序 emit ApplyDelta(event.bids, event.asks)，更新 last_u_ = event.final_id
        清空 buffer_（含被丟棄跟已套用的）
        state = Live

on_disconnected():
    emit InvalidateVenue
    reset (清 buffer_/last_u_, snapshot_requested_ = false)
    state = Buffering
    // 不在這裡 emit RequestSnapshot -- WS 都斷了，此刻抓的 snapshot
    // 在真的重連上、開始收到 live event 之前只會越來越舊
```

## 5. `SequencePolicy`（per-exchange 判斷式，template 參數）

`SymbolSync<SequencePolicy>` 的狀態機骨架**只寫一次**，交易所之間的差異收斂成三個判斷式：

```cpp
struct SequencePolicyConcept {
    // true：Feed 把自己的 snapshot 當成同一條已排序 WS 連線上的第一則訊息推送
    // （實務上等同 kSnapshotViaRest == false）；false：snapshot 走獨立 REST
    // 呼叫，跟已經在推的 diff 串流競速。見 SymbolSync::on_snapshot() 的
    // empty-buffer 分支——這個旗標為什麼非有不可，見第 10 節第 8 項。
    static constexpr bool kTrustsConnectionOrder;
    static bool should_drop_buffered(const DepthUpdate&, const SnapshotMessage&);
    static bool bridges_snapshot(const DepthUpdate&, const SnapshotMessage&);
    static bool is_contiguous(const DepthUpdate&, std::uint64_t last_applied_final_id);
};
```

### `BinanceFuturesSequencePolicy`（USDⓈ-M Futures，目前唯一要做的）

依據官方文件逐字引用的步驟（見文末 Sources）：

```cpp
struct BinanceFuturesSequencePolicy {
    static constexpr bool kTrustsConnectionOrder = false;  // snapshot 走 REST，跟 diff 串流競速
    static bool should_drop_buffered(const DepthUpdate& e, const SnapshotMessage& s) {
        return e.final_id < s.last_update_id;                                    // 步驟4：嚴格 <
    }
    static bool bridges_snapshot(const DepthUpdate& e, const SnapshotMessage& s) {
        return e.first_id <= s.last_update_id && e.final_id >= s.last_update_id;  // 步驟5：無 +1 偏移
    }
    static bool is_contiguous(const DepthUpdate& e, std::uint64_t last_applied_final_id) {
        return e.prev_final_id == last_applied_final_id;                         // 步驟6：pu 校驗
    }
};
```

**修正記錄**：一開始套用的是 Binance **Spot** 的公式（`U == last_u+1` 連續性、`u <= lastUpdateId` 丟棄、銜接條件 `U <= lastUpdateId+1 <= u`），跟 Futures 官方文件實際內容不同——Futures 用顯式的 `pu` back-pointer 做穩態校驗、銜接條件**沒有** +1 偏移、丟棄條件是**嚴格小於**。已經用 WebFetch 直接抓官方頁面逐字核對過，見文末 Sources。**Spot 的 policy 目前不需要**（使用者明確說「目前處理U本位的就好」），之後真的要接 Spot 才需要另外設計一個 `BinanceSpotSequencePolicy`。

### `BybitSequencePolicy`（Bybit linear + spot，已實作，兩個市場共用同一個 policy）

Bybit v5 public orderbook stream（`bybit-exchange.github.io/docs/v5/websocket/public/orderbook`）跟連上就自動推 snapshot 這一類交易所（見下方 `TrustConnectionOrderPolicy` 草案）同樣是「連上同一條 WS 就自動推第一筆 snapshot，之後全是 diff」的模式，但**沒有直接套用那個 trivial 版本**——Bybit 實際上每則訊息都帶一個真正的序號欄位 `u`，直接丟棄這個保護等於自願放棄偵測掉包的能力。驗證分兩步，不只憑文件字面，且 linear 跟 spot 分別各驗證一次（不假設兩個市場共通）：

- **文件查證**：官方文件本身**沒有寫明**顯式的缺口偵測規則，只提到「訊息中途收到 `u=1`」代表伺服器端重啟、要求前端整本重建。
- **即時探測**：
  - linear（2026-09-18，`wss://stream.bybit.com/v5/public/linear`，`orderbook.50.BTCUSDT`）：連續 30 則訊息裡，snapshot 之後每一則 delta 的 `u` 精確地 `+1` 遞增，完全沒有跳號；`seq`（cross sequence）則跳號幅度不固定（同一段樣本裡從 39 跳到 873），證實它是跨深度層級/topic 的新鮮度比較用欄位，不是本 topic 的連續性保證。
  - spot（2026-09-18，`wss://stream.bybit.com/v5/public/spot`，`orderbook.50.BTCUSDT`）：同樣連續 25 則訊息，`u` 一樣精確 `+1` 遞增，`seq` 一樣跳號幅度不定（2 到 20 都有）——跟 linear 完全同一套行為，於是 `BybitSequencePolicy` 直接共用，不需要一個 `BybitSpotSequencePolicy`。這是**驗證出來的結論，不是假設**：Bybit 官方文件沒有把這個行為當成跨產品的正式保證來寫，inverse/option 完全沒驗證過，不能直接套用同一個 policy。

因此選擇比文件字面更嚴格的做法，實際拿 `u` 做連續性校驗：

```cpp
struct BybitSequencePolicy {
    static constexpr bool kTrustsConnectionOrder = true;  // 見下方修正記錄
    static bool should_drop_buffered(const DepthUpdate& e, const SnapshotMessage& s) {
        return e.final_id <= s.last_update_id;        // 非嚴格 <=
    }
    static bool bridges_snapshot(const DepthUpdate& e, const SnapshotMessage& s) {
        return e.final_id == s.last_update_id + 1;     // 精確 +1，不是範圍
    }
    static bool is_contiguous(const DepthUpdate& e, std::uint64_t last_applied_final_id) {
        return e.final_id == last_applied_final_id + 1;
    }
};
```

- Bybit 一則訊息只有一個 `u`，沒有 Binance 那種 `first_id`/`final_id` 範圍 + `pu` 反向指標——`DepthUpdate::first_id`/`::final_id` 兩個欄位都塞同一個 `u`（`bybit_wire.hpp::parse_bybit_orderbook_message` 明確設定），`::prev_final_id` 沒有對應欄位，明確設成 `0`，不留給未初始化狀態。
- 文件提到的「`u=1` 代表重啟」不需要特殊處理：一旦本地已經是 live 狀態，`u=1` 不可能等於 `last_applied_final_id+1`，天然就會落到既有的 gap 處理路徑（`InvalidateVenue` + 重新 buffer），不需要另開一個分支。

**修正記錄（一個真的擋掉資料流的 bug，不是紙上談兵）**：`SymbolSync::on_snapshot()` 原本的設計（第 4 節）隱含假設呼叫時 `buffer_` 一定已經有內容——這個假設對 Binance 成立（snapshot 走獨立 REST，在它抵達前，diff 串流已經先在推、已經囤了幾筆），但對 Bybit **完全不成立**：Bybit 的 snapshot 本身就是連線後讀到的第一則訊息，`on_snapshot()` 執行當下 `buffer_` 是空的。`std::find_if` 在空 range 上永遠回傳 `end()`，於是永遠落到「沒有 bridge、retry」那條路徑，回傳 `RequestSnapshot`——但這個 action 對 `kSnapshotViaRest == false` 的 Feed 是 no-op（見第 6 節），沒有人會再發一次請求，`state_` 因此永遠卡在 `Buffering`，之後每一則 delta 都被塞進 `buffer_`、不斷增長、永遠不會真的套用進 book。**Bybit venue 會安靜地對聚合 book 貢獻零筆資料**，不會有任何錯誤訊息。

寫在框架設計階段的每一個 `SymbolSync`/`SymbolSyncBybitTest` 測試都是照 Binance 的 REST-race 順序（`on_depth_update()` 先於 `on_snapshot()`）驅動事件，沒有一個真的模擬過 Bybit 的實際訊息順序，所以完全沒抓到——是接上真實 `aggregator_main.cpp` 之後由一次程式碼審查（advisor）用手動追蹤程式碼流程抓出來的，不是任何自動化測試先發現的。修法是幫 `SequencePolicy` 加一個新的必要成員 `kTrustsConnectionOrder`（`BinanceFuturesSequencePolicy` 設 `false`，行為不變；`BybitSequencePolicy` 設 `true`），`on_snapshot()` 在「沒有 bridge」分支裡多一個 `if constexpr` 短路：`kTrustsConnectionOrder == true` 且**從一開始就沒有任何事件被 buffer 過**（不是「buffer 現在剛好是空的」——這兩者不同，見下一段）時，直接把 snapshot 當成起點進入 Live 狀態，不再要求先有一個 bridge 事件。

**修正這個修正時又踩到的兩個坑**（都被 advisor 在寫完第一版後的複查抓到，不是一次到位）：
1. 一開始的條件判斷式直接檢查 `buffer_.empty()`（在 `should_drop_buffered` 的 `erase_if` **之後**），沒辦法分辨「本來就沒 buffer 過任何東西」跟「buffer 過，但全部被判定成過期而丟光了」——後者是真正的 gap（有事件在 snapshot 抵達前就到了，違反 Bybit 的排序保證），不該被這個捷徑吃掉，卻會被誤判成前者而直接去套用一個丟棄了真實 buffered 事件的 snapshot。修法：在 `erase_if` **之前**先捕捉一次 `buffer_.empty()`，用這個「進入函式當下」的快照值做判斷，不是事後的值。
2. 上面這個修法本身還有第二個漏洞：live 狀態下的 gap 分支（`on_depth_update` 裡 `is_contiguous` 失敗那條路）也會 `reset()` 後把觸發 gap 的事件塞回 `buffer_`——這是 `buffer_` 的第二個寫入點，不只 Buffering 狀態那一個。如果誤判邏輯只在意「進入函式當下 `buffer_` 是否空」，這個路徑塞進去的事件一樣會在下一次 `on_snapshot()` 被正確納入判斷（因為那時候 `buffer_` 真的非空），所以捕捉時機本身（函式入口）已經同時涵蓋兩個寫入點，不需要額外用一個獨立的 bool flag 去追蹤「Buffering 分支有沒有真的塞過東西」——若真的改用一個只在 Buffering 分支裡設定的 flag，反而會漏掉這條 live-gap 路徑，重新踩進同一個坑。三個對應的判別測試：`SnapshotArrivingBeforeAnyBufferedEventGoesLiveDirectly`（驗證修好的那個真實 bug）、`NonEmptyBufferThatDoesNotBridgeStillRetriesDespiteTrustingConnectionOrder`（驗證捷徑不會亂吃真正的 gap）、`PostGapBufferedEventIsNotDiscardedByTheEmptyBufferShortcut`（驗證第二個寫入點沒被漏掉）——都在 `tests/symbol_sync_test.cpp`。

**尚未解決、刻意不在這次範圍內處理的相關限制**：live 狀態下發生 gap 時，`on_depth_update()` 只回傳 `InvalidateVenue`，並不會主動要求重新連線或重新拿 snapshot——`VenueSession::run()` 的讀取迴圈會繼續在同一條連線上等下一則訊息，但沒有任何機制會讓這個 symbol 離開 `Buffering` 狀態，除非連線真的斷線重連（重新觸發 `on_connected()`）。這是共用元件既有的限制，Binance 跟 Bybit 都受影響，不是這次改動造成的新問題——但實務嚴重程度不對稱：Binance 的 REST snapshot 理論上可以隨時再打一次（只是目前的程式碼路徑沒有這樣做），而 Bybit 文件明講「訊息中途收到 `u=1`」是伺服器端重啟的正常訊號，代表 Bybit 這邊 mid-stream 的 desync 是**預期會發生**的事件，不是罕見邊界情況，這個限制對 Bybit 的實際影響因此比對 Binance 更大。列在第 10 節第 8 項，留給之後處理。

### WS 自動推 snapshot 的交易所（例如某些非 Binance 交易所：連上就自動推第一筆 snapshot，之後全是 diff）

不需要另一套骨架，`SequencePolicy` 可以是近乎 trivial 的版本：

```cpp
struct TrustConnectionOrderPolicy {
    static constexpr bool kTrustsConnectionOrder = true;
    static bool should_drop_buffered(const DepthUpdate&, const SnapshotMessage&) { return false; }
    static bool bridges_snapshot(const DepthUpdate&, const SnapshotMessage&) { return true; }  // 傳輸層保證順序
    static bool is_contiguous(const DepthUpdate& e, std::uint64_t last) {
        return true;  // 或如果交易所有自己的序號欄位，比對那個（Bybit 就是這種——見 BybitSequencePolicy）
    }
};
```

這種交易所因為單一 WS 連線保證有序送達，「resync」實質上退化成跟「重連」同一件事——不需要囤 buffer 找銜接點。

## 6. `VenueFeed`（已實作：`service/binance_futures_feed.hpp` / `tests/binance_futures_feed_test.cpp`）

```cpp
using ParsedMessage = std::optional<std::variant<SnapshotMessage, DepthUpdate>>;

class BinanceFuturesFeed {
  public:
    static constexpr bool kSnapshotViaRest = true;  // false 的話 RequestSnapshot 對 VenueSession 是 no-op

    std::string subscribe_message(std::span<const SymbolId> symbols,
                                   std::string_view update_speed = "100ms") const;  // 純函式
    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const;
    HttpRequestSpec snapshot_request(const SymbolId& symbol) const;  // GET /fapi/v1/depth?symbol=...&limit=1000
    std::expected<SnapshotMessage, std::errc> parse_snapshot_response(SymbolId symbol,
                                                                        std::string_view body) const;
};
```

**修正記錄**：
- `ParseError` 一開始是設計骨架裡的佔位型別名稱，還沒決定。確認後**不新增型別**，沿用專案既有的 `std::expected<T, std::errc>` 慣例（`aggregate_order_book.hpp`/`notional.hpp`/`price_bands.hpp` 都是這樣），錯誤值用 `std::errc::bad_message`（對應 POSIX `EBADMSG`，語意上比 `invalid_argument` 更精確地描述「外部餵進來的訊息本身壞掉」）。
- `parse_message` 回傳型別實作時再多包一層 `std::optional`（`ParsedMessage = std::optional<std::variant<SnapshotMessage, DepthUpdate>>`），因為連上後 Binance 會回一個 SUBSCRIBE ack（`{"result":null,"id":1}`，完全沒有 `"e"` 欄位）——這是「有效訊息但跟 book 狀態無關」，用 `std::nullopt` 表示，跟「訊息本身壞掉」（`std::errc::bad_message`）明確分開，不會混在一起。
- `kSnapshotViaRest` 這個編譯期常數，是為了讓 `RequestSnapshot` 這個 action 的「履行方式」（真的打 HTTP GET vs. 純 no-op 等 WS 自然推送）可以在同一份 `VenueSession` 泛型邏輯裡分岔，不需要為 WS-push-snapshot 的交易所另開一份 `VenueSession`。
- `parse_snapshot_response` 多一個 `symbol` 參數：REST 回應本身不含 symbol 欄位（`{"lastUpdateId":...,"bids":[...],"asks":[...]}`），只能由呼叫端（知道自己打了哪個 symbol 的請求）補上。
- **JSON 庫選擇**：使用者選 `simdjson`。原本打算跟 `grpc` 一起走 vcpkg（`find_package(simdjson CONFIG REQUIRED)`），但 simdjson 的 vcpkg port 需要系統裝 `pkg-config`，這台機器沒有 Homebrew 也沒有 pkg-config。與其安裝一整個 Homebrew（較大、較不易復原的系統變更），改用 simdjson 官方支援的 **CMake `FetchContent`**（跟這個專案已經在用的 googletest 同一招），完全不需要 vcpkg/pkg-config。新增 `HERMENEUTIC_BUILD_INGESTION` 選項（預設 `ON`，不需要 vcpkg toolchain），`hermeneutic_binance_futures_feed_test` 掛在這個選項底下，即使沒設定 `HERMENEUTIC_BUILD_SERVICE`/`VCPKG_ROOT` 也能跑。
- 解析邏輯用 simdjson 的 on-demand API（拋例外的預設模式），在 `parse_message`/`parse_snapshot_response` 外層包 `try/catch (const simdjson::simdjson_error&)`，統一轉成 `std::errc::bad_message`——跟 `aggregate_order_book.hpp` 捕捉 `std::bad_alloc` 轉成錯誤碼是同一種既有模式。
- Binance 用字串傳 price/quantity（避免 wire format 本身出現浮點數歧義），用 `std::from_chars`（非 locale-dependent、不拋例外）轉成 `double` 再建構 `Price`/`Size`。

### `BybitLinearFeed` / `BybitSpotFeed`（已實作：`service/bybit_linear_feed.hpp`、`service/bybit_spot_feed.hpp`、共用解析邏輯在 `service/bybit_wire.hpp`）

```cpp
class BybitLinearFeed {
  public:
    static constexpr bool kSnapshotViaRest = false;  // snapshot 由 WS 自己推，RequestSnapshot 是 no-op
    std::string_view ws_target() const { return "/v5/public/linear"; }
    std::string subscribe_message(std::span<const SymbolId> symbols, int depth = 50) const;  // orderbook.{depth}.{symbol}
    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const;
    // 沒有 snapshot_request()/parse_snapshot_response()：kSnapshotViaRest==false 時
    // VenueSession 的 handle_request_snapshot() 走 if constexpr 的另一支，這兩個方法
    // 根本不會被實例化，Feed 也就不需要提供
};

class BybitSpotFeed {
  public:
    static constexpr bool kSnapshotViaRest = false;
    std::string_view ws_target() const { return "/v5/public/spot"; }  // 唯一跟 BybitLinearFeed 不同的地方
    // subscribe_message()/parse_message() 都直接轉發給 bybit_wire.hpp 的共用函式
};
```

**修正記錄**：
- **wire host/port/target**：`wss://stream.bybit.com/v5/public/{linear,spot}`，訂閱格式 `{"op":"subscribe","args":["orderbook.{depth}.{symbol}"]}`（`depth` 對 linear/spot 合法值都是 1/50/200/1000，未在程式碼裡驗證，呼叫端自己保證）。
- **`kSnapshotViaRest = false` 是這兩個 Feed 跟 Binance 系列最大的結構差異**：Bybit 把 snapshot 當成訂閱後同一條已排序 WS 連線上的第一則訊息推送，不像 Binance 得另外發一個 REST 請求去跟已經在推的 diff 串流競速。`VenueSession::handle_request_snapshot()`（`service/venue_session.hpp`）本來就用 `if constexpr (!Feed::kSnapshotViaRest)` 讓這支路徑對這種交易所直接是 no-op——這個分支之前只有 `docs/ingestion_design.md` 的 `TrustConnectionOrderPolicy` 草案提過構想，這是第一次真的用到它的 Feed（但光是這個分支 no-op 本身，還不足以讓這種 Feed 真正能動——見上一節 `BybitSequencePolicy` 的修正記錄，那才是真正卡住資料流的地方）。
- **`parse_message` 的「有效但不相關」訊息判斷**：Bybit 的 subscribe ack（`{"success":true,"ret_msg":"","conn_id":"...","req_id":"","op":"subscribe"}`）完全沒有 `"topic"` 欄位，同一個判斷式也順便涵蓋了任何缺少 `"topic"` 的控制訊息（例如未來可能出現的 pong）；有 `"topic"` 但 `"type"` 不是 `"snapshot"`/`"delta"` 的訊息一樣回傳 `std::nullopt`（forward-compatible，不當成壞資料）。
  - **踩到的坑**：simdjson on-demand API 是 lazy 的——`root["topic"].get_string();` 這樣把回傳值直接丟棄的寫法，實際上**不會**觸發欄位查找、也就永遠不會拋出 `NO_SUCH_FIELD`，等於這個「有沒有 topic 欄位」的判斷完全是死代碼，會一路往下走到需要 `"type"`/`"data"` 欄位存在的分支才在那裡才真正拋錯（於是原本該回 `std::nullopt` 的 subscribe ack 變成回 `std::errc::bad_message`）。修法是把回傳值實際指定給一個變數（`std::string_view topic = root["topic"].get_string();`）強制求值——這是被一個測試（`SubscribeAckHasNoTopicFieldAndIsIgnored`）抓到的，不是紙上推導出來的；Binance 的 `BinanceFuturesFeed::parse_message` 剛好一開始就是用賦值寫法（`event_type = root["e"].get_string();`），沒有踩過這個坑，純屬巧合，不是刻意的防禦。
  - **spot 的訊息欄位順序跟 linear 不同**：spot 是 `topic`/`ts`/`type`/`data`/`cts`（`ts` 在 `type` 前面），linear 是 `topic`/`type`/`ts`/`data`/`cts`。因為用的是 simdjson 的 `operator[]`（支援跳著找、不要求依照 JSON 出現順序存取），這個差異不需要任何程式碼分支，但兩個市場都各自拿真實 payload 驗證過，不是只驗證 linear 就假設 spot 一樣能過。
- **`DepthUpdate` 欄位映射**：Bybit 一則 delta 只有一個 `u`（沒有 Binance 的 `first_id`/`final_id` 範圍 + `pu`），`parse_bybit_orderbook_message` 把同一個值同時填進 `first_id`/`final_id`，`prev_final_id` 明確設成 `0`（理由同 `BybitSequencePolicy` 那節：不留給未初始化狀態）。
- **`json_wire.hpp`（`service/json_wire.hpp`）**：`detail::parse_decimal_string`/`parse_level`/`parse_levels`（JSON 字串轉 `Price`/`Size`，Binance 跟 Bybit 剛好用同一套「price/quantity 用字串傳」慣例）本來要抽出來時，這個分支已經有自己的 `bybit_linear_feed.hpp` 私有定義，還不知道 Binance Spot 那次已經先在 main 上建了 `service/binance_wire.hpp` 放同一組 helper（連同 `HttpRequestSpec`）。兩邊各自獨立踩進同一個 ODR 問題、各自解了一次——rebase 到 main 上的 `binance_wire.hpp` 時才發現這個重複，兩份 `namespace ...::ingestion::detail { parse_decimal_string/... }` 進到 `aggregator_main.cpp` 同一個翻譯單元一樣會 ODR 衝突，只是這次是「我方案」跟「main 方案」互撞，不是「Binance feed」跟「Bybit feed」互撞。**合併方式**：把這組 helper 留在新的、跟交易所無關的 `service/json_wire.hpp`（`namespace ...::ingestion::detail`），`binance_wire.hpp` 瘦身成只剩 `HttpRequestSpec`（Binance 真正專屬的部分——Bybit 沒有 REST snapshot，用不到）並改成 include `json_wire.hpp`；`bybit_wire.hpp` 也直接 include `json_wire.hpp`。三個 Feed header（`binance_futures_feed.hpp`/`binance_spot_feed.hpp` 透過 `binance_wire.hpp` 間接拿到，`bybit_linear_feed.hpp`/`bybit_spot_feed.hpp` 直接 include）現在共用同一份解析邏輯，不再各自維護一份。
- **`bybit_wire.hpp`（`service/bybit_wire.hpp`）**：加 `BybitSpotFeed` 時發現 linear/spot 的 `parse_message`/`subscribe_message` 邏輯是**逐位元組相同**（實測驗證過，不是猜的——見上一節），唯一真正不同的是 `ws_target()`。與其像 Binance Futures/Spot 那樣整份複製一份幾乎一樣的 class（那邊有 `pu` 欄位、REST 端點、limit 上限等真的不同的地方，值得分開），這裡直接把 `parse_bybit_orderbook_message`/`bybit_orderbook_subscribe_message` 兩個函式抽到 `bybit_wire.hpp`，`BybitLinearFeed`/`BybitSpotFeed` 都只剩端點常數 + 轉發呼叫，不重複維護同一段解析邏輯兩次。
- **即時驗證**：linear 跟 spot 各自獨立驗證，過程中發現並修正了 `SymbolSync` 的真實 bug（見上一節），所以驗證分兩輪：
  - 第一輪（`SymbolSync` bug 修好之前）：接上 `aggregator_main.cpp` 後同時跑 Binance Futures + Bybit linear 兩個 venue，訂閱 `SubscribeBbo` 收到持續變動的 book_seq/bid/ask，**但這輪其實只證明了 Binance 那條路徑在動**——兩個 venue 同時掛著時，`SubscribeBbo` 沒辦法分辨某一筆 BBO 是哪個 venue 貢獻的，而當時 Bybit 那條路徑因為 bug 實際上貢獻的是零筆資料，這輪驗證完全沒發現。這是 advisor 指出的錯誤結論，原始記錄已經改正，教訓記在這裡：**多 venue 同時掛著跑的驗證，證明不了任何單一 venue 真的有在貢獻**，要證明某個 venue 有效，必須讓它是當時唯一掛著的 venue。
  - 第二輪（`SymbolSync` bug 修好之後，正式驗證）：`BybitLinearFeed` 跟 `BybitSpotFeed` 分別單獨掛（不接 Binance，也不同時掛兩個 Bybit 市場）跑過一次臨時的單一 venue 版本 `hermeneutic_aggregator_service`（`service/live_bybit_spot_only_server_main.cpp` 等，驗證完已刪除，不留在 repo），對 BTCUSDT 訂閱 `SubscribeBbo`，兩輪都收到 8 筆真實、持續變動的 book_seq/bid/ask（例：linear `book_seq=175 bid=77711.9@1.839 ask=77712@3.359`；spot `book_seq=217 bid=77779@1.00672 ask=77779.1@0.130993`），過程中 server log 都沒有任何 error/disconnect。這才是真正證明「這個 venue 自己的路徑真的能把資料送進聚合 book、透過真正的 gRPC 路徑送到訂閱者」的驗證。
  - 三個 venue 同時掛著（`aggregator_main.cpp` 實際部署的樣子）沒有另外再做一次端對端驗證——三條路徑各自都已經單獨驗證過，`SymbolBook::apply_batch`/`invalidate_venue` 的 per-`VenueId` 隔離也已經在第 10 節第 2 項驗證過，沒有理由三個一起跑會表現不同，但這是一個尚未實測的假設，不是已驗證的事實，記在這裡。

## 7. `VenueSession<Feed, Policy>`（I/O driver，orchestrator，只寫一次）

**命名修正**：一開始叫 `VenueConnection`，容易誤會成「它本身就是一條 transport 連線」。改名 `VenueSession`，跟底下真正的 transport 層分開：

- **`WebSocketConnection`（已實作：`service/websocket_connection.hpp` / `tests/websocket_connection_test.cpp`）**：transport 層，venue-agnostic，薄。一條 WS 連線的生命週期本身（connect/send/read/close），包 `beast::websocket::stream<NextLayer>`。完全不知道 symbol/resync/交易所是什麼。**每次重連 = 新的一個 instance**，`VenueSession` 自己不會斷。
  - **template 化在 `NextLayer` 上**，不是寫死 SSL：`PlainWebSocketConnection`（`beast::tcp_stream`）給測試用（本地 server，不用處理測試憑證），`TlsWebSocketConnection`（`net::ssl::stream<beast::tcp_stream>`）給正式環境接 `wss://` 用。兩者共用同一份 connect/send/read/close 邏輯，只有 `connect()` 內用 `if constexpr` 判斷 `NextLayer` 是不是 SSL stream 來決定要不要多做 SNI 設定 + TLS handshake。測試涵蓋 connect/send/read/close 的機制本身（用本地 plain TCP echo server），不涵蓋 TLS handshake 這條分支本身（那段是 Asio/OpenSSL 自己的、有廣泛測試覆蓋的邏輯，不是本專案自己的程式碼）。
  - `asio::strand` 的部分尚未加——目前 `WebSocketConnection` 本身不管理 strand，這是 `VenueSession` 建構它的時候要決定的事（見下）。
- **HTTP snapshot（已實作：`service/http_client.hpp` / `tests/http_client_test.cpp`）**：不需要獨立的「HttpConnection」物件，寫成一次性函式 `http_get<NextLayer>(host, port, target, stream_args...) -> awaitable<expected<string, errc>>` 就夠，因為 REST snapshot 只是偶發的一次性 GET，不是常駐連線。跟 `WebSocketConnection` 同樣的 template 手法（`NextLayer` 決定要不要走 SSL），也跟它共用同一個 `detail::is_ssl_stream_v` trait（抽到 `service/net_traits.hpp`，避免兩邊各自重複定義）。測試涵蓋成功回應、非 200 狀態碼、連線被拒絕三種情況，一樣用本地 plain TCP HTTP server，不用測試憑證。
- **`VenueSession<Feed, Policy, NextLayer>`（已實作：`service/venue_session.hpp` / `tests/venue_session_test.cpp`）**：真正的 orchestrator，生命週期橫跨很多次 `WebSocketConnection`。擁有這條 session 負責的所有 symbol 的 `SymbolSync<Policy>`（`unordered_map<SymbolId, SymbolSync<Policy>>`），驅動迴圈：

```
loop:
    ws = 建立新的 WebSocketConnection（backoff + jitter 等待後）
    ws.send(feed.subscribe_message(symbols))
    for each symbol_sync: symbol_sync.on_connected()
    loop 讀 ws:
        text = co_await ws.read()
        parsed = feed.parse_message(text)
        對應 symbol 的 symbol_sync.on_snapshot(...) 或 on_depth_update(...)
        對吐出來的每個 action:
            RequestSnapshot -> co_spawn() 一個獨立背景 coroutine 去 http_get(feed.snapshot_request(symbol))，
                                結果丟進 on_snapshot()；kSnapshotViaRest==false 則整個 no-op
                                （見下方「co_spawn 而非 co_await」的說明——這不是實作細節，是必要條件）
            ApplySnapshot/ApplyDelta/InvalidateVenue -> 呼叫 SymbolRegistry 查到的 SymbolBook
    // ws 掛了/丟例外，跳出內層迴圈
    for each symbol_sync: symbol_sync.on_disconnected()   // 這條連線負責的每個 symbol 各自 invalidate
    // 回到 loop 開頭
```

**`VenueSession`本體只寫一次、venue-agnostic**：per-exchange 差異全部是「模板參數的具體型別」，不是「orchestration 的實作」——`VenueSession<BinanceFuturesFeed, BinanceFuturesSequencePolicy, ...>` 跟未來的 `VenueSession<OkxFeed, OkxSequencePolicy, ...>` 共用同一份類別程式碼。這跟本專案既有風格一致（`BasicFixedPoint<Decimals>`、`aggregator_service.hpp` 裡的 `capture_snapshot_before<Compare>`，都是用 template 參數化差異點，不是繼承/virtual）。第三個 template 參數 `NextLayer` 跟 `WebSocketConnection`/`http_get` 同一招：測試用 `beast::tcp_stream`（本地 server），正式環境用 SSL stream。

真正需要 runtime polymorphism（type erasure）的地方只有最上層——如果之後要有一個 `IngestionRunner` 同時管理多種不同具體型別的 `VenueSession`（Binance 的、OKX 的……），那一層可以留一個很薄的非模板介面（例如 `IVenueConnection { start(); stop(); }`），把 type erasure 限制在生命週期管理這個邊界，`SymbolSync`/`VenueSession` 本體的邏輯完全不用付虛擬呼叫成本。**這層尚未設計，見第 10 節。**

### 實作時踩到的坑（都是真的踩過，不是紙上談兵）

1. **`RequestSnapshot` 必須用 `co_spawn` 丟到背景、絕對不能 `co_await` 內聯處理。** 這不是效能優化，是正確性的必要條件：如果內聯 `co_await http_get(...)`，`execute_action` 會一路卡住，`run()` 的讀取迴圈整個被 HTTP fetch 卡住，永遠沒機會在等 snapshot 回來的同時繼續讀 WS、把 live event 塞進 buffer——`on_snapshot()` 每次都會發現 buffer 是空的、永遠銜接不上、無限重試。這正是顧問一開始強調「要在發 snapshot 請求之前就先開始緩衝」的具體體現：不是文件寫寫而已，是 coroutine 排程層面真的會卡死。
2. **傳給 `co_spawn` 的 coroutine，所有參數必須是值傳遞，不能是參考。** `handle_request_snapshot`（處理 `RequestSnapshot` 的那個 coroutine）一開始寫成 `const SymbolId& symbol`，結果是真實的 use-after-free：`co_spawn` 出去之後，呼叫端（`execute_action`）幾乎立刻執行完畢、它所在的 coroutine frame 被銷毀，連帶讓參考指向的 `symbol` 區域變數一起死掉；但 `handle_request_snapshot` 還在等 HTTP fetch（真的要花時間），等它恢復執行、要用 `symbol` 時，參考的東西早就沒了。實際症狀是 SIGTRAP crash（沒有任何例外訊息，直接跳過），花了不少時間才用 lldb + 大量 debug print 定位到。**教訓：任何要交給 `co_spawn`（而非直接 `co_await`）的 coroutine，其所有參數都必須用值傳遞**——因為它的生命週期不再受呼叫端的 frame 保護。`execute_actions`/`execute_action` 目前仍用 `const SymbolId&`，這是安全的，因為它們永遠是被直接 `co_await`（不是 spawn）呼叫，呼叫端的 frame 保證活到它們執行完——但這條規則是脆弱的，之後如果誰把其中一個也改成 spawn，要記得同步把參數改成值傳遞。
3. **std::visit 的 visitor 如果宣告回傳 `net::awaitable<void>`，函式本體卻完全沒有 `co_await`/`co_return`，會在函式結尾「掉出」而沒有真正產生回傳值。** 這不是空手而回——Clang 會在非 void、非 coroutine 函式掉出結尾的地方插入 trap 指令，一執行到就是 SIGTRAP，是第二個造成同一個 crash 症狀的真實 bug（先修好 bug 2 之後才浮現，之前被 bug 2 的 crash 蓋住了）。修法：如果 visitor 內部完全是同步邏輯（`SymbolBook` 的呼叫都不會 suspend），就老實宣告成回傳 `void` 的一般函式，不要為了「看起來要放進 `co_await std::visit(...)` 這種寫法」硬掛一個 `-> net::awaitable<void>`。
4. **測試時，假的 WS server 送完一則訊息就讓 coroutine 結束、底層 socket 跟著被解構關閉連線，會被 `VenueSession` 正確判定為斷線，觸發 `on_disconnected()` 把剛緩衝好的事件整個清空。** 這不是 bug，是系統正確的行為，但一開始把它誤判成又一個記憶體損壞 bug、花了好一陣子用 constructor/destructor 追蹤 + 逐點印位址才排除。**教訓：測試裡模擬「連線還活著、資料還沒送完」的假 server，送完 scripted 訊息之後，要故意再掛一個不會結束的 read（或其他方式）撐住連線的生命週期，不能讓 coroutine 提早 return。**

## 8. `SymbolRegistry`（已實作：`service/venue_session.hpp`）

`std::unordered_map<SymbolId, SymbolBook*>`，啟動時用 `add(symbol, service.book(symbol))` 建好，不動態增減。沒有特別的設計難度，純查表。

## 9. 其他修正記錄摘要

- **測試哲學延續**：`SymbolSync`/`VenueFeed` 是 sans-io、可以完全用假資料序列單元測試，不需要真的 socket——這是選擇整個分層方式（sans-io 核心 vs. 薄 I/O driver）的主要動機，也是使用者明確表達的偏好（「比較好 unittest」）。
- 資料來源全部經過 WebFetch 直接查證官方文件，不是純憑訓練記憶：
  - REST snapshot：`GET /fapi/v1/depth`（回傳 `lastUpdateId`/`E`/`T`/`bids`/`asks`）
  - WS diff stream：`{symbol}@depth@{updateSpeed}`，payload 含 `e`/`E`/`T`/`s`/`U`/`u`/`pu`/`b`/`a`/`ps`/`st`
  - 官方維護本地 order book 的 9 步驟程序（見 Sources）

## 10. 尚未定案 / 待做事項

1. ~~`SymbolBook::apply_batch`~~ **已完成**：一次呼叫套用整批 bid/ask 變化，只 bump 一次 `seq_`、只 broadcast 一次，跟 `apply_snapshot` 一樣的「先驗證全部 level、再套用」模式（負數 size 任一 level 有錯，整批都不套用）。診斷結果的 bids/asks 順序沿用 `capture_snapshot_before` 的做法（用跟 aggregate map 相同的 comparator 建中繼 map），維持「diff 順序跟 snapshot 順序一致」這條既有的協定保證，不是照輸入順序原樣印出。`VenueSession::execute_action` 處理 `ApplyDelta` 這個 action 時已經改成呼叫這個新方法（`service/venue_session.hpp`），不再對 bids/asks 各自逐筆呼叫 `apply_delta`。新增 3 個測試（一次 seq bump、跨 venue 正確加總、負數 size 整批拒絕且不 broadcast）。
2. ~~`IngestionRunner`/`IVenueSession` 的 type-erasure 邊界~~ **已完成**：`service/ingestion_runner.hpp`（`IVenueSession`、`VenueSessionAdapter<Feed,Policy,NextLayer>`、`IngestionRunner`）+ `VenueSession` 本身加上 strand/cancellation/`stop()`（`service/venue_session.hpp`）。測試：`tests/venue_session_test.cpp` 的 `StopAbortsBackoffWaitAndDoesNotReconnect`/`StopDrainsInFlightSnapshotFetch`，`tests/ingestion_runner_test.cpp` 的 `StopAllLetsIoContextFinishWithoutIoStop`。`aggregator_main.cpp` 已改用 `IngestionRunner`。細節與過程中修正記錄如下。

   **命名**：介面叫 `IVenueSession`，不是 `IVenueConnection`——第 7 節已經把 `VenueConnection` 改名成 `VenueSession`，理由是「connection 容易誤會成 transport 層」，同一個理由適用在這裡。

   **type erasure 的邊界為什麼很薄**：`VenueSession<Feed,Policy,NextLayer>::start()`/`stop()` 不管 `Feed`/`Policy`/`NextLayer` 是什麼組合，簽名永遠一樣（都是 `void`，不帶 template 參數）。所以完全不需要抹掉 `Feed`/`Policy` 本身（`SymbolSync`/`VenueFeed` 那層邏輯繼續是純 template，不付虛擬呼叫成本）——只需要抹掉「持有一個 `VenueSession<...>` 並呼叫它 `start()`/`stop()`」這件事：

   ```cpp
   class IVenueSession {
     public:
       virtual ~IVenueSession() = default;
       virtual void start() = 0;  // VenueSession::start() 的轉發；executor 已經在建構時定案（見下），這裡不用再傳
       virtual void stop() = 0;   // VenueSession::stop() 的轉發
   };

   template <typename Feed, typename Policy, typename NextLayer>
   class VenueSessionAdapter : public IVenueSession {
     public:
       template <typename... Args>
       explicit VenueSessionAdapter(Args&&... args) : session_(std::forward<Args>(args)...) {}
       void start() override { session_.start(/* 記錄 venue label 的 lambda，見 ingestion_runner.hpp */); }
       void stop() override { session_.stop(); }
     private:
       VenueSession<Feed, Policy, NextLayer> session_;
   };
   ```

   **跟提案時的差異：`sig_`/`stopping_`/`pending_snapshots_` 都留在 `VenueSession` 上，不是 adapter**。提案階段本來想放在 adapter，實作時發現這些狀態（尤其是 cancellation signal）本來就得跟 `run()`/`handle_request_snapshot()` 的 spawn 點放在一起才管用，硬放在 adapter 只會多一層轉發。`IVenueSession::start()` 因此也不帶 `net::any_io_executor` 參數——`VenueSession` 建構時就需要一個 executor 來建 `strand_`（見下），所以乾脆讓建構子收下它，`start()`/`stop()` 都不用再傳。

   **考慮過但否決：用 `std::variant<VenueSessionAdapter<Binance...>, VenueSessionAdapter<Okx...>, ...>` 或手刻 function-pointer vtable 取代 `virtual`**。否決理由：`start()`/`stop()` 一個 process 生命週期裡每個 venue 只各呼叫一次（五個 venue 就是十次 virtual call，一次性成本），真正的高頻路徑（`parse_message` → `SymbolSync` → `apply_batch`）完全不經過 `IVenueSession`，還是在 `VenueSession<Feed,Policy,NextLayer>` 內部單型化，虛擬呼叫成本從沒發生在這條路徑上——第 7 節講的「`SymbolSync`/`VenueSession` 本體不付虛擬呼叫成本」原則,守住的正是這裡,不是「全專案禁用 virtual」。`std::variant` 版本還會直接違背這一項自己定的 factory seam（把具體 template 參數留在該交易所自己的 `.cpp`、不外洩到 `aggregator_main.cpp`）——variant 宣告本身就得列出每個 venue 的具體 `Feed`/`Policy`/`NextLayer`，等於逼 `aggregator_main.cpp` 看到所有 venue 的 Beast/simdjson template 實例化。手刻 vtable 則是編譯器已經幫 virtual 做的事，自己重做一遍、還得手動管生命週期（自訂 deleter），拿不到任何好處。維持 `virtual`。

   **儲存方式：`vector<unique_ptr<IVenueSession>>`，adapter 內就地建構 `session_`，不是 `vector<Adapter>`**。`run()` 的 coroutine frame 會捕捉 `this`，一旦 spawn 出去，`session_` 就再也不能被搬動——`vector<Adapter>` 重新配置會讓已經在跑的 coroutine 手上的 `this` 直接懸空。用 `unique_ptr` 讓 adapter 物件位址穩定即可，完全不需要在意 `VenueSession<Feed,Policy,NextLayer>` 本身是否 movable（已查證過它是安全 implicitly movable，但這裡刻意選一個不依賴這件事的設計）。

   **`stop()` 是這一項真正要解決的問題，不是裝飾**。`run()` 原本是 `while(true)` 迴圈、完全沒有取消機制；`aggregator_main.cpp` 原本靠 `io.stop()` 把整個 `io_context` 連同所有 pending handler 一起粗暴丟掉，不是優雅關閉。做到「呼叫 `stop()` 之後乾淨結束、不留 in-flight 的 HTTP fetch/WS 讀取」，靠的是下面四件事，缺一個都不算數：

   1. **`net::cancellation_signal run_sig_` + `stopping_` flag，兩個都要，因為取消不會回溯**。單純 emit 一個 signal 還不夠：`stop()` emit 之後，正在等的 `connection.read()` 會丟 `operation_aborted`，被 `run()` 既有的 `catch (const std::exception&)` 當成普通斷線吞掉 → `disconnected = true` → invalidate 全部 symbol → `co_await backoff(1)`——這個 timer 如果沒被同一次 emit 波及，會乖乖等滿一輪 backoff 再重連。修法：`stopping_` 在 catch 區塊之後、`backoff` 之前檢查一次，是的話直接跳出迴圈；`backoff()` 本身也包一層 `try/catch`，因為 stop 剛好落在 backoff 等待期間時，是這個 timer（不是 read）被取消，若不接住會直接逃出 `run()`、跳過後面的 snapshot 排空。兩個檢查點（disconnected 處理後、backoff 之後）各一次，涵蓋所有回到迴圈頂端的路徑。
      另外，`stop()` 不能從呼叫端執行緒直接 `run_sig_.emit(...)`——`run()` 跑在 io thread 上，`cancellation_signal::emit` 對它正在取消的操作不是執行緒安全的。要 `net::post(strand_, [this]{ stopping_ = true; run_sig_.emit(net::cancellation_type::terminal); ... })`，這樣也不需要把 `stopping_` 弄成 `std::atomic`。
      **已用 spike 驗證（`clang++ -std=c++20`，直接連 vcpkg 裝的 Boost 1.92 標頭，不經過專案的 CMake）**：`beast::websocket::stream::async_read`／`steady_timer::async_wait` 確實支援 per-op cancellation，`net::post` 到執行 io_context 那個執行緒後再 `emit(terminal)`，兩者都如預期拋出 `operation_aborted`。

   2. **孤兒 `handle_request_snapshot` 的生命週期**：`net::co_spawn` 出去的這個 coroutine，恢復執行時會摸 `symbol_syncs_`、呼叫 `execute_actions`——一旦 runner 可以銷毀 session，關閉當下如果剛好有一個 snapshot fetch 還在飛，這就是第 7 節坑 2 記錄過的同一種 use-after-free，只是換一個時機重演。修法：每次 spawn 把一個 `net::cancellation_signal` 插進 `snapshot_sigs_`（`std::list<net::cancellation_signal>`），`run()` 收尾前用 `drain_pending_snapshots()` 等 `snapshot_sigs_.empty()` 才真正 `co_return`——沒有另外配一個計數器，`std::list` 的 empty() 本身就是唯一真相；一開始確實加了一個 `pending_snapshots_` int 跟著 spawn/completion handler 各自 `+1`/`-1`，程式碼審查點出這是跟 `snapshot_sigs_` 在同兩個地方同步變化的重複狀態，拿掉了。
      **實作時發現、提案階段沒設計到的坑**：每個 in-flight 的 snapshot fetch 要有**自己的** `net::cancellation_signal`，不能共用 `run_sig_`——一個 `cancellation_slot` 只會轉發給最近一次綁定的操作，如果多個 fetch（不同 symbol 各自 gap 一次）都綁同一個 signal 的 slot，後綁的會偷走先綁的登記，等於讓 `run()` 自己的 cancellation 註冊被悄悄頂替掉。改成 `std::list<net::cancellation_signal> snapshot_sigs_`（`std::list` 保證插入/刪除其他元素時既有的 iterator/reference 不失效，用來讓每個 fetch 的 completion handler 捕捉自己的 iterator、完成時自行 `erase` 自己），每次 spawn 各配一個。
      **`stop()` 對 `snapshot_sigs_` 逐一 `emit()` 時，遍歷期間不能讓任何節點被刪掉**：`emit()` 可能同步地直接把被取消的操作 resume 到完成（WS read/timer 已實測是這樣，`http_get` 這條 resolve/connect/read 的鏈沒特別驗證過，但不能排除），如果某個 fetch 剛好在 `emit()` 呼叫當下就跑完、它的 completion handler 把自己那個節點從 `snapshot_sigs_` 裡 `erase` 掉，用 range-for 的話，內部藏著的 iterator 這時候就指向一個剛被刪掉的節點，再往後走是未定義行為。第一版修法是手寫迴圈、`emit()` 之前先把 `std::next(it)` 存起來——但 `next` 指向的是**另一個**節點，如果 reentrant 地被刪掉的剛好是 `next` 指向的那個（例如兩個 symbol 同時 gap、各自的 fetch 都在飛），存起來的 `next` 一樣是懸空的，這個修法只擋住了「當前節點被刪」，沒擋住「任何節點被刪」。`cancellation_signal` 不能複製也不能搬移，沒辦法把它從 list 裡先搬出來避開這個問題，所以真正的修法是換一個角度：讓 completion handler 的 `snapshot_sigs_.erase(sig_it)` 改成 `net::post(strand_, [this, sig_it]{ snapshot_sigs_.erase(sig_it); })`——反正這個 handler 本來就跑在 `strand_` 上，把 erase 這件事延後到「目前這個 strand task 完全跑完之後」執行，`stop()` 的迴圈（本身也是一個 strand task）進行期間，`snapshot_sigs_` 就保證不會被任何 reentrant 的完成事件動到，range-for 因此又是安全的。
      **實作時發現、同樣沒預料到、比預期更棘手的坑：`run()` 呼叫 `drain_pending_snapshots()` 這件事本身就會被自己的 `run_sig_` 卡死**——`run()` 進到收尾這段時，`run_sig_` 早就已經 `emit(terminal)` 過了（不然不會走到這裡），而這個「已取消」狀態是**這個 coroutine 之後任何 `co_await` 都會立刻撞到的殘留狀態，不是只影響剛好被取消的那一個操作**。第一版修法只在 `drain_pending_snapshots()` 內部的 timer wait 上加 `net::redirect_error`，結果實測（`hermeneutic_venue_session_test` 的 `StopAbortsBackoffWaitAndDoesNotReconnect` 一開始就是這樣失敗的，訊息是 `co_await: Operation canceled`）發現：連 `co_await drain_pending_snapshots();` 這個呼叫本身——即使當下沒有任何 in-flight fetch、迴圈本體一次都沒真的執行——都會立刻拋出，因為「已取消」的狀態不是綁在某個具體的 I/O 物件上，而是整個 coroutine 鏈往下傳的環境狀態，任何後續 `co_await`（包括呼叫進另一個 coroutine 這件事本身）都會被判定為「已經被取消了」。真正的修法：在呼叫 `drain_pending_snapshots()` **之前**，先 `co_await net::this_coro::reset_cancellation_state();`，把這個環境狀態重置乾淨（仍然連著同一個 `run_sig_` 的 slot，只是清掉「已經 emit 過」這個殘留記錄），之後的 `co_await` 才會恢復正常行為。`drain_pending_snapshots()` 內部的 `net::redirect_error` 保留下來當第二層防護（萬一收尾途中又有新的 emit 落在這個迴圈執行期間），但真正解決問題的是這個 reset。
      **同一個坑的第二個發生位置，code review 實測 5/5 重現、比上面那個更嚴重，已修好並已跑過「刪掉修法會失敗、加回來會過」的驗證**：`reset_cancellation_state()` 一開始只加在 `drain_pending_snapshots()` 前面，但 `run()` 裡另一個完全不受保護的 `co_await` 是 `on_disconnected()` 的 invalidate 呼叫（緊接在 catch 區塊之後）。如果 `stop()` 是在連線還活著、卡在 `connection.read()` 的時候被呼叫——這其實是最常見的正式環境收尾情境，不是只有「stop 剛好落在 backoff 期間」這種邊角案例——那麼 read 本身被 cancellation 中止、留下同一種「已取消」的殘留狀態，緊接著的 invalidate 這個 `co_await` 就會立刻拋出、逃出 `run()`，連 `stopping_` 都還沒檢查到，`reset_cancellation_state()`/`drain_pending_snapshots()` 也永遠不會執行到。
      修法：在 `run()` 裡多加一次 `co_await net::this_coro::reset_cancellation_state();`（catch 之後、`invalidate_all()` 之前）。

      **這裡踩到第二層坑，值得記下來避免以後重蹈覆轍：曾經嘗試把 reset 移進被呼叫的 coroutine 本身，讓「cleanup 不會繼承殘留取消狀態」變成函式定義的性質而不是呼叫端要記住的紀律——這個方向實測是錯的，會導致 `StopAbortsBackoffWaitAndDoesNotReconnect` 這種原本正常的路徑也開始失敗。** 原因：`co_await SomeCoroutine()` 這個動作本身——也就是「進入」一個被 co_await 的 coroutine 這件事——如果呼叫端當下的環境取消狀態已經是「已取消」，會在**進入當下**就立刻拋出，被呼叫的 coroutine 本體（包括它自己開頭寫的 reset）根本沒有機會執行到。所以 reset 一定要發生在**呼叫端自己的 frame 裡、緊接在那次 `co_await` 呼叫之前**，不能委託給被呼叫的 coroutine 自己開頭做——委託給被呼叫方的版本，症狀跟一開始沒加 reset時一模一樣（因為呼叫本身在 reset 執行之前就已經先炸了）。最終定案：`invalidate_all()`／`drain_pending_snapshots()` 維持乾淨的、不知道 cancellation 這回事的普通 coroutine（純粹描述「要做什麼」），`run()` 自己在呼叫它們之前各自明確 `co_await net::this_coro::reset_cancellation_state();` 一次——兩個呼叫點都在 `run()` 本體裡，寫成註解提醒而不是包裝成看似「結構上保證」但實際上不成立的抽象。教訓：「呼叫進 cleanup 之前要 reset」這件事只在**每一個**可能接在一次取消後面的呼叫點做才算數，而且必須做在呼叫端，不是被呼叫端；之後如果在 `run()` 裡新增第三個這樣的呼叫點，需要同樣手動加這一行，沒有更省事的結構化寫法可以繞過去。
      補一個直接測試這個情境的迴歸測試（`stop()` 落在連線活著、卡在 `read()` 的時候，不是落在 backoff 期間）；原本 `StopDrainsInFlightSnapshotFetch` 的 `on_done` handler 沒檢查 `exception_ptr`，這個 bug 在既有測試裡是綠的，看不出來。

   3. **strand——第 10 節第 4 項「尚未決定」在這裡先接住了**：`VenueSession` 建構時用 `net::make_strand(executor)` 建一個 `net::strand<net::any_io_executor>`，`run()` 跟每個 `handle_request_snapshot` 的 spawn 都跑在這個 strand 上，`stop()` 本身也是 `net::post(strand_, ...)`——`symbol_syncs_`/`snapshot_sigs_`/`stopping_` 因此完全不需要額外的鎖，之後真的要調 `io_context` thread pool 大小（第 10 節第 4 項）不會再牽動這層。

   **`IngestionRunner`**：輸入是「per-venue 設定」，不是裸的 `VenueSession`——每個交易所可能只掛一部分 symbol，所以每個 `VenueSession` 要有自己那份 subset 建出來的 `SymbolRegistry`，不是共用一份全 symbol 的 registry。多個 venue 寫進同一個 `SymbolBook` 本來就是安全的（`apply_batch`/`invalidate_venue` 是 per-`VenueId`、且有自己的 mutex），這點值得在這裡明講，免得之後有人看到多個 session 碰同一個 `SymbolBook*` 就緊張。

   ```cpp
   class IngestionRunner {
     public:
       template <typename Feed, typename Policy, typename NextLayer, typename... Args>
       void add(Args&&... args) {  // 就地建構進 VenueSessionAdapter<Feed,Policy,NextLayer>
           sessions_.push_back(std::make_unique<VenueSessionAdapter<Feed, Policy, NextLayer>>(
               std::forward<Args>(args)...));
       }
       void start_all() { for (auto& s : sessions_) s->start(); }  // executor 已經在 add() 傳進去的 VenueSession 建構參數裡定案
       void stop_all() { for (auto& s : sessions_) s->stop(); }
     private:
       std::vector<std::unique_ptr<IVenueSession>> sessions_;
   };
   ```

   `aggregator_main.cpp` 原本手寫的「建一個 Binance `VenueSession` + `co_spawn` + 錯誤 log lambda」整段，已經換成 `runner.add<BinanceFuturesFeed, BinanceFuturesSequencePolicy, ssl_stream>(..., io.get_executor(), &ssl_ctx)` + `runner.start_all()`；之後加 OKX 只是再一行 `add<...>`。錯誤 log 的 per-venue 標籤收進 `VenueSessionAdapter::start()` 內建的 completion handler 裡（用 `session_.venue()` 當前綴），不用每個 venue 各自複製一份。per-venue factory 函式（例如 `make_binance_futures_session(...)`，把具體 template 參數留在該交易所自己的 `.cpp`）目前還沒做，只有一個交易所時還不需要，等真的加第二個 venue 時再看要不要加這層。

   **驗收標準（已用 `IngestionRunnerTest.StopAllLetsIoContextFinishWithoutIoStop` 驗證）**：`runner.stop_all()` 之後，`io_thread.join()`（main.cpp 現有的收尾）**不需要**額外呼叫 `io.stop()` 就能返回——測試裡用一個 3 秒的 watchdog 執行緒當安全網（避免真的卡死時整個測試 binary 掛住），並斷言這個 watchdog **沒有**被觸發過，這才是真正在驗證「不需要 `io.stop()` 兜底」，而不是「反正呼叫了 `io.stop()` 所以測試會結束」。`aggregator_main.cpp` 也已經按這個驗收標準改寫（見第 7 項）。

   **已知限制，刻意不做**：`aggregator_main.cpp` 今天沒有任何訊號處理（`SIGTERM`/`SIGINT`），`server->Wait()` 只會在有人呼叫 `server->Shutdown()` 時返回——而目前沒有任何程式碼會呼叫它。也就是說 `runner.stop_all()` 這條優雅收尾路徑，今天在正式環境裡實際上**還沒有任何觸發點**會走到。這不是這次改動造成的退步——改動前的 `io.stop()` 收尾路徑一樣沒有觸發點——但值得明講，避免以為「stop() 做完了，正式環境就會優雅關閉」。要接訊號處理是後續獨立的事，不在這次範圍內。

   **已對真實 Binance Futures 實測（用完即刪的臨時程式，不在 repo 裡）**：原本只在 fake WS/HTTP test double 上測過 `stop()`，沒驗證過真正的 `net::ssl::stream<beast::tcp_stream>`（正式環境用的 NextLayer）底下取消/收尾是否一樣正常——fake server 用的是 `beast::tcp_stream`，沒有 TLS handshake/teardown 這一段。寫了一個臨時的 `service/live_binance_stop_check_main.cpp`（跟 `aggregator_main.cpp` 幾乎一樣，差別只是跑完後主動呼叫 `stop_all()` 並量時間），實際跑起來連上 `wss://fstream.binance.com`、訂閱 BTCUSDT，15 秒內收到 1 個真實 snapshot + 142 個真實 diff，接著呼叫 `stop_all()`：`io_thread.join()` 在 **3ms** 內返回,不需要 `io.stop()`。跟 fake test double 的結果一致，把「http_get/SSL stream 這條沒特別驗證過」的殘留風險（見上面第 2 點）從真實環境角度補上了一次驗證。驗證完後這個臨時檔案跟對應的 CMake target 已經刪掉，沒有留在 repo 裡（跑真實交易所需要網路、不適合放進 CI）。

   **Code review 第二輪抓到、已修好的兩點**：
   - **`stop()` 在 `start()` 從沒跑過之前被呼叫，會被靜默、永久地丟掉**：`stop()` 只需要 `strand_` 存在（建構時就有了）就能成功 `net::post`、把 `stopping_` 設成 `true`；`run()` 原本要到第一次斷線/backoff 循環之後才會檢查 `stopping_`，所以一個「還沒 start 就被 stop」的 session 理論上會照樣連線、照樣讀真實交易所的資料。
      **第一版修法（`run()` 的 `while(true)` 迴圈最上面加 `if (stopping_) co_return;`）實測是不夠的，而且原因跟 §5 那個「latching」坑是同一個家族但更極端**：`net::co_spawn(strand_, run(), net::bind_cancellation_slot(run_sig_.slot(), on_done))` 這行呼叫本身，會在呼叫當下（不管 `run()` 的 coroutine 有沒有機會真的執行過一行）就把 `run_sig_` 的 slot 綁定到這個 coroutine。如果 `stop()` 早就 emit 過（`start()` 都還沒被呼叫），那麼一旦 `start()` 真的呼叫 `co_spawn` 完成綁定，`run_sig_` 上那次 emit 會回溯地命中這個剛綁好、一行都還沒執行過的 coroutine——整個 `co_spawn` 出來的 operation 會直接以 `operation_aborted` 結束，`run()` 的函式本體（包括我加在最上面的那個 `stopping_` 檢查）**完全沒有機會執行到**（用一系列 debug print 實測過：一個都沒印出來）。也就是說，先前那個「連線讀真實資料」的擔心沒有發生，實際發生的是另一件事：`on_done` 收到一個 `operation_aborted` 例外，而不是乾淨的 `nullptr`——`VenueSessionAdapter::start()` 因此會把一次完全正常、預期內的「還沒開始就被要求停止」記成一次「ingestion session ended: Operation canceled」的錯誤 log。
      **真正的修法在 `start()`，不是 `run()`**：把 `start()` 原本「直接呼叫 `co_spawn`」改成先 `net::post(strand_, ...)` 把「檢查 `stopping_`、視情況才 `co_spawn`」這件事本身也丟進 strand 排隊，跟 `stop()` 的 post 用同一個佇列排序。如果 `stop()` 先被排進去、先執行（設好 `stopping_ = true`，這時候 `run_sig_.emit()` 因為還沒有任何 coroutine 綁定它，是真正無害的 no-op），那麼 `start()` 排進去的檢查邏輯之後執行時就會看到 `stopping_ == true`，直接 `on_done(nullptr)`、完全不呼叫 `co_spawn`——`run_sig_` 的 slot 永遠不會被綁到一個已經沒有意義的 coroutine 上，也就不會有例外被製造出來。`run()` 迴圈最上面那個 `if (stopping_) co_return;` 保留下來當第二層防護（涵蓋 coroutine 已經綁定、但真的開始執行前那個更窄的時間窗，如果真的存在的話），但真正解決問題的是 `start()` 這一層。新增 `StopBeforeStartPreventsConnecting` 測試直接驗證：先 `stop()` 再 `start()`，斷言完全沒有嘗試連線、`on_done` 乾淨地帶 `nullptr` 觸發。
   - **`aggregator_main.cpp` 拿掉 `io.stop()` 之後，收尾沒有 bounded-time 的兜底**：原本粗暴的 `io.stop()` 順便也是「不管三七二十一，時間到了就返回」的保證；改成純 `runner.stop_all()` 之後，如果 `drain_pending_snapshots()` 真的卡住不收斂（例如某個未來場景下 cancellation 沒能讓某個 async 操作真的中止），`io_thread.join()` 會無限期卡住，正式環境的 process 就真的關不掉。修法：仿照 `IngestionRunnerTest` 裡的 3 秒 watchdog 手法，在 `aggregator_main.cpp` 加一個 `kShutdownTimeout`（10 秒）的 watchdog 執行緒——正常情況下（已經實測，見上面「已對真實 Binance Futures 實測」，`join()` 3ms 內就返回）watchdog 完全不會被觸發；真的卡住的話，才強制 `io.stop()` 兜底，並且印一行 log 說明發生了什麼，不再是無聲卡死。

   **Code review 第二輪提過、確認過但暫不處理的幾點**：
   - `run()` 最外層的 `catch` 只接 `const std::exception&`——這是既有行為，不是這次改動加的，但這次新加的 `stop()`/`net::post(strand_, ...)` 讓後果多了一種：如果哪次真的丟出非 `std::exception` 的例外，`run()` 會直接以未捕捉例外結束，之後任何 `stop()` 呼叫 post 到 `strand_` 的工作就沒人執行、也沒有任何 log。維持現狀，因為擴大這個 catch 的範圍是另一個獨立的決定，不屬於這次的範圍。這個既有的殘留風險現在多牽動一件事：`execute_action()` 裡 snapshot fetch 完成後那個 deferred 的 `snapshot_sigs_.erase(sig_it)`（見上面第 2 點）也是靠 `net::post(strand_, ...)` 排隊執行的——如果 `run()` 真的因為這條路徑以未捕捉例外結束（跳過 `drain_pending_snapshots()`），這個 session 之後被銷毀時，那個排隊中、還沒執行的 erase lambda 就會對著一個已經解構的 session 呼叫 `snapshot_sigs_.erase(...)`。前提跟這條本來就記錄的殘留風險是同一個，不是新的獨立問題。新加的 `kShutdownTimeout` watchdog 對這個場景沒有幫助——它保證的是「`io_thread.join()` 不會無限期卡住」，不是「`run()` 一定會正常收尾」，兩者是不同的保證。
   - `IngestionRunner::stop_all()` 本身不回報「全部真的收尾完了」——「`on_done` 觸發後才能安全銷毀 session」這個承諾，目前是靠唯一的呼叫端（`aggregator_main.cpp` 在 `stop_all()` 後面接著 `io_thread.join()`）湊巧做對，`IngestionRunner`/`IVenueSession` 本身沒有把這個承諾做成 API 的一部分。之後如果出現第二種呼叫端（例如不是靠 join 一個專屬 `io_context` 的方式），需要重新設計一個「等全部 session 真的 drain 完」的介面，不是現在就加。
   - `drain_pending_snapshots()` 用 5ms 一次的 busy poll，不是事件驅動——這是設計時就承認的取捨（見上面的函式註解），沒有新資訊顯示它現在有問題，維持原樣。
   - 測試檔案裡 `fail_test_on_exception()` 這個 helper 在 `tests/` 目錄下重複了第四次（`websocket_connection_test.cpp`/`http_client_test.cpp`/`venue_session_test.cpp` 都各自有一份，`ingestion_runner_test.cpp` 又複製一次）。這個重複在這次改動之前就存在，`ingestion_runner_test.cpp` 只是照抄既有慣例；要抽成共用的 test-utils header 得動到其他三個既有測試檔案，超出這次改動的範圍，不在這裡處理。

   **Code review 第三輪抓到、已修好的四點**：
   - **`on_disconnected()` 的 invalidate 呼叫（現在叫 `invalidate_all()`）在 `run()` 裡沒包 try/catch**：如果它拋例外（`registry_.book(symbol)->apply_snapshot`/`apply_batch`/`invalidate_venue` 理論上只操作記憶體內的狀態，正常不該拋，但例如 `bad_alloc` 這種資源耗盡的情況並非不可能），會直接逃出 `run()`，跳過 `stopping_` 檢查跟 `drain_pending_snapshots()`——正是這一整套機制想避免的 use-after-free，只是換一個觸發點。修法：`co_await invalidate_all();` 包一層 `try/catch (const std::exception&)`，即使 invalidate 失敗，收尾邏輯（`stopping_` 檢查、drain）還是會執行到。順便把 `disconnected` 這個布林值拿掉——內層 `while(true)` 讀取迴圈完全沒有 `break`/`return`，唯一離開外層 `try` 的方式就是丟例外，所以 `disconnected` 在檢查點上永遠是 `true`，是一個死掉的條件，拿掉之後 `invalidate_all()` 直接無條件呼叫，程式碼更誠實地反映實際行為。
   - **`handle_request_snapshot()` 沒有在套用結果前檢查 `stopping_`**：`stop()` 會對每個 in-flight fetch 自己的 `cancellation_signal` 各別 `emit()`，但 fetch 走的 resolve/connect/read 這條鏈（`http_get`）的 per-op cancellation 沒有像 WS read/timer 那樣被明確驗證過——如果取消沒有即時生效、回應還是正常送達，這個 fetch 的完成處理（`execute_actions` → `ApplySnapshot`/`ApplyDelta`）就會在 `invalidate_all()` 已經因為這次收尾而 invalidate 過這個 venue 之後，重新把資料寫回去，等於悄悄復活一個「已經被宣告失效」的 venue 貢獻。修法：`handle_request_snapshot()` 在 `fetch()` 拿到回應之後、真正套用之前，多一個 `if (stopping_) co_return;`——不管底層的 cancellation 有沒有即時生效，只要已經進入收尾，這個結果就不該被套用。
   - **`heartbeat_thread` 用 `.detach()`，正式環境的 shutdown 路徑一旦真的走到底會 use-after-free**：`aggregator_main.cpp` 目前沒有訊號處理，`server->Wait()` 實際上永遠不會返回（見上面「已知限制」），所以這個 UAF 今天不會真的發生——但這是既有程式碼（這次改動之前就有），而這次改動剛好是在替「`server->Wait()` 真的返回之後」這條路徑做收尾的正確性工程，放著這顆地雷不管、等哪天真的接了訊號處理才爆炸並不合理。`main()` 結尾 `return 0;` 那一刻會解構區域變數 `service`；如果 `heartbeat_thread` 這時候還活著（睡到一半），下一輪醒來會對著已經解構（或正在解構）的 `service` 呼叫 `send_heartbeat()`。修法：拿掉 `.detach()`，改成一個 `std::atomic<bool> heartbeat_stop`，`main()` 收尾時設成 `true` 並 `join()`，跟這次新加的 ingestion 收尾邏輯一樣，在 `service` 真的被解構之前就確保這個執行緒已經停了。
   - **`kShutdownTimeout`（10 秒）比 `WebSocketConnection::connect()` 自己的 30 秒 connect/TLS-handshake timeout 還短**：如果 `stop()` 剛好落在一次還在走、但沒有卡死、只是正常地要花將近 30 秒的 connect/handshake（例如網路狀況不好），這個 watchdog 會在真正的優雅收尾有機會完成之前就先開槍，強制走回粗暴的 `io.stop()`——等於讓「10 秒內沒收尾完就當作卡死」這個假設，把「正常但慢」跟「真的卡死」混為一談。修法：把 `kShutdownTimeout` 拉到 35 秒（比 30 秒的 connect timeout 留一點餘裕），註解裡明講這個數字為什麼要比已知最長的單一操作 timeout 還長。即使如此，`tcp::resolver` 底層的 `getaddrinfo()` 是在背景執行緒上跑到 OS 呼叫真的返回為止，不受 awaitable 層級的 cancellation 影響——一個真的卡死/被黑洞掉的 DNS 解析仍然可能讓 process teardown 拖過這個 timeout，這是 Boost.Asio/OS 層級的限制，不是拉長 timeout 數字能解決的問題，註解裡也明講了。

   **Code review 第三輪提過、確認過但暫不處理的兩點**：
   - `VenueSession::start()` 沒有防止被呼叫兩次的執行期防護——第二次呼叫會用一個新的 `bind_cancellation_slot` 蓋掉 `run_sig_` 原本的綁定（跟「每個 snapshot fetch 要有自己的 signal」是同一個「slot 只轉發給最近一次綁定」的道理），讓第一個 `run()` 變成孤兒、`stop()` 之後只會抓到第二個。目前唯一的呼叫端（`IngestionRunner::add()`）就結構上只會呼叫一次，不會踩到；已經在 `start()` 的文件註解裡把這個當作明確的前置條件寫清楚（只能呼叫一次），沒有加執行期的 assert/guard——現在沒有任何呼叫端會誤用，加防護是為了一個假設性的未來呼叫端先寫代碼，不是這次範圍要做的事。
   - `backoff()` 的 `catch (const std::exception&)` 會接住所有 `std::exception`，不只是 `stop()` 造成的取消——但這跟 `run()` 最外層那個 catch 是同一套刻意的設計哲學（見第 1 點的既有 catch 註解：「stop()'s terminal cancellation surfaces exactly the same way... which is why it's stopping_ - not a separate exception type - that tells the two apart」），不是疏漏。改成只接特定的 cancellation 例外型別，會讓這個檔案裡兩個原本一致的 catch 語意產生分歧，不在這裡動。

   **考慮過但否決：把 `VenueSession` 從 coroutine 改寫成 callback 風格，換取「更簡單」**。否決理由：上面幾個問題（cancellation 時序、孤兒非同步操作的生命週期、共享狀態要不要鎖）在 callback 風格下同樣存在，只是換一種說法——不是「`cancellation_signal` + `stopping_` flag」，而是「`shared_ptr<this>` keep-alive + 讓 `close()` 逼所有 pending callback 帶錯誤回來」。callback 風格對「孤兒 snapshot fetch」這種生命週期問題確實有更現成的慣用手法（引用計數天然處理），但代價是把 `run()` 現在線性的「connect → subscribe → read loop → backoff → 重來」拆成一堆各自存 member 狀態的 handler 函式——這正是第 7 節記錄的兩個真實 bug（`co_spawn` 內聯卡死讀取迴圈、參考參數在 spawn 後懸空）已經在這層邏輯本身踩過的坑，coroutine 版本讓這兩個坑至少在字面上更容易看見。維持 coroutine。
   另外，若之後有人參考通用 C++20 coroutine runtime 文章想套用 `std::stop_source`/`std::stop_token`：**這在 Asio 裡不夠**，`stop_token` 只是被動旗標，本身叫不醒一個正在 `co_await` 卡住的 `async_read`/`async_wait`；真正能中斷 pending I/O 的是 Asio 的 `cancellation_signal`/`cancellation_slot`（即上面第 1 點），兩者不能互相替代。
3. **backoff/jitter 的實際參數**：形狀已定（per-connection，帶 jitter：`min(30s, 500ms * 2^attempt)` + 最多 20% 隨機抖動），數值是暫定的，未經真實流量調校。
4. **多執行緒 `io_context` thread pool 的大小**：先前討論過大方向（parsing 平行、apply 序列化在各自 `SymbolBook` 的 mutex 上），實際執行緒數量策略未定；`VenueSession::run()` 目前也還沒實際跑在多執行緒 `io_context` 上測試過，只驗證過單執行緒 `io_context::run_for()`。**strand 這半步已經在第 2 項（`IngestionRunner`/`IVenueSession`）裡先接住**：`VenueSession` 拿一個 `net::strand`，`run()` 跟 `handle_request_snapshot` 的 spawn 都掛在同一個 strand 上，這裡才不會被之後真的上多執行緒的決定回頭咬。
5. ~~Binance Spot 的 `SequencePolicy`~~ **已完成**：`BinanceSpotSequencePolicy`（`include/bobby/hermeneutic/symbol_sync.hpp`）+ `BinanceSpotFeed`（`service/binance_spot_feed.hpp`），對照 developers.binance.com 的 Spot 文件直接 WebFetch 逐字核對（2026-09-18，見文末 Sources），不是套用 Futures 的公式：
   - 丟棄條件 `final_id <= last_update_id`（非嚴格 `<=`，跟 Futures 的嚴格 `<` 不同）
   - 銜接條件 `first_id <= last_update_id+1 && final_id >= last_update_id+1`（有 `+1` 偏移，Futures 沒有）
   - 穩態校驗用 `first_id == last_applied_final_id + 1`（"U 接續上一則的 u+1"），不是 `pu` 反向指標——Spot 的 `depthUpdate` 根本沒有 `pu` 欄位，`BinanceSpotFeed::parse_message` 把 `DepthUpdate::prev_final_id` 明確設成 `0`（不是留給 UB：`DepthUpdate` 沒有預設成員初始化，不明確賦值會是未定義值，`BinanceSpotSequencePolicy` 剛好從不讀這個欄位，這種 bug 不會被任何測試抓到，只能靠寫程式碼時就注意）。

   Spot 文件另外還記載了第三個分支「`u` 小於本地 book 的 update ID 就直接忽略該事件」，`SymbolSync` 沒有對應的 ignore action——比 `is_contiguous` 判定失敗更早的一種「太舊」情況，目前會落到 gap 處理（整個重來），跟真的「太新」的 gap 用同一套處理。單一有序 TCP 連線上理論上不會發生（等於交易所自己送出亂序事件），刻意不特別建模，記在 `BinanceSpotSequencePolicy` 的註解跟這裡，避免被誤會成遺漏。

   `BinanceSpotFeed` 跟 `BinanceFuturesFeed` 差異：WS 走 `stream.binance.com:9443/ws`（Spot 明文 WS port 是 9443，不是 443；bare `/ws` + SUBSCRIBE 已直接對真實 Binance 驗證過會送出**未包裝**的 payload，不是 `/stream` 那種 `{"stream":...,"data":...}` 包裝格式——這件事 sans-io 測試本身測不出來，見下方即時驗證），REST snapshot 走 `api.binance.com/api/v3/depth?symbol=...&limit=5000`（Spot 上限 5000，Futures 上限 1000；選滿額度是因為 Spot 的 `<=` 丟棄規則比 Futures 的嚴格 `<` 更容易在冷門 symbol 上把 buffer 清空、導致 `on_snapshot` 立刻又發一次 `RequestSnapshot`，滿額度快照能降低重試頻率，但沒有加任何節流機制去解決這個交互作用，只記在這裡）。

   **`HttpRequestSpec` 跟 `detail::parse_decimal_string`/`parse_level`/`parse_levels` 抽到新的 `service/binance_wire.hpp`**，原本只在 `binance_futures_feed.hpp` 裡私有定義——`VenueSession` 本來就是靠 `auto spec = feed.snapshot_request(symbol)` 鴨子定型讀取 `.host`/`.port`/`.target`（已確認 grep 過 `venue_session.hpp` 完全不 include 任何 Feed header、不具名引用 `HttpRequestSpec` 型別），理論上兩個 Feed 各自定義一份同名 struct 也不會被 `VenueSession` 擋下來，但 `aggregator_main.cpp` 一份 `.cpp` 同時 include 兩個 Feed header 時，兩份完全相同的 `namespace bobby::hermeneutic::ingestion { struct HttpRequestSpec {...}; namespace detail { ... } }` 會直接 ODR 違規（重複定義）。這是真正的兩個使用者才動手抽的（不是預先設計），`binance_futures_feed.hpp` 也已經改成 include 這個共用 header，不再重複定義。

   **即時驗證（跟 Futures 用完即刪的臨時程式同一招，`service/live_binance_spot_check_main.cpp`，驗證完已刪除，不留在 repo）**：實際連上 `wss://stream.binance.com:9443/ws`，送 `SUBSCRIBE` 訂閱 `btcusdt@depth@100ms`，收到 1 個 SUBSCRIBE ack（`nullopt`，符合預期）+ 3 個真實 `depthUpdate`（`parse_message` 正確解析出 `DepthUpdate`，`U`/`u`/bids/asks 都有值），證實 bare `/ws` 對 Spot 確實跟 Futures 一樣送未包裝 payload，不是 `/stream` 才有的包裝格式（這是 sans-io 測試結構性測不出來的唯一一件事，因為需要真的連線才知道 wire 上到底送什麼）。REST 那邊也對 `https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000` 實際打過，收到 `lastUpdateId`+5000 bids+5000 asks，`parse_snapshot_response` 正確解析。未做 Futures 之前做過的長時間（十幾分鐘）穩定性 soak test——這次只驗證「wire format 假設是否成立」這一個問題，不是收尾/backoff/cancellation 這些已經在 `VenueSession` 本體驗證過、與交易所無關的機制。

   **`aggregator_main.cpp` 已接上兩個 venue**：每個 symbol 同時建 `IngestionRunner::add<BinanceFuturesFeed, BinanceFuturesSequencePolicy, ...>` 跟 `add<BinanceSpotFeed, BinanceSpotSequencePolicy, ...>`，各自一份獨立的 `SymbolRegistry`（不能共用同一份再 `std::move` 兩次——第一次 `add()` 會把 registry 整個搬空，第二個 venue 拿到的會是 moved-from 的空 map，`registry_.book(symbol)` 永遠回傳 `nullptr`）。~~同一個 `SymbolBook` 因此會同時收到 spot 跟 perp 兩個 `VenueId` 的貢獻，聚合後的 aggregate book 是兩個市場流動性的合併~~ **這個「spot+perp 合併進同一本 book」的決定，第 9 節加 OKX 的時候被推翻了**：spot 跟 perp 是不同的商品、不同的價格跟流動性，合併從來就不是刻意設計的結果，只是碰巧每個 venue 都拿裸的 canonical symbol 當 registry key。現在 Futures/Spot 各自接 `.PERP`/`.SPOT` 兩個獨立的 `SymbolBook`，見第 9 節。
6. ~~正式環境的 `net::ssl::context` 建構/憑證驗證設定~~ **已完成並實測**：`aggregator_main.cpp` 接上 ingestion 後第一次真的編譯到 `net::ssl::stream<beast::tcp_stream>` 這個 template 實例化，發現漏了 `#include <boost/beast/websocket/ssl.hpp>`（Beast 對 SSL stream 的 `async_teardown` customization point 是獨立 header，沒 include 的話會在 `boost/beast/websocket/teardown.hpp` 出現 `static_assert(sizeof(Socket)==-1, "Unknown Socket type in async_teardown.")`）。修好後**實際跑起來連上 `wss://fstream.binance.com/ws` + `https://fapi.binance.com`，收到真實 BTCUSDT order book**（snapshot 1928 bids/1854 asks，diff 持續進來）。另外完成一次約 19 分鐘的即時雙 symbol（BTCUSDT、ETHUSDT，同一個 `hermeneutic_aggregator_service` process）穩定性驗證：全程只建立 1 次連線（無斷線重連），雙邊都收到真實 snapshot 並持續套用 diff（各自 diff_count 達 7500+ 才手動停止，不是自然結束），heartbeat 全程以預期節奏送達，驗證用的 log 裡沒有任何 error/disconnect 紀錄。（負責跑這個驗證的背景 agent 自己中途就停在一則「等 15 分鐘再回報」的訊息、沒有真的送出最終報告或清掉留下的兩個 process；這份紀錄是事後直接讀它留下的 log、確認狀態正常後，手動收尾補上的。）
7. ~~`aggregator_main.cpp` 尚未接上 ingestion~~ **已完成，現在接六個 venue**：見 `service/aggregator_main.cpp`——建構 `AggregatorService` 後，額外建一個 `net::ssl::context`（`tlsv12_client`，`set_default_verify_paths()` + `verify_peer`），透過 `IngestionRunner::add<Feed,Policy,NextLayer>()` 接上 `BinanceFuturesFeed`/`BinanceFuturesSequencePolicy`（VenueId `"binance_futures"`）、`BinanceSpotFeed`/`BinanceSpotSequencePolicy`（VenueId `"binance_spot"`）、`BybitLinearFeed`/`BybitSequencePolicy`（VenueId `"bybit_linear"`）、`BybitSpotFeed`/`BybitSequencePolicy`（VenueId `"bybit_spot"`）、`OkxFeed`/`OkxSequencePolicy`（VenueId `"okx_spot"`/`"okx_swap"`，同一個 Feed/Policy 型別接兩次，見第 9 節）六個 venue，`co_spawn` 上一個獨立的 `io_context`（自己的 thread 跑 `io.run()`），跟原本的 gRPC server／heartbeat thread 並存，`server->Wait()` 回來後 `runner.stop_all()` + join 收尾。**每個 venue 一份獨立的 `SymbolRegistry`，不是共用一份再 `std::move` 好幾次**：`VenueSession` 的建構子把 `registry` 整個按值搬進去，同一份 registry 傳給後面的 `add<>()` 只會拿到 moved-from 的空 map，`registry_.book(symbol)` 永遠回 `nullptr`。~~同一個 `SymbolBook` 因此同時收四個 venue 的貢獻，聚合後是四邊流動性的合併~~ **spot/perp 從第 9 節起分成兩本獨立的 `SymbolBook`**：perp/futures/swap 三個 venue（Binance Futures、Bybit linear、OKX swap）寫進 `.PERP` 那本，spot 三個 venue（Binance Spot、Bybit spot、OKX spot）寫進 `.SPOT` 那本，`SymbolBook::apply_batch`/`invalidate_venue` 本來就是 per-`VenueId`（見第 10 節第 2 項）這件事現在用在「同一本 book 裡三個 venue 各自記帳」上，不是「六個 venue 全部混在一本 book 裡」。之後真要再加第七個交易所，一樣走 `IngestionRunner::add<>()`，不需要再動這層邏輯。
8. **`SymbolSync` 的 live-gap 恢復機制，對 `kTrustsConnectionOrder == true` 的 venue（目前是 Bybit）比對 Binance 更需要處理**：見上方 `BybitSequencePolicy` 修正記錄的最後一段——live 狀態下發生 gap，`on_depth_update()` 只回傳 `InvalidateVenue`，並沒有機制讓這個 symbol 真正離開 `Buffering`（不像剛連線時有 `kTrustsConnectionOrder` 的捷徑可以直接從下一筆 snapshot 起步——這個捷徑生效的前提是「進入函式當下 buffer 完全沒被寫過」，live-gap 重新進入 Buffering 之後這個前提已經不成立，捷徑不會、也不該對這種情況生效，見 `PostGapBufferedEventIsNotDiscardedByTheEmptyBufferShortcut` 那個測試），除非連線真的斷線重連。Binance 的 REST 模型下這同樣是個限制，但理論上隨時可以再補一個「gap 時主動重新 `RequestSnapshot`」的機制；Bybit 文件明講「訊息中途收到 `u=1`」是伺服器端重啟的正常訊號，代表這種 mid-stream desync 對 Bybit 是**預期會發生**的事件，不是邊界情況——這個尚未解決的限制對 Bybit 的實際嚴重程度因此比對 Binance 高。尚未設計具體修法（可能方向：gap 時如果 `kTrustsConnectionOrder`，主動觸發整條連線重連，而不是留在同一條連線上乾等）。
9. **加入 OKX（`service/okx_feed.hpp`，`OkxSequencePolicy`）**：Channel 選 `books`（400 檔增量深度 + WS 推 snapshot），不用 `books5`（每次全量快照，非增量）或 `books-l2-tbt`（VIP 限定）。

   **`OkxSequencePolicy` 用的是 `kTrustsConnectionOrder` 這個既有機制**（`symbol_sync.hpp`，加 Bybit 時已經加進去，不是這次重新設計），驗證方式不太一樣：使用者直接貼了 OKX 官方文件對 `seqId`/`prevSeqId` 的說明（含 idle-heartbeat `prevSeqId==seqId` 跟 sequence-reset `seqId` 本身會往回跳兩個例外情況），逐步驗算文件給的四則訊息範例（snapshot seqId=10 → 15 → 15(heartbeat) → 3(reset) → 5）證實純粹用 `prevSeqId == 上一則訊息的 seqId` 這條鏈式規則四種情況全部正確——不需要對 reset 額外開分支，跟 Binance Futures 的 `pu` 反向指標是同一種模型。Checksum（CRC32）刻意不做，只靠這條鏈式比對，跟 Binance/Bybit 目前的嚴謹程度一致（都沒有額外的完整性校驗層）。

   **OKX 現貨/永續是同一個 Feed/Policy 型別，不像 Binance/Bybit 各自兩個 Feed 類別**：OKX 的 `books` channel 在 protocol 層面對兩者完全一樣（同一個 endpoint、同一個 channel 名稱、同一種訊息格式），差別只在 `instId` 字串（現貨 `BTC-USDT`、永續 `BTC-USDT-SWAP`）。`OkxFeed` 因此只有一個類別，~~`aggregator_main.cpp` 用同一個型別接兩次（VenueId `"okx_spot"`/`"okx_swap"`，symbol 清單依 `is_swap()` 分流）。到期合約（dated futures，例如 `BTC-USDT-250328`）、選擇權這次不支援——`okx_canonical()` 對這種形狀回傳 `std::nullopt`，`aggregator_main.cpp` 遇到就直接報錯退出，不會靜默算出一個不存在的 canonical symbol。~~ **`okx_canonical()`/`is_swap()`（native instId → canonical 方向的判斷）已刪除**：現在 `aggregator_main.cpp` 接的是 JSON subscription config（見第 11 節之後，`book_subscription.hpp`），每個 book 該由哪些 venue、哪種 instrument type 供應是設定檔本身講清楚的，`symbol.hpp` 的 `native_symbol()` 走反方向（canonical → native，config 的 base/quote 已經拆好、不用猜）直接算出 `okx_spot`/`okx_swap` 各自要訂閱的 instId，VenueId 分流因此不再需要在 native instId 上判斷 `is_swap()`；到期合約/選擇權那種形狀，一樣是設定檔階段就沒有對應的 venue/type 組合，不會被拿去訂閱。

   **Spot 跟 Perp 現在是兩個獨立的 `SymbolBook`，不是合併成一個**——這是這次連帶做的架構調整，比「加 OKX」本身範圍更大：canonical symbol 多一個對稱、顯式的後綴慣例，`.PERP`（永續/期貨）跟 `.SPOT`（現貨），兩邊都要標示，不是其中一邊沿用舊的裸符號。`AggregatorService` 的符號清單因此從 `symbols` 改成 `{symbol+".PERP", symbol+".SPOT"}`，Binance Futures/Bybit linear/OKX swap 三個 venue 接 `.PERP`，Binance Spot/Bybit spot/OKX spot 三個 venue 接 `.SPOT`。這改變了 gRPC 可訂閱的 symbol 字串（`request->symbol()` 直接就是 `AggregatorService::book()` 的 key）：client 現在要訂閱 `"BTCUSDT.PERP"`，不是 `"BTCUSDT"`。`SymbolRegistry`/`VenueSession`/`AggregateOrderBook` 完全不用改——本來就是任意字串當 key，純粹是 `aggregator_main.cpp` 的 wiring 層決定。

   **OKX 的 keepalive：實測後判定不需要動 `VenueSession`**。OKX 一般文件描述應用層文字 `"ping"`/`"pong"` 是建議的連線保活機制，跟 Binance/Bybit 靠 WebSocket 協定層 ping/pong（Beast 自動處理）不同，一開始評估過這可能需要在 `VenueSession::run()` 的讀取迴圈旁邊多開一個 sibling coroutine 定期送 `"ping"`——這是這個專案目前唯一 venue-agnostic 的核心、也是踩過三次真實 coroutine bug 的檔案，動之前先做了即時驗證閘門：實際訂閱 `wss://ws.okx.com:8443/ws/v5/public` 的 `books/BTC-USDT`（40 秒）跟 `books/TRX-USDT`（80 秒，超過文件說的 ~60 秒「長時間無更新」心跳門檻），client 端除了一開始的 subscribe 請求之外全程不送任何東西（2026-09-18）。兩次連線全程正常，持續收到真實 update 或 OKX 自己送的 idle-heartbeat（空 bids/asks），沒有斷線。結論：只要訂閱了至少一個 instId（`VenueSession` 連上後一定會訂閱），OKX server 端自己的推播就足以維持連線，不需要 client 主動送任何東西——`venue_session.hpp` 完全不用動，`OkxFeed::parse_message` 對字面 `"pong"` 的特判留著當無害的保險，不代表這條路徑真的會被觸發。

   **第 8 項提到的 live-gap 恢復限制，同樣適用於 OKX**：`kTrustsConnectionOrder` 的捷徑只在「進入 `on_snapshot()` 當下 buffer 完全沒被寫過」時生效，mid-stream 的 gap 會讓這個 symbol 卡在 `Buffering` 直到整條連線斷線重連——跟 Bybit 面對的是同一個尚未解決的限制，不是 OKX 特有的新問題。

10. ~~`symbol.hpp::book_key()` 的字串格式是這個專案目前唯一還在製造「無法還原」歧義的地方，尚未決定要不要改，牽涉 breaking change~~ **已完成**：`native_symbol()`/`venue_id()` 都吃 `(Venue, BaseQuote, BookType)`、對整個 domain model保持 total；只有 `book_key()` 把 `{BTC, USDT}` 攤平成無底線的 `BTCUSDT.SPOT`/`BTCUSDT.PERP`，base/quote 的邊界從此永久消失——跟 `split_base_quote()` 自己註解講的「串接字串在沒有維護 quote-asset 字典時無法反推邊界」是同一個問題，也是這個專案先前刪掉 `okx_canonical()`（第 9 節、`5d5dd69`）的同一個理由：猜邊界是死路，`book_key()` 是最後一個還在做這件事的地方。判斷式：能不能寫出 `parse_book_key(s) -> optional<pair<BaseQuote, BookType>>` 當精確反函式？現在不行。連帶地，`book_subscription.hpp` 的 `seen_book_keys` 用串接後的字串去重，「不同的 (base, quote, type) 不會撞成同一個 key」這件事目前是靠實際幣別代碼的巧合成立，不是靠結構保證。

    **現況是刻意的，先講清楚不是 bug**：`book_key()` 的註解本來就講明「client 訂閱 `"BTCUSDT.PERP"` 不受設定檔輸入拼寫（`BASE_QUOTE`，例如 `BTC_USDT`）影響」——`BASE_QUOTE` 底線只是設定檔/CLI 的*輸入*拼寫慣例，用來消除 base/quote 邊界的歧義；`book_key()` 產生的才是 gRPC client 實際訂閱用的*穩定* key，兩者故意脫鉤，這樣設定檔輸入格式以後要改，既有 client 的訂閱字串不用跟著動。這個「輸入拼寫跟 key 脫鉤」的原則本身沒有問題，該保留。

    **如果哪天可以接受 breaking change，比較好的方向：把 book 的身份做成 gRPC 上結構化的型別，字串只留給人類看（log/CLI/錯誤訊息）**，而不是繼續在字串格式本身打轉（例如光是把 `book_key()` 輸出改成帶底線的 `BTC_USDT.SPOT`，只解決得了「能不能反解析」，解決不了「無效輸入在型別層級就不可表達」）：

    ```proto
    enum MarketType { MARKET_TYPE_UNSPECIFIED = 0; SPOT = 1; PERP = 2; }
    message BookId { string base = 1; string quote = 2; MarketType market = 3; }
    ```

    `SubscribeBboRequest`/`SubscribeL2DiffRequest` 都改成帶一個 `BookId book = 1`；`AggregatorService` 內部的 map 直接用 `struct BookId`（配 `operator<=>`/`std::hash`）當 key，不再是字串。三個訴求分別對應：
    - **不容易出錯**：打錯的 key 在型別層級就不可表達，不用等 server 回 `NOT_FOUND` 才發現——現在 `client_main.cpp` 吃的是 `argv` 字串，完全沒辦法在本地驗證。
    - **延展性**：以後加新市場類型（幣本位反向合約、到期期貨）只是加一個 enum value，`symbol.hpp` 裡每個 exhaustive switch（`native_symbol()`/`venue_id()` 那種、本來就靠 `-Wreturn-type` 逼你補 case 的寫法）會編譯失敗直到補上處理——這已經是這個檔案自己的既有慣例，字串後綴格式完全享受不到這層保護。要加新維度（結算幣別、venue-scoped book）也只是加 proto 欄位，向後相容。
    - **可維護性**：除了 CLI/設定檔這個輸入邊界之外，其餘地方完全不需要解析，沒有「round-trip 是否正確」這種要一直維護的性質。`to_string(BookId) -> "BTC_USDT.SPOT"` 之類的字串形式保留給 log/錯誤訊息/CLI 用，parse 只在輸入邊界做一次。

    會動到的地方：`symbol.hpp`（`book_key`、新的 `BookId`/parse）、`book_subscription.hpp`（`seen_book_keys` 改用 `BookId` 當 set key）、`proto/bobby/hermeneutic/aggregator/aggregator.proto`、`aggregator_service.hpp`（`book()`、`SymbolBook` map、`NOT_FOUND` 路徑）、`server_main.cpp` 的 registry 迴圈、`client_main.cpp`（argv 解析 + usage）、`README.md`、以及 `symbol_test.cpp`/`book_subscription_test.cpp`/`aggregator_service_test.cpp`（`UnknownSymbolFailsWithNotFound`、`MultiSymbolAggregatorServiceTest` 都要跟著改）。

    **實作完成，2026-09-19，`worktree-aggregator-client-bookkey-doc` 分支**：上面的提案大方向照做，`book_key()`/`parse_book_key()` 換成 `symbol::BookId`（`BaseQuote symbol; BookType type;`）+ `to_string(BookId)`/`parse_book_id(string_view)`，proto 加了 `MarketType` enum + `BookId` message，`SubscribeBboRequest`/`SubscribeL2DiffRequest` 的 `symbol` 欄位換成 `BookId book`。實作時跟提案有三處出入，都是往更簡單的方向收斂，不是推翻原設計：

    - **鍵值容器選 `std::unordered_map`/`std::unordered_set`，只實作 `std::hash<BookId>`，沒有加 `operator<=>`**。`AggregatorService::books_`、`book_subscription.hpp` 的 `seen_book_ids` 都只需要「當 hash 容器的 key」，用不到排序；`Asset`/`BaseQuote` 自己的註解本來就講「排序對貨幣代碼沒有意義,能省則省」，`BookId` 沒理由破例。`std::unordered_map` 是 node-based，`AggregatorService::book()` 回傳的 `SymbolBook*` 在 rehash 之後位址依然穩定，這點跟原本的 `std::map` 一樣，`server_main.cpp` 的 registry 建構順序不用重新檢查。
    - **`to_string(BookId)` 輸出帶底線的 `"BTC_USDT.SPOT"`,不是沿用舊 `book_key()` 的無底線 `"BTCUSDT.SPOT"`**。這是這次改動存在的理由本身——沿用無底線格式只會讓 `to_string`/`parse_book_id` 這組新函式看起來能反解析、實際上還是靠巧合，`parse_book_id` 因此是 `to_string` 真正精確的反函式（新增了對應的 round-trip 屬性測試,`symbol_test.cpp`），不是又一個「大部分情況能用」的猜測。
    - **`hermeneutic_aggregator_client` 的 CLI 維持原本的 variadic「一個 token 一本 book」形狀（`<book1> [book2 ...]`）,不是提案文字裡寫的 `<base> <quote> <spot|perp>` 三元組**——三元組沒辦法表達「同一次執行訂閱多本 book」這個既有能力。改成在 argv 邊界呼叫 `parse_book_id()` 解析每個 token（例如 `BTC_USDT.SPOT`）,再用 `fill_wire_book_id()`（新增的 `apps/aggregator/book_id.hpp`,proto BookId ↔ `symbol::BookId` 的薄轉接層,故意不讓 `symbol.hpp` 沾到 gRPC 依賴）填進 wire message——這正是設計裡「parse 只在輸入邊界做一次」原則的直接體現,只是邊界token 的形狀選了 `to_string()` 自己的輸出格式,而不是拆成三個獨立 argv。

    **實作時發現、提案沒設計到的一個新失敗模式**：proto3 對沒設定的欄位有隱式的零值（`market` 預設是 `MARKET_TYPE_UNSPECIFIED`,`base`/`quote` 預設是空字串）,所以「請求本身就是畸形的」（欄位沒填,或填了不合法的組合）跟「請求是合法的 BookId,只是不是這個 server 的book」是兩種不同的失敗,不該共用同一個 gRPC status code。`to_symbol_book_id()`（`book_id.hpp`）對前者回傳 `nullopt`,`SubscribeL2Diff`/`SubscribeBbo` 據此回 `INVALID_ARGUMENT`；後者（`AggregatorService::book()` 查無此 key）維持原本的 `NOT_FOUND`。`aggregator_service_test.cpp` 新增 `MalformedBookFailsWithInvalidArgument` 驗證前者,`UnknownSymbolFailsWithNotFound` 改成送一個合法但不存在的 `BookId{"DOGE","USDT",Spot}` 驗證後者維持原行為。

    會動到的地方跟提案時列的一致,額外多了 `apps/aggregator/book_id.hpp`（新檔案,proto↔C++ 轉接層）跟 `CMakeLists.txt`（`hermeneutic_aggregator_client`/`hermeneutic_aggregator_service_test` 都要多連 `hermeneutic::symbol`,`hermeneutic_aggregator_client` 也要多加 repo-root include dir 讓 `apps/aggregator/book_id.hpp` 這個路徑能解析）。

    **命名一致性修正,2026-09-19**：`symbol.hpp` 的 `enum class BookType` 改名成 `MarketType`,呼應上面 proto 自己的 `MarketType` enum（`BookId.market`）——這個對應在 `book_id.hpp` 裡本來就是 1:1 手動 switch,兩邊叫不同名字純粹是命名漂移,設計文件裡也找不到刻意分歧的理由（不像 `venue_id()` 的 venue-native 詞彙,那個有明確寫「故意不跟 BookId 統一」）。改動範圍：`symbol.hpp`（定義處+`venue_id()`/`native_symbol()`/`to_string()`/`parse_book_id()` 的參數與比較）、`book_id.hpp`（`to_symbol_book_id()`/`fill_wire_book_id()`,新增註解說明同名但不同 namespace 的兩個 `MarketType` 如何不會撞名）、`book_subscription.hpp`、`server_main.cpp`,以及 `symbol_test.cpp`/`book_subscription_test.cpp`/`venue_session_test.cpp`/`aggregator_service_test.cpp` 四個測試檔。純改名,不影響任何行為;`symbol.hpp` 仍然是零依賴,跟 proto 同名只是巧合式一致,不是耦合。

    **注意,上一段跟下面兩段完成後,本節開頭（第 479 行）那句「`native_symbol()`/`venue_id()` 都吃 `(Venue, BaseQuote, BookType)`」已經是歷史記錄,不是現況**——`venue_id()`、`BookType`、`Venue` 三個識別符都已經不存在了（依序被下面兩次改動移除/改名）。第 479 行保留不動,因為它精確記錄了當時（`book_key()`→`BookId` 提案時）的真實狀態；現況請看下面兩段。

    **`VenueId` 從字串換成結構化型別,2026-09-19,同一天**：`aggregate_order_book.hpp` 的 `VenueId` 原本是 `using VenueId = std::string`——`AggregateOrderBook`/`VenueSession`/`IngestionRunner` 拿它當 per-venue bookkeeping 的 map key,但沒有任何機制保證這個字串真的對應到某個 `(Venue, MarketType)` 組合,只有 `server_main.cpp` 的 `wire_venue()` 這一個呼叫端透過 `venue_id()` 正確產生它,其餘完全靠約定。跟 `book_key()`→`BookId` 同一種「身分應該在型別系統裡,不是字串慣例」的理由,改成 `symbol.hpp` 裡的 `struct VenueId { Venue venue; MarketType type; }`（純 equality/hash,不排序,跟 `BookId` 同理）+ `std::hash<VenueId>`；原本的 `venue_id(Venue, MarketType) -> std::string` 改造成 `to_string(const VenueId&)`,跟 `to_string(BookId)` 對稱,純顯示用。`aggregate_order_book.hpp` 的 `venues_` 也從 `std::map` 換成 `std::unordered_map`（要 hash 不要排序,跟 `BookId` 的容器選擇一致）。第一版留下一個範圍缺口——`book_subscription.hpp` 的 `VenueSubscription` 跟 `server_main.cpp` 的 `VenueGroups` 還各自用 `(Venue, MarketType)` 拆開存,是同一種「同一個 pairing 被獨立拼兩次」的問題,只是搬到上一層——由 advisor 複核抓到,已一併摺進同一個 commit（`VenueSubscription::venue_id` 改成單一 `VenueId` 欄位,`VenueGroups` 改成 `std::unordered_map<VenueId, VenueGroup>`）。Build+test 120/15/27/5/1（`hermeneutic_ingestion_runner_test` 這次也納入檢查,因為改到了 `ingestion_runner.hpp`）,單一 venue live 驗證 `bbo`/`volume-bands` 皆正常。Commit `9a193d2`,pushed 到 `worktree-fix-scattered-issues`。

    **`Venue` 改名成 `Exchange`,2026-09-19,同一天**：使用者發現 `enum class Venue { Binance, Bybit, Okx }` 自己的 comment 寫的是「Exchanges this project can source liquidity from」——這個 enum 一直以來指的其實是「哪家公司」,`Venue` 這個名字語意上是錯的；真正符合「venue」語意（一家交易所 scoped 到某個市場,例如 binance_spot 跟 binance_futures 是兩個不同 venue,即使同一家公司）的,是上一段剛結構化的 `VenueId`。改法：`enum class Venue` 改名 `Exchange`,`parse_venue()`/`venue_name()` 改名 `parse_exchange()`/`exchange_name()`,`native_symbol()`/`to_string(VenueId)` 的參數與 switch 跟著改;`VenueId` 這個型別名稱本身不變,但它的第一個欄位從 `Venue venue` 改成 `Exchange exchange`。`VenueId`/`VenueSession`/`venues_`/`VenueSubscription::venue_id`/`VenueGroups`/`wire_venue()`——所有已經正確代表「exchange+market」這個 venue 概念的識別符——保持不動;JSON 設定檔的 `"venues"` 欄位跟操作者可讀的錯誤訊息（"unknown venue"、"listed twice" 等)也保持不動,因為那些描述的是設定檔本身的詞彙,跟內部 C++ 型別命名是兩件事。改動範圍：`symbol.hpp`、`book_subscription.hpp`、`server_main.cpp`,以及 `symbol_test.cpp`/`book_subscription_test.cpp`/`ingestion_runner_test.cpp`/`venue_session_test.cpp`/`aggregate_order_book_test.cpp`/`aggregator_service_test.cpp` 六個測試檔。純改名,不影響任何行為,build+test 120/15/27/5/1 跟改名前完全一致,單一 venue live 驗證確認 `to_string(VenueId)` 仍正確印出 `"binance_spot"`。

## 11. 下一步

`SymbolSync<SequencePolicy>` + `BinanceFuturesSequencePolicy`/`BinanceSpotSequencePolicy`/`BybitSequencePolicy`/`OkxSequencePolicy`（測試涵蓋四種 policy，包含共用的 `kTrustsConnectionOrder` 機制）、`BinanceFuturesFeed`（11 測試）、`BinanceSpotFeed`（12 測試）、`BybitLinearFeed`（10 測試）、`BybitSpotFeed`（8 測試）、`OkxFeed`（17 測試，含 `okx_canonical()`/`is_swap()`）、`WebSocketConnection`（1 測試）、`http_get`（3 測試）、`VenueSession<Feed,Policy,NextLayer>` + `SymbolRegistry`（整合測試）都已完成並測試通過。**`aggregator_main.cpp` 已經接上 ingestion（第 10 節第 7 項），並且分別對 Binance Futures（`wss://fstream.binance.com`/`https://fapi.binance.com`）、Binance Spot（`wss://stream.binance.com:9443`/`https://api.binance.com`）、Bybit linear（`wss://stream.bybit.com/v5/public/linear`）、Bybit spot（`.../v5/public/spot`）各自單獨跑起來過，各自收到真實 BTCUSDT order book，Bybit 兩個 venue 也各自透過真正的 `SubscribeBbo` gRPC 路徑確認過能把資料送到訂閱者**（第 10 節第 6/7 項的 TLS/端對端驗證；六個 venue 同時掛著跑則還沒有另外驗證過，見第 6 節「即時驗證」小節最後一段的說明）。整條「真實交易所 WS/REST → resync → 套用進真實 book → 真實 gRPC 訂閱者收到正確結果」的路徑，Binance/Bybit 四個 venue 都不只是測試證明可以動，是真的連過真實交易所跑過一次——但 Bybit 這兩個 venue 第一輪的「驗證」其實是無效的（見第 6 節），因為當時 `SymbolSync` 有一個會讓 Bybit 安靜貢獻零筆資料的真實 bug（見第 5 節 `BybitSequencePolicy` 的修正記錄），第二輪單獨掛 Bybit venue 重跑才是真正證明有效的那次。**OKX 兩個 venue 也已經完成端對端驗證**：用跟 Binance Spot 那次同一招的臨時、用完即刪的 `service/live_okx_check_main.cpp`——真實 `AggregatorService`（book `"BTCUSDT.SPOT"`/`"BTCUSDT.PERP"`）+ 真實 gRPC server + 真實 `Aggregator::Stub` client 對兩個 symbol 各開一個 `SubscribeBbo` 串流 + 真實 `OkxFeed`/`OkxSequencePolicy` 的 `okx_spot`（`BTC-USDT`）/`okx_swap`（`BTC-USDT-SWAP`）兩個 `VenueSession` 連 `wss://ws.okx.com:8443/ws/v5/public`（2026-09-18）。跑 15 秒：兩個訂閱一開始都收到空書（`bid_present=0 ask_present=0`，`SubscribeBbo` 送出當下狀態的既有行為），snapshot 套用後很快兩邊都變成有值，之後持續收到真實更新——spot 55 筆 BBO、perp 119 筆（含收尾時 `stop_all()` 觸發 `invalidate_venue` 送出的最後一筆空書），全程無 crash、無錯誤 log，`kTrustsConnectionOrder` 的空 buffer 捷徑確實讓兩個 symbol 都直接從第一筆 snapshot 起步進 Live，不是理論上而已。

**用 AddressSanitizer 把 okx_spot、okx_swap 分開各自單獨跑過一次**（不是兩個一起跑）——刻意分開驗證，避免「其中一個 venue 有資料、另一個其實沒有」被另一邊的真實流量掩蓋成看起來兩邊都在動的假象：一份獨立的 `build-asan`（`-fsanitize=address -fno-omit-frame-pointer -g -O1`，跟平常的 `build-vcpkg` 分開，不影響正常建置），`hermeneutic_live_okx_check` 改成吃一個 `argv`（`spot`/`swap`）只接一個 venue。兩次都跑 15 秒：spot 單獨跑收到 48～77 筆真實 BBO（兩次結果不同純粹是市場當下活躍度不同），swap 單獨跑收到 122 筆，兩次 exit code 都是 0、沒有任何 ASan ERROR/SUMMARY。第一次有跳出一個 `container-overflow`（在 `AggregatorService` 建構子的 `std::map::try_emplace` 裡，`std::string` 複製建構時觸發）——加 `ASAN_OPTIONS=detect_container_overflow=0` 後乾淨通過，這是 libc++ container-annotation 已知的誤報類型（vcpkg 預先建置好的 `protobuf`/`grpc` 靜態庫沒有用 ASan 編譯，跟這次特別加了 ASan 的 `hermeneutic_proto`/`hermeneutic` 混在一起連結，兩邊對同一個 `std::string`/容器的 annotation 狀態對不上），不是這個專案自己程式碼的真實 bug——ASan 官方文件本身就提到這個限制。

**同一輪也另外改訂閱 `SubscribeL2Diff`（不是 `SubscribeBbo`）確認真正的 L2 深度更新筆數**，因為 BBO 只反映最佳買賣價變化，不能代表完整深度真的有在動：spot 兩次分別收到 110/135 筆 `L2Diff`（第一筆 `book_seq=1` 是把 OKX 400 檔 snapshot 灌進原本空的 aggregate book，本身就會產生一次 400 檔的 diff，之後才是正常的增量），swap 兩次分別收到 140/145 筆，同樣兩次 exit code 都是 0、沒有 ASan 發現。

剩下的收尾項目（第 10 節）：`SymbolBook::apply_batch`（1，已完成）、`IngestionRunner`/`IVenueSession` 的多交易所 type-erasure 邊界（2，**已完成並測試**——`stop()` 的 cancellation 傳播/孤兒 snapshot 排空/strand 三件事是核心難點，見第 2 項內文）、backoff 參數調校（3）、多執行緒 `io_context` 策略（4）、Binance Spot 的 `SequencePolicy`（5，**已完成**，見上——Bybit linear/spot、OKX spot/swap 則是各自獨立的擴充，見第 6/7/9 節）、Bybit/OKX 共通的 live-gap 恢復機制（8，尚未解決，見第 8/9 項）。

---

**Sources**：
- [Order Book (REST /fapi/v1/depth)](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#order-book)（Binance，2026-09-16 查證）
- [Diff. Book Depth Streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#diff-book-depth-streams)（Binance，2026-09-16 查證）
- [How to manage a local order book correctly (USDⓈ-M Futures)](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)（Binance，2026-09-16 查證）
- [How to manage a local order book correctly (Spot)](https://developers.binance.com/en/docs/products/spot/web-socket-streams#how-to-manage-a-local-order-book-correctly)（Binance，2026-09-18 查證）
- [Diff. Book Depth Streams (Spot)](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~#diff-book-depth)（Binance，2026-09-18 查證）
- [Order Book (REST /api/v3/depth, Spot)](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)（Binance，2026-09-18 查證）
- [Orderbook (WebSocket, v5 public)](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)（Bybit，2026-09-18 查證，涵蓋 linear 跟 spot 兩個市場；含 WebFetch 逐字擷取跟對兩個市場各自的即時 probe，見第 5 節 `BybitSequencePolicy`）
- OKX v5 `books` channel（docs.okx.com，2026-09-18）：**WebFetch 沒能抓到這頁的實際內容**——OKX 官方文件是 client-side render 的頁面，WebFetch 只拿到導覽列跟 WS endpoint 之類的靜態外殼，抓不到 channel 本身的 JSON schema。`seqId`/`prevSeqId` 的鏈式規則（含 idle-heartbeat 跟 sequence-reset 兩個例外）是使用者直接貼給這個專案的官方文件原文，不是训练資料裡的既有印象；channel 選擇（`books`）、`kTrustsConnectionOrder` 是否需要額外的 keepalive 這兩件事則是靠即時連線驗證確認，見第 9 節。跟 Binance/Bybit 不同，這是這個專案第一次在文件本身沒辦法逐字核對的情況下，靠「使用者提供的原文 + 即時 probe」這個組合完成驗證，記在這裡避免被誤會成又是訓練資料猜測。
