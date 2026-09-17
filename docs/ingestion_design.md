# 行情 Ingestion 設計文件

狀態：`SymbolBook`、`SymbolSync<SequencePolicy>`（含 `BinanceFuturesSequencePolicy`）、`BinanceFuturesFeed` 已實作並測試過（`include/bobby/hermeneutic/symbol_sync.hpp`、`service/binance_futures_feed.hpp`、對應測試共 19 個，全部 0ms）。下一步是 `VenueSession`。

本文件目的：把設計討論過程中反覆修正、目前只存在對話 scrollback 裡的決策跟理由固定下來，避免之後被 context 摘要掉、或被下一個 session 遺忘。**特別保留「曾經想錯、後來怎麼修正」的部分**，不只是最終乾淨版本——因為那些修正本身就是之後容易重蹈覆轍的地方。

## 1. 目標與範圍

從多個交易所（目前只做 Binance USDⓈ-M Futures）訂閱多個 symbol 的 L2 order book diff，維護正確的本地 order book 狀態，餵進已經存在的 `SymbolBook`（`service/aggregator_service.hpp`，`AggregatorService::book(symbol)`）。目標是非同步、多執行緒（Boost.Asio + Beast），且核心邏輯採 **sans-io** 設計：resync/序號校驗這類最容易出錯的邏輯完全不碰 socket，可以純用假資料序列做單元測試。

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
    static bool should_drop_buffered(const DepthUpdate&, const SnapshotMessage&);
    static bool bridges_snapshot(const DepthUpdate&, const SnapshotMessage&);
    static bool is_contiguous(const DepthUpdate&, std::uint64_t last_applied_final_id);
};
```

### `BinanceFuturesSequencePolicy`（USDⓈ-M Futures，目前唯一要做的）

依據官方文件逐字引用的步驟（見文末 Sources）：

```cpp
struct BinanceFuturesSequencePolicy {
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

### WS 自動推 snapshot 的交易所（例如某些非 Binance 交易所：連上就自動推第一筆 snapshot，之後全是 diff）

不需要另一套骨架，`SequencePolicy` 可以是近乎 trivial 的版本：

```cpp
struct TrustConnectionOrderPolicy {
    static bool should_drop_buffered(const DepthUpdate&, const SnapshotMessage&) { return false; }
    static bool bridges_snapshot(const DepthUpdate&, const SnapshotMessage&) { return true; }  // 傳輸層保證順序
    static bool is_contiguous(const DepthUpdate& e, std::uint64_t last) {
        return true;  // 或如果交易所有自己的序號欄位，比對那個
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

## 7. `VenueSession<Feed, Policy>`（I/O driver，orchestrator，只寫一次）

**命名修正**：一開始叫 `VenueConnection`，容易誤會成「它本身就是一條 transport 連線」。改名 `VenueSession`，跟底下真正的 transport 層分開：

- **`WebSocketConnection`**（transport 層，venue-agnostic，薄）：一條 WS 連線的生命週期本身（connect/send/read/偵測斷線），包 `beast::websocket::stream` + 專屬 `asio::strand`。完全不知道 symbol/resync/交易所是什麼。**每次重連 = 新的一個 instance**，`VenueSession` 自己不會斷。
- **HTTP snapshot**：不需要獨立的「HttpConnection」物件，寫成一次性函式 `http_get(io_context&, HttpRequestSpec) -> awaitable<expected<string, errc>>` 就夠，因為 REST snapshot 只是偶發的一次性 GET，不是常駐連線。
- **`VenueSession<Feed, Policy>`**：真正的 orchestrator，生命週期橫跨很多次 `WebSocketConnection`。擁有這條 session 負責的所有 symbol 的 `SymbolSync<Policy>`（`map<SymbolId, SymbolSync<Policy>>`），驅動迴圈：

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
            RequestSnapshot -> 若 Feed::kSnapshotViaRest 則 co_await http_get(feed.snapshot_request(symbol))，
                                結果丟進 on_snapshot()；否則 no-op
            ApplySnapshot/ApplyDelta/InvalidateVenue -> 呼叫 SymbolRegistry 查到的 SymbolBook
    // ws 掛了/丟例外，跳出內層迴圈
    for each symbol_sync: symbol_sync.on_disconnected()   // 這條連線負責的每個 symbol 各自 invalidate
    // 回到 loop 開頭
```

**`VenueSession`本體只寫一次、venue-agnostic**：per-exchange 差異全部是「模板參數的具體型別」，不是「orchestration 的實作」——`VenueSession<BinanceFuturesFeed, BinanceFuturesSequencePolicy>` 跟未來的 `VenueSession<OkxFeed, OkxSequencePolicy>` 共用同一份類別程式碼。這跟本專案既有風格一致（`BasicFixedPoint<Decimals>`、`aggregator_service.hpp` 裡的 `capture_snapshot_before<Compare>`，都是用 template 參數化差異點，不是繼承/virtual）。

真正需要 runtime polymorphism（type erasure）的地方只有最上層——如果之後要有一個 `IngestionRunner` 同時管理多種不同具體型別的 `VenueSession`（Binance 的、OKX 的……），那一層可以留一個很薄的非模板介面（例如 `IVenueConnection { start(); stop(); }`），把 type erasure 限制在生命週期管理這個邊界，`SymbolSync`/`VenueSession` 本體的邏輯完全不用付虛擬呼叫成本。**這層尚未設計，見第 10 節。**

## 8. `SymbolRegistry`

`std::unordered_map<SymbolId, SymbolBook*>`（或直接用 `AggregatorService::book(symbol)`），啟動時從 `AggregatorService` 建好，不動態增減。沒有特別的設計難度，純查表。

## 9. 其他修正記錄摘要

- **測試哲學延續**：`SymbolSync`/`VenueFeed` 是 sans-io、可以完全用假資料序列單元測試，不需要真的 socket——這是選擇整個分層方式（sans-io 核心 vs. 薄 I/O driver）的主要動機，也是使用者明確表達的偏好（「比較好 unittest」）。
- 資料來源全部經過 WebFetch 直接查證官方文件，不是純憑訓練記憶：
  - REST snapshot：`GET /fapi/v1/depth`（回傳 `lastUpdateId`/`E`/`T`/`bids`/`asks`）
  - WS diff stream：`{symbol}@depth@{updateSpeed}`，payload 含 `e`/`E`/`T`/`s`/`U`/`u`/`pu`/`b`/`a`/`ps`/`st`
  - 官方維護本地 order book 的 9 步驟程序（見 Sources）

## 10. 尚未定案 / 待做事項

1. **`SymbolBook::apply_batch`**：一則 `DepthUpdate`（一批 level 變化）目前會變成多次 `apply_delta` 呼叫、多次 seq bump，要不要比照 `apply_snapshot` 做成一次 seq bump，尚未決定。
2. **`IngestionRunner`/`IVenueConnection` 的 type-erasure 邊界**：多交易所、多 `VenueSession` 具體型別的統一生命週期管理層，尚未設計。
3. **backoff/jitter 的實際參數**：形狀已定（per-connection，帶 jitter），數值未定。
4. **多執行緒 `io_context` thread pool 的大小**：先前討論過大方向（parsing 平行、apply 序列化在各自 `SymbolBook` 的 mutex 上），實際執行緒數量策略未定。
5. **Binance Spot 的 `SequencePolicy`**：使用者明確表示目前不需要，之後才做。
6. **REST client 實作**：預期用 Beast 的 HTTP client，尚未寫。

## 11. 下一步

`SymbolSync<SequencePolicy>` + `BinanceFuturesSequencePolicy`（9 個測試）、`BinanceFuturesFeed`（10 個測試，含用官方文件逐字範例當 fixture）都已完成，共 19 個測試全部 0ms、零真實 I/O。下一步是 `VenueSession<Feed, Policy>`（真正接 Beast WebSocket + HTTP client 的 I/O driver），會需要引入 Boost（Beast/Asio），屆時再決定要 vcpkg 還是 FetchContent。

---

**Sources**（Binance 官方文件，2026-09-16 查證）：
- [Order Book (REST /fapi/v1/depth)](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#order-book)
- [Diff. Book Depth Streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#diff-book-depth-streams)
- [How to manage a local order book correctly (USDⓈ-M Futures)](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
