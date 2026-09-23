# API / Protocol Design and Extensibility

This document covers `hermeneutic_aggregator_service`'s external gRPC API
(`proto/bobby/hermeneutic/aggregator/aggregator.proto`), the semantic
contracts behind that wire protocol, and the three main extension points:
adding a venue, adding a market type, and adding RPCs/fields. Most of the
design rationale already lives as doc comments on the `.proto`/headers
themselves; this document's job is to string those scattered decisions
into a single read-through view and record *why* things are this way, not
just what the spec says. It's scoped to the API shape exposed *after*
`SymbolBook` - and where the system deliberately leaves room for future
extension - not how data flows from an exchange into `SymbolBook` in the
first place; that ingestion-layer design history (the sans-io
`SymbolSync`/`SequencePolicy` state machine, per-exchange sequencing
corrections, live-verification records) used to live in
`docs/ingestion_design.md`, since superseded and removed - its full text
remains recoverable from git history at commit `b18e16a`'s parent.

## 1. Service overview

The `Aggregator` service (`proto/bobby/hermeneutic/aggregator/aggregator.proto`)
has exactly three RPCs:

```protobuf
service Aggregator {
  rpc SubscribeL2Diff(SubscribeL2DiffRequest) returns (stream L2Update);
  rpc SubscribeBbo(SubscribeBboRequest) returns (stream BboUpdate);
  rpc ListBooks(ListBooksRequest) returns (ListBooksResponse);
}
```

- **`SubscribeL2Diff`**: server-streaming. Sends one complete `L2Snapshot`
  first, then an `L2Diff` for every subsequent change to the aggregate
  book, until the client cancels. The only source that lets a client
  reconstruct the full order book.
- **`SubscribeBbo`**: an independent server-streaming RPC that only sends
  the best bid/ask. It observes the same underlying aggregate book as
  `SubscribeL2Diff`, but the two streams' lifecycles and backpressure are
  completely independent - see section 4. A client that only cares about
  the top of book never has to parse `L2Diff` at all.
- **`ListBooks`**: unary. Returns the fixed set of books this server
  instance was started with. That set never changes for the lifetime of
  the process (see `AggregatorService`'s constructor), and it's the same
  set `SubscribeL2Diff`/`SubscribeBbo` validate requests against.

All of this is implemented in `apps/aggregator/aggregator_service.hpp`
(the sole `grpc::Service` in the project): one `AggregatorService`
instance serves multiple books from one `unordered_map<BookId, SymbolBook>`.
This is deliberate: two `grpc::Service` instances (even of the same
generated type) can't be registered on the same `grpc::Server`, so "one
process/port per symbol" is ruled out in favor of one service that routes
internally.

## 2. `BookId`: a structured address, not a string key

```protobuf
enum MarketType {
  MARKET_TYPE_UNSPECIFIED = 0;
  SPOT = 1;
  PERP = 2;
}

message BookId {
  string base = 1;
  string quote = 2;
  MarketType market = 3;
}
```

A book's identity is the three fields `{base, quote, market}`, not a
concatenated string like `"BTC_USDT.SPOT"` that has to be re-parsed back
into base/quote at every boundary. Reasoning: splitting a concatenated
ticker (e.g. `"BTCUSDT"`) back into base/quote is fundamentally ambiguous
without maintaining a quote-asset dictionary (see `symbol.hpp`'s
`split_base_quote()`/`native_symbol()` comments for the full argument). So
this boundary is never left for either side to guess at - `BookId` is the
same structured type on the wire, in `AggregatorService`'s map key, and in
the client's CLI parsing.

The corresponding C++ type is `bobby::hermeneutic::symbol::BookId`
(`symbol.hpp`), converted to/from the wire type through the thin
conversion layer in `apps/aggregator/book_id.hpp`
(`to_symbol_book_id()`/`fill_wire_book_id()`) - `symbol.hpp` itself has no
dependency on proto/gRPC at all; that conversion only happens at the two
places that actually need to cross the boundary (the server side in
`aggregator_service.hpp`, the client side in `client_main.cpp`).

### 2.1 Validation split: `INVALID_ARGUMENT` vs `NOT_FOUND`

`MARKET_TYPE_UNSPECIFIED` is proto3's implicit zero value for an unset
field - never a valid book - so request validation happens in two layers:

- **Malformed** (`market` unset, or `base`/`quote` empty) → `to_symbol_book_id()`
  returns `nullopt` → the RPC returns `INVALID_ARGUMENT`. This kind of
  request never even reaches the book lookup.
- **Well-formed but not a book this server has** → `AggregatorService::book()`
  returns `nullptr` → the RPC returns `NOT_FOUND`.

Splitting the two ensures a typo'd book is always a loud failure, never a
silently empty stream - whether the mistake is in a field or in the book
name itself, the client can immediately tell "my request itself is broken"
apart from "this server doesn't serve that book".

### 2.2 Handling an open-ended enum

proto3 enums are open on the wire: a newer client can send a `MarketType`
value this server's own generated code doesn't even have a name for.
`to_symbol_book_id()` (`book_id.hpp`) therefore uses a `default:` branch to
treat "any value with no matching case" as `nullopt` - the one place in
this project where the switch deliberately has a `default:` branch
instead of being written exhaustively, because here the whole point is
that "a value the switch doesn't recognize" is itself valid input, not a
bug. The reverse direction,
`fill_wire_book_id()` (C++ enum → wire enum), stays an exhaustive switch: a
C++ enum is a closed set, so a new `symbol::MarketType` value not covered
there should be a compile warning, not a silently wrong value sent over
the wire.

## 3. Wire messages and semantic contracts

### 3.1 Fixed-point encoding

```protobuf
// price = price_raw / 1e9
// size  = size_raw  / 1e6
message PriceLevel {
  int64 price_raw = 1;
  int64 size_raw = 2;
}
```

This scale is fixed by the protocol itself (`static_assert(Price::decimals == 9)`,
`static_assert(Size::decimals == 6)` in `aggregator_service.hpp`), not a
per-subscription negotiable parameter - in other words, if the number of
decimal digits `Price`/`Size` (both `BasicFixedPoint`) use ever changes,
this wire contract changes with it, rather than the wire carrying its own
scale field. Integers, not floats, are used on the wire specifically to
avoid floating-point error in prices/sizes over the network.

- **`L2Snapshot`/`L2Diff` levels**: a level is a *replacement*, not a
  delta - `size_raw > 0` is the new absolute size at that price, `size_raw == 0`
  removes the level. This matches the semantics of many exchanges' own
  wire formats (e.g. Binance's depth diff), so a client never has to do
  its own addition/subtraction.
- **`Bbo`**: always the complete current best bid/ask state, never a
  delta, and never dependent on a previously received `Bbo` to
  reconstruct - unlike `L2Diff`, deliberately: a BBO subscriber never has
  to maintain a local order book.

### 3.2 `book_seq`: different contiguity contracts on the two streams

`book_seq` is the aggregate book's own revision number, not "how many
messages this stream has sent" - this is why one counter,
`SymbolBook::seq_`, feeds both `SubscribeL2Diff` and `SubscribeBbo` (see
`aggregator_service.hpp`'s `SymbolBook` class comment).

- **On `SubscribeL2Diff`**: `book_seq` is guaranteed contiguous. Every
  aggregate-book revision produces exactly one `L2Diff`, so the next
  observed update is guaranteed to be `book_seq = N+1`. **A client that
  sees a gap must discard its local order book and resubscribe for a
  fresh snapshot** - this is the one thing this protocol requires of a
  client. `apps/aggregator/client_book.hpp`'s `is_book_seq_gap()` is the
  reference implementation of that check; `client_main.cpp`'s
  `volume-bands`/`price-bands` modes print a `GAP` line and stop that
  stream outright on a detected gap (they don't auto-resubscribe - see
  section 5).
- **On `SubscribeBbo`**: `book_seq` is monotonically increasing but
  **not** guaranteed contiguous. Most aggregate-book revisions only touch
  levels away from the top of book and never produce a `Bbo` message at
  all, so a gap in `Bbo.book_seq` relative to `L2Diff.book_seq` is normal,
  not a sign of a missed message - every `Bbo` actually delivered is, by
  construction, the complete and correct state as of the `book_seq` it
  carries. This field isn't there for BBO-side gap detection; it's there
  so a client subscribed to both streams (or someone comparing recorded
  data after the fact) can correlate a `Bbo` against the matching
  `L2Diff` revision, even though the two gRPC streams have no guaranteed
  arrival order relative to each other.

One line to remember: **a gap on `SubscribeL2Diff` is an error; a gap on
`SubscribeBbo` is normal.** This is the single easiest thing for a new
client implementer to get backwards - worth rereading `Bbo.book_seq`'s
full comment in `aggregator.proto` before writing any new client.

### 3.3 `ts_ns`: observability only, never for ordering

`L2Snapshot`/`L2Diff`/`Bbo`/`Heartbeat` all carry a `fixed64 ts_ns` - the
aggregator's own wall-clock time, in Unix epoch nanoseconds, at the moment
this message was built. It is **not** an exchange timestamp, not an
upstream-receive timestamp, and not a book-mutation timestamp. It exists
purely for things like latency measurement, and **must never be used for
ordering** - wall-clock time is not monotonic (NTP sync, manual clock
changes can make it jump backward); `book_seq` is what actually determines
order and detects missed messages. `fixed64` rather than `int64`/`uint64`
varint encoding is used because a current epoch-nanos value is already
large enough that varint encoding needs 9 bytes, one more than fixed64's
constant 8 - the same choice OpenTelemetry makes for its own
`*_unix_nano` fields.

### 3.4 `Heartbeat`: a liveness signal independent of book revisions

```protobuf
message Heartbeat {
  fixed64 ts_ns = 1;
  repeated string live_venues = 2;
}
```

Sent on a fixed interval (currently once a second, `server_main.cpp`'s
`kHeartbeatInterval`), independent of any book change, and it **never**
advances `book_seq` - a heartbeat is not a book revision. Reasoning: without
this signal, a client has no way to tell "the book is genuinely unchanged"
apart from "the aggregator/feed is stalled" during a quiet period.
`L2Update`/`BboUpdate` each have their own `heartbeat` field as an
independent liveness signal per stream - a heartbeat on `SubscribeL2Diff`
says nothing about `SubscribeBbo`'s liveness or vice versa, even though
both reuse the same `Heartbeat` message type.

`live_venues` is the set of venues currently contributing real data
(venue-native display strings, e.g. `"binance_spot"`); an empty set is the
strongest signal a client has that this book is currently not to be
trusted - regardless of whether the cause is a disconnect, a resync gap,
or the book never having been seeded at all, this field deliberately
doesn't distinguish the cause, since the corrective action (don't trust
this book) is the same either way. Known limitations (see the full
comment on `live_venues` in `aggregator.proto`):

- proto3 gives `repeated` fields no wire-level distinction between "never
  set" and "explicitly empty" - a build of `AggregatorService` predating
  the `live_venues` concept looks identical, on the wire, to a build that
  genuinely has zero live venues. Mixed-version rolling deploys are
  outside what this signal currently covers.
- Element order is not guaranteed stable (the server populates it by
  iterating an `unordered_map`) - a client comparing successive heartbeats
  should treat this as a set, not a sequence.
- A venue appearing in `live_venues` only means the WebSocket idle-ping
  mechanism got *some* response - if that venue sits behind a load
  balancer/reverse proxy that answers protocol-level pings independently
  of its own backend's health, this field can report a venue as live
  indefinitely even though real market data has stopped arriving. This is
  a known, currently-undetectable-from-this-field-alone blind spot (see
  `connect()` in `include/bobby/hermeneutic/net/websocket_connection.hpp`
  for the full reasoning).

## 4. Subscription lifecycle and backpressure

Each subscriber gets its own `SubscriberQueue<T>` (`aggregator_service.hpp`);
a publisher only ever pushes, and that stream's own handler thread is the
sole thing that drains it and calls `Write()`. A slow subscriber can only
overflow its own queue - it never slows down broadcasting to other
subscribers, or ingestion itself.

The two streams choose different `OverflowPolicy` values, directly tied
to the semantic difference from sections 3.1/3.2:

| Stream | Policy | Why |
|---|---|---|
| `SubscribeL2Diff` | `Close` | Missing even one diff leaves this subscriber's local order book genuinely wrong, not just stale - the only valid recovery is a fresh `SubscribeL2Diff()` (a new snapshot). So overflow clears the queue and closes it immediately, rather than keeping stale, no-longer-trustworthy data. The RPC returns `RESOURCE_EXHAUSTED`, telling the client explicitly to reconnect for a fresh snapshot. |
| `SubscribeBbo` | `DropOldest` | `Bbo` is the complete current state, not a delta - a slow subscriber loses nothing by having a stale queued `Bbo` replaced by a newer one. Overflow drops the oldest entry and keeps accepting pushes, without disconnecting. |

Both policies apply at a higher, but not unlimited, capacity during the
bootstrap window between a queue's registration and that subscriber's own
first successful `Write()` (`kBootstrapCapacityMultiplier = 4`) - a newly
subscribed queue is registered for broadcast before its own drain loop has
even started, so an ordinary burst landing in that window shouldn't trip
the same tight threshold a genuinely slow, already-draining subscriber
would. There's still a real ceiling, though: a connection that never
reads must not be able to drive unbounded memory growth.

`subscribe_impl()` (`aggregator_service.hpp`) has a key ordering: it
captures the initial snapshot/BBO state and registers the queue under the
mutex first, then releases the lock **before** calling the (blocking)
`writer->Write()` - `Write()` must never be called while the mutex is
held, or a new subscriber's slow connection would stall both ingestion and
every other subscriber's broadcast.

## 5. Client-side contract summary

Any new client must respect these rules (the existing
`hermeneutic_aggregator_client` - `apps/aggregator/client_main.cpp` - is a
reference implementation, not the only valid one):

1. **A gap in `SubscribeL2Diff`'s `book_seq` invalidates the local order
   book**, and requires a fresh subscription for a new snapshot before
   accumulating correctly again (see section 3.2, `client_book.hpp`'s
   `is_book_seq_gap()`). The existing client chooses to print a `GAP` line
   and stop that stream on a detected gap, leaving "whether to
   auto-resubscribe" up to the operator - that's not something the
   protocol requires, just this diagnostic client's own choice.
2. **Every level is a replacement, not a delta** (`size_raw == 0` means
   delete - see section 3.1); `apply_levels()` (`client_book.hpp`) is the
   reference implementation of that rule.
3. **`ts_ns` must never be used for ordering or freshness**, only for
   latency measurement (see section 3.3).
4. **An empty `live_venues` set means this book is currently untrustworthy**,
   regardless of the underlying cause (see section 3.4).
5. **A gap in `SubscribeBbo`'s `book_seq` is normal**, not an error to
   handle (the exact opposite of point 1 - easy to confuse, see section 3.2).
6. The client's handling of malformed data is deliberately lenient:
   `apply_levels()` doesn't validate `price_raw`/`size_raw` (unlike the
   server side's `AggregateOrderBook::apply_batch()`, which does via
   `is_valid_level()`); a bad value stays in the local book, and
   downstream computations like `price_band_depth()`/`volume_band_prices()`
   print `"ERROR"` on the next `print_bands()` call rather than silently
   computing a wrong result - a known, scoped-in response for a
   diagnostic client, not an oversight.

## 6. Extensibility: adding an exchange (venue)

Adding a new venue requires three orthogonal, independently unit-testable
pieces:

### 6.1 `VenueFeed`: pure parse/encode, zero I/O

A duck-typed interface (no virtual inheritance, resolved through template
instantiation - see `ingestion_runner.hpp`'s comment on why the
per-message hot path never crosses this boundary), which every
`*_feed.hpp` under an `exchange/` directory must provide (see
`BinanceFuturesFeed` in
`include/bobby/hermeneutic/exchange/binance/binance_futures_feed.hpp` as a
template):

```cpp
class SomeVenueFeed {
  public:
    static constexpr bool kSnapshotViaRest = /* true: snapshot fetched via REST; false: pushed over WS */;

    std::string_view ws_host() const;
    std::string_view ws_port() const;
    std::string_view ws_target() const;

    std::string subscribe_message(std::span<const NativeSymbol> symbols) const;

    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const;

    // Only needed when kSnapshotViaRest == true:
    HttpRequestSpec snapshot_request(const NativeSymbol& symbol) const;
    std::expected<SnapshotMessage, std::errc> parse_snapshot_response(
        NativeSymbol symbol, std::string_view body) const;
};
```

- `parse_message()` returns `ParsedMessage`
  (`std::optional<std::variant<SnapshotMessage, DepthUpdate>>`, defined
  once in `exchange/feed_wire.hpp` and shared by every venue to avoid an
  ODR conflict from multiple Feed headers being included together) -
  `nullopt` means "this message was recognized but is irrelevant to book
  state" (e.g. a subscribe ack, a heartbeat), not an error;
  `std::unexpected(std::errc::bad_message)` is a genuine parse failure.
- `DepthUpdate` is a **batch** of bid/ask changes sharing one sequencing
  envelope (`first_id`/`final_id`/`prev_final_id`), not one level per
  message - this deliberately mirrors each exchange's own wire shape. An
  earlier version of this type modeled one `(side, price, size)` triple
  per `DepthUpdate` instead, which was wrong: an exchange's own depth-diff
  message is itself a batch of bid/ask changes sharing one `U`/`u` pair,
  not one level per message, so that design would have made a gap check
  look like it applied per level instead of per message. `SnapshotMessage` is the same batch shape
  for bids/asks but carries its own, differently-named sequencing anchor
  instead: a single `last_update_id`, not `first_id`/`final_id`/
  `prev_final_id` - a snapshot has nothing to bridge *from*, only a single
  value `SequencePolicy::bridges_snapshot()` checks a buffered
  `DepthUpdate` against.
- `HttpRequestSpec` (`exchange/feed_wire.hpp`) is pure data, no I/O of its
  own - actually issuing the request is `VenueSession::fetch()`'s job
  (`ingestion/venue_session.hpp`). This type is deliberately declared in
  `exchange/feed_wire.hpp` rather than `net/http_client.hpp`, because the
  latter needs Boost.Asio/Beast/OpenSSL (`HERMENEUTIC_BUILD_SERVICE`),
  while any Feed header (including this type's declaration) only needs
  simdjson (`HERMENEUTIC_BUILD_INGESTION`, no vcpkg required).

`detail::parse_decimal_string_to_fixed<>()`/`detail::parse_level()`/
`detail::parse_levels()` (also in `feed_wire.hpp`) are shared
string-to-fixed-point utilities - exchanges commonly send price/quantity
as JSON strings (not numbers) to avoid floating-point ambiguity; a new
Feed should reuse these rather than reimplementing them.

### 6.2 `SequencePolicy`: the one venue-specific part of the resync algorithm

`SymbolSync<SequencePolicy>` (`book/symbol_sync.hpp`) is the sans-io
resync state machine, never touching a socket or a timer, driven by
`SequencePolicy` through three static predicate functions plus one
compile-time flag:

```cpp
struct SomeSequencePolicy {
    static bool should_drop_buffered(const DepthUpdate&, const SnapshotMessage&);
    static bool bridges_snapshot(const DepthUpdate&, const SnapshotMessage&);
    static bool is_contiguous(const DepthUpdate&, std::uint64_t last_applied_final_id);
    static constexpr bool kTrustsConnectionOrder = /* true: the WS connection guarantees the snapshot arrives first (Feed's kSnapshotViaRest == false); false: the snapshot comes via a separate REST call raced against the already-flowing diff stream */;
};
```

See `exchange/binance/binance_futures_sequence_policy.hpp`
(`BinanceFuturesSequencePolicy`) for a reference implementation, and read
the other exchanges' own `*_sequence_policy.hpp` headers (Binance Spot,
Bybit, OKX) for the pitfalls each one's sequencing rules hit in practice
(Binance Futures' `pu` field, Spot having no `pu` equivalent, Bybit's/OKX's
own reset/contiguity conventions) before implementing a new venue - don't
rely on the interface signatures alone.

### 6.3 Wiring into `server_main.cpp`: `wire_venue<Feed, Policy>()`

`server_main.cpp` uses a template function `wire_venue<Feed, Policy>()` to
separate "did the config actually ask for this venue" from "how to wire
it into `IngestionRunner`" - if the config doesn't mention a given venue,
`find_group()` finds no matching `VenueGroup`, and that particular
`wire_venue<>()` call is a no-op (the Feed/Policy template is still
instantiated and linked into the binary; it just never registers that
venue with `IngestionRunner::add()`). The full checklist for adding a new
venue:

1. Add a value to `symbol.hpp`'s `Exchange` enum, and fill in the new case
   in `exchange_name()`/`venue_naming()`/`native_symbol()` - these three
   are deliberately written as true exhaustive switches (no `default:`),
   so a missed case there is a compile warning, not a silent
   misclassification. `parse_exchange()` is different: it's an if/else
   chain over string tokens (`"BINANCE"`, `"BYBIT"`, `"OKX"`, ...) with an
   implicit `return std::nullopt;` fallthrough, not a switch over
   `Exchange` at all, so forgetting to add the new venue's token there
   gets **no** compiler diagnostic - it just fails to parse silently at
   config-load time (a missing venue in `parse_exchange()` surfaces as
   `ingestion/book_subscription.hpp` rejecting the config with "unknown
   venue", not as a build warning). `ingestion/book_subscription.hpp`'s
   JSON `"venues"` parsing calls this same `parse_exchange()` directly,
   so there's no separate mapping table to maintain - just no
   compile-time safety net for this one function either.
2. Write a new `exchange/<venue>/<venue>_feed.hpp` (section 6.1) and
   `<venue>_sequence_policy.hpp` (section 6.2), along with unit tests
   under `tests/exchange/<venue>/` (`SymbolSync`/`VenueFeed` are pure
   functions - no real connection needed to test them).
3. Add one line to `server_main.cpp`'s `main()`:
   `wire_venue<NewFeed, NewPolicy>(runner, groups, VenueId{Exchange::NewVenue, MarketType::Spot or Perp}, ...)`,
   alongside the existing Binance/Bybit/OKX calls.
4. `server_main.cpp` has a safety net: if the config names a `(venue, market)`
   pair no `wire_venue<>()` call claims (present in `groups` but absent
   from `wired_venue_ids`), startup fails immediately via
   `fail_on_bad_venues()` rather than starting up quietly with that book
   permanently starved of data - forgetting step 3 gets caught at startup,
   not discovered later as a silent gap.

A new venue **never needs to touch** `aggregator.proto`, `AggregatorService`,
or any client-side code - `hermeneutic::ingestion`
(`VenueSession`/`IngestionRunner`) never mentions `aggregator::SymbolBook`
or protobuf at all; it's templated on any `Book` type that exposes
`apply_snapshot`/`apply_batch`/`invalidate_venue` (see the README's
"ingestion and the gRPC surface are two independently reusable layers"
technical decision). This is one of the system's biggest extension
boundaries: adding an exchange is purely an ingestion-layer change and
never touches the wire protocol.

## 7. Extensibility: adding a market type

`symbol::MarketType` (currently just `Spot`/`Perp`) is deliberately
designed with no `Unspecified`/`Invalid` member - "unknown/malformed" is
always represented by the **absence** of a `MarketType`
(`std::optional`/`std::expected` returning `nullopt`), never an extra
enumerator inside the type itself. Adding a market type (e.g. options, in
the future) touches these places, but they don't all give you the same
safety net:

- `symbol.hpp`: `to_string(BookId)`, `to_string(VenueId)`, `native_symbol()`,
  and `book_id.hpp`'s `fill_wire_book_id()` are true exhaustive switches
  (no `default:`) - a missed case here is a compile warning.
- `book_id.hpp`'s `to_symbol_book_id()` and
  `ingestion/book_subscription.hpp`'s `parse_market_type()` are the
  opposite case: `to_symbol_book_id()` has an explicit `default:` branch
  that treats any unhandled value as `nullopt` (see section 2.2 - this is
  deliberate, since it must also degrade safely for a wire enum value
  this server's codegen doesn't know about at all), and
  `parse_market_type()` is a plain if/else over `"SPOT"`/`"PERP"` with a
  trailing `return std::nullopt;`, not a switch at all. Forgetting to add
  the new type to either one gets **no** compile warning - it silently
  falls back to `INVALID_ARGUMENT` (wire) or a config-parse rejection
  (JSON), which only shows up as a runtime symptom, not a build failure.
- `aggregator.proto`: a new `MarketType` enum value - since proto3 enums
  are open on the wire, an old server receiving the new value falls into
  `to_symbol_book_id()`'s `default:` branch above and returns
  `INVALID_ARGUMENT` - a safe degradation, not a crash.

In short: when following this checklist, don't assume every one of these
call sites will warn you if you miss it - only the first group will.

`aggregator_service.hpp` (`SymbolBook`/`AggregatorService`) and
`book/aggregate_order_book.hpp` themselves never branch on the value of
`MarketType` - `AggregateOrderBook` does key its per-venue state by
`VenueId` (which embeds `MarketType`, as `unordered_map<VenueId, L2OrderBook> venues_`),
but it only ever treats that as an opaque equality/hash key, never
switching on it or caring what the `MarketType` inside actually is. This
means the blast radius of adding a market type is entirely confined to
"how a book is named/parsed" (the switches listed above) - it never
touches subscription lifecycle, fan-out, or backpressure.

## 8. Extensibility: adding an RPC or a field

### 8.1 Adding fields

- **New message fields**: proto3 is forward-compatible - an old client
  ignores a field it doesn't recognize, and a new client talking to an
  old server just sees that field's default value. `Heartbeat.live_venues`
  is a live example (see section 3.4's "known limitations"): adding a
  `repeated` field to an already-running protocol costs you the ability
  to distinguish, on the wire, "the other side's build is too old to
  have populated this" from "this state is genuinely empty". Before
  adding a field, think through whether its "unset" state could be
  misread as a meaningful value - if so, it may need its own presence
  flag rather than relying on the field's own zero value.
- **New enum values**: proto3 enums are open on the wire; what old code
  does with a value it doesn't recognize depends entirely on whether the
  C++ side's switch is written to expect that - a `default:` branch that
  treats any unrecognized value as a safe fallback, deliberately *not* an
  exhaustive switch (the pattern `to_symbol_book_id()` uses, see section
  2.2) - the one place in this project that already handles an
  unrecognized enum value safely. Any protocol change touching an enum
  should copy this pattern rather than
  assume the other side only ever sends values you recognize.

### 8.2 Adding an RPC

A new RPC broadly follows the shape of the existing three:

- **Streaming** (like `SubscribeL2Diff`/`SubscribeBbo`): decide its
  `OverflowPolicy` (`Close` vs `DropOldest`, depending on whether this
  stream's messages are deltas or complete state - see section 4's
  criteria), whether to reuse `SymbolBook`'s existing `Fanout<T>`/
  `SubscriberQueue<T>` machinery (almost certainly yes if the new RPC is
  another view of the same book), and whether `book_seq` on it is
  contiguous-required like `SubscribeL2Diff` or gaps-are-normal like
  `SubscribeBbo` - that decision belongs in the `.proto` comment itself,
  not left for a client to guess.
- **Unary** (like `ListBooks`): no `SubscriberQueue` needed, but for a
  query-style RPC consider whether it needs a `book` field at all -
  `ListBooksRequest{}` is empty because it answers "which books does this
  server instance serve", not "what's this book's current state".

Neither a new RPC nor a new field needs to touch conversion logic outside
`book_id.hpp` - `symbol::BookId` is already a stable, bidirectionally
converted type.

## 9. Extensibility: a new aggregator client consumption mode

`hermeneutic_aggregator_client` (`apps/aggregator/client_main.cpp`)
currently has three publisher modes (`bbo`/`volume-bands`/`price-bands`)
plus one query mode (`list`), dispatched via a `Mode` enum plus
`parse_mode()`/`print_usage()`. Adding a new consumption mode (e.g. a new
kind of band computation, or forwarding data to another system) looks
like:

1. Decide whether to subscribe to `SubscribeBbo` or `SubscribeL2Diff` -
   top-of-book-only goes through the former (like `publish_bbo()`);
   maintaining local order-book depth goes through the latter and applies
   the client-side contract listed in section 5 (like `publish_l2_bands()`).
2. If a local order book is needed, reuse `client_book.hpp`'s
   `apply_levels()`/`is_book_seq_gap()` rather than reimplementing them -
   both were pulled out specifically so new client logic can reuse them.
3. One thread per book is the existing shape (`main()` does
   `emplace_back` for a publisher thread per `book_id`); `print_line()`
   already handles atomic multi-threaded stdout writes - a new mode
   should reuse it rather than reimplementing its own mutex handling.
4. `StreamCanceller`/`notify_all_of_stop()` is the shared cancellation-on-
   timeout mechanism; a new mode gets it for free just by accepting the
   same `std::atomic<bool>* stop` parameter.

This client is positioned as a "starting point, not the only
implementation" (see `client_main.cpp`'s own top-of-file comment: "meant
to stay - a starting point for whatever actually consumes the
aggregator's output next") - a fully independent new client (a different
language, a different process) only needs to follow the protocol contract
in sections 3 and 5, not build on top of this C++ program.

## 10. Known limitations and unaddressed extension surfaces

These aren't things this document sets out to solve, but any future
extension should know they exist (full discussion lives in each item's own
memory/document):

- **Version skew in `Heartbeat.live_venues`** (section 3.4) - during a
  mixed-version rolling deploy, this field can't distinguish "an old build
  never populated this" from "there really are zero live venues".
- **SymbolSync's reconnect blast radius** - one symbol's live gap
  currently force-reconnects the entire shared `VenueSession`, not just
  that one symbol (a known, accepted-but-unfixed limitation).
- **SymbolSync's snapshot fetch has no dedup/backoff** - there's
  currently no per-symbol REST fetch cancellation or "back off when there's
  no bridge" mechanism, which could in theory hammer an exchange's REST
  endpoint.
- **An unraised possible direction (not a recorded known issue - purely
  this document's author's own observation)**: under high-frequency
  updates, every `L2Diff`/`Bbo` is currently sent as its own independent
  gRPC message; whether batching multiple `L2Diff`s from the same tick
  round is worth doing has never actually been raised or discussed -
  listed here only to flag it as an untouched corner of the protocol, not
  as a validated performance bottleneck.
