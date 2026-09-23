#pragma once

#include <atomic>
#include <barrier>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "bobby/hermeneutic/ingestion/venue_session.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::ingestion {

// The thin, non-template boundary a group of differently-typed
// VenueSession<Feed,Policy,NextLayer> is managed through. Every concrete
// instantiation's start()/stop() already have the same (non-template)
// signature, so nothing about SymbolSync/VenueFeed needs to become
// virtual - the per-message hot path (parse_message -> SymbolSync ->
// apply_batch) never crosses this interface, it stays fully monomorphized
// inside VenueSession.
class IVenueSession {
  public:
    virtual ~IVenueSession() = default;

    // VenueSession::start() under the hood - the executor/strand it runs
    // on was already fixed at the concrete VenueSession's construction, so
    // this takes no argument beyond `on_session_done`. That fires once
    // this session's own run() has fully finished, regardless of why -
    // the expected reason is stop() draining it, but this doesn't
    // distinguish that from any other way run() could end. See
    // IngestionRunner::start_all()'s own comment for what it's for.
    virtual void start(std::function<void()> on_session_done) = 0;

    // VenueSession::stop() under the hood - see there for what this
    // actually guarantees (a real drain, not just a cooperative flag).
    virtual void stop() = 0;
};

template <typename Feed, typename Policy, typename NextLayer, typename Book>
class VenueSessionAdapter : public IVenueSession {
  public:
    // Constructs the VenueSession in place rather than taking one by
    // value: once started, run()'s coroutine frame captures `this`, so
    // this object must never move afterward - a vector of adapters-by-
    // value would dangle on reallocation. That's why IngestionRunner
    // stores these behind unique_ptr instead of storing adapters inline.
    template <typename... Args>
    explicit VenueSessionAdapter(Args&&... args) : session_(std::forward<Args>(args)...) {}

    void start(std::function<void()> on_session_done) override {
        // run() only returns via an uncaught exception in its own setup,
        // or - the expected path - once stop() has finished draining it.
        // Either way, this adapter's own contribution (logging which
        // venue this was) runs first, then `on_session_done` - see
        // IngestionRunner::start_all()'s own comment for what that's
        // for. Formatted to a string once here, at capture time, rather
        // than capturing the VenueId itself and formatting on every
        // call - log_exception() only ever wants a display string,
        // never the structured value.
        session_.start([venue = bobby::hermeneutic::symbol::to_string(session_.venue()),
                        on_session_done = std::move(on_session_done)](std::exception_ptr e) {
            log_exception(venue, "ingestion session ended", e);
            on_session_done();
        });
    }

    void stop() override { session_.stop(); }

  private:
    VenueSession<Feed, Policy, NextLayer, Book> session_;
};

// Owns a group of differently-typed VenueSessions and manages them
// uniformly: add() registers one (its concrete Feed/Policy/NextLayer stay
// known only at the call site - e.g. a per-venue factory function - they
// never need to appear in this class or in whatever constructs it),
// start_all()/stop_all() drive the whole group at once.
//
// stop_all() alone is enough for a clean teardown: once every session's
// own start()'s on_done has fired, nothing is left touching any of them,
// so the caller can destroy this IngestionRunner (and, separately, stop
// its io_context) without needing io_context::stop() as a blunt backstop.
class IngestionRunner {
  public:
    template <typename Feed, typename Policy, typename NextLayer, typename Book, typename... Args>
    void add(Args&&... args) {
        sessions_.push_back(std::make_unique<VenueSessionAdapter<Feed, Policy, NextLayer, Book>>(
            std::forward<Args>(args)...));
    }

    // `on_all_drained`, if given, fires exactly once - after every
    // session's own start() has completed (i.e. every venue's run() has
    // actually finished) *and* stop_all() has actually been called. A
    // caller that also drives this same work's io_context and wants to
    // know precisely when nothing here is generating new work anymore -
    // rather than assuming based on timing, or reaching for
    // io_context::stop() as a blunt "just abandon whatever's still
    // pending" fallback - hooks in here. See server_main.cpp's own use:
    // it cancels a signal_set's re-armed wait (kept alive to catch a
    // second shutdown signal) only once this fires, so that escape
    // hatch stays live for exactly as long as a genuinely stuck drain
    // could still need it, instead of being cancelled unconditionally
    // on a timing guess.
    //
    // The stop_all() requirement is enforced here, by this class, not
    // left to every caller to separately track and check for itself:
    // every session finishing is *expected* to mean stop_all() asked
    // them to, but nothing about VenueSession::run() enforces that as an
    // invariant (a future change to its own error handling could let it
    // return some other way) - a caller that reacts to on_all_drained by
    // disarming something safety-critical (server_main.cpp cancels a
    // signal_set's shutdown-escape-hatch wait) needs that guarantee to
    // come from this API itself, not from remembering to replicate the
    // same check at every call site.
    //
    // Only one subscriber, deliberately: nothing in this codebase needs
    // more than one caller reacting to "every venue has drained," and a
    // multi-listener version would be unexercised, untested machinery
    // until something actually needs it.
    //
    // Precondition on the zero-sessions path below: whatever
    // `on_all_drained` might trigger (server_main.cpp's use cancels a
    // signal_set's pending wait) must already be set up by the time
    // start_all() is called, since that path invokes it synchronously,
    // inline, before this function returns - not merely "soon after."
    // Every real caller in this codebase always has at least one venue
    // wired by the time it calls this, so this path is exercised only by
    // this file's own tests, never in production - but it's still part
    // of the contract, not just a test convenience. Deliberately *not*
    // gated on stop_all() the way the non-empty path below is: with zero
    // sessions there is nothing to drain and therefore no "too early"
    // to guard against - the guarantee that matters (on_all_drained
    // only reflects venues that have actually finished) holds
    // vacuously.
    //
    // If any session's own start() throws synchronously instead of
    // invoking its completion, that session's arrival never happens and
    // `on_all_drained` never fires for the rest of the process - start()
    // isn't documented to throw, and nothing here tries to recover from
    // it if it does, matching every other caller in this codebase: an
    // uncaught exception here already propagates out of main() and
    // terminates the process before "on_all_drained never fires" could
    // matter.
    void start_all(std::function<void()> on_all_drained = {}) {
        if (sessions_.empty()) {
            if (on_all_drained) on_all_drained();
            return;
        }
        // A one-shot std::barrier, not a hand-rolled atomic countdown:
        // every session's own completion calls arrive() (never
        // arrive_and_wait() - nothing here ever blocks a thread on this),
        // and the barrier's own completion-function mechanism invokes
        // the callback exactly once, on whichever session's completion
        // happens to be the last to arrive, on that thread - no
        // memory-order or off-by-one reasoning to re-derive for
        // something the standard library already got right once.
        // `this` is captured (not just `on_all_drained`) so the
        // completion function can check stop_requested_ - safe for the
        // same reason capturing `barrier` in each session's own lambda
        // below is: this callback can only ever run while some session
        // this IngestionRunner still owns is completing, which requires
        // this IngestionRunner to still be alive to own it.
        auto completion = [this, callback = std::move(on_all_drained)]() mutable {
            if (stop_requested_.load() && callback) callback();
        };
        auto barrier = std::make_shared<std::barrier<decltype(completion)>>(
            static_cast<std::ptrdiff_t>(sessions_.size()), std::move(completion));
        for (auto& session : sessions_) {
            // arrive()'s return value (an arrival_token, [[nodiscard]])
            // is only useful to a caller that might later wait() on this
            // exact phase - nothing here ever does, so it's explicitly
            // discarded rather than silently ignored.
            session->start([barrier] { (void)barrier->arrive(); });
        }
    }

    void stop_all() {
        stop_requested_ = true;
        for (auto& session : sessions_) session->stop();
    }

  private:
    std::atomic<bool> stop_requested_{false};
    std::vector<std::unique_ptr<IVenueSession>> sessions_;
};

}  // namespace bobby::hermeneutic::ingestion
