#pragma once

#include <exception>
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
// inside VenueSession. See docs/ingestion_design.md 第 10 節第 2 項.
class IVenueSession {
  public:
    virtual ~IVenueSession() = default;

    // VenueSession::start() under the hood - the executor/strand it runs
    // on was already fixed at the concrete VenueSession's construction, so
    // this takes no argument.
    virtual void start() = 0;

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

    void start() override {
        // run() only returns via an uncaught exception in its own setup,
        // or - the expected path - once stop() has finished draining it.
        // Either way there's nothing generic left to do beyond logging
        // which venue this was; that's the one thing this adapter adds
        // over calling VenueSession::start() directly. Formatted to a
        // string once here, at capture time, rather than capturing the
        // VenueId itself and formatting on every call - log_exception()
        // only ever wants a display string, never the structured value.
        session_.start([venue = bobby::hermeneutic::symbol::to_string(session_.venue())](std::exception_ptr e) {
            log_exception(venue, "ingestion session ended", e);
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
// See docs/ingestion_design.md 第 10 節第 2 項's 驗收標準.
class IngestionRunner {
  public:
    template <typename Feed, typename Policy, typename NextLayer, typename Book, typename... Args>
    void add(Args&&... args) {
        sessions_.push_back(std::make_unique<VenueSessionAdapter<Feed, Policy, NextLayer, Book>>(
            std::forward<Args>(args)...));
    }

    void start_all() {
        for (auto& session : sessions_) session->start();
    }

    void stop_all() {
        for (auto& session : sessions_) session->stop();
    }

  private:
    std::vector<std::unique_ptr<IVenueSession>> sessions_;
};

}  // namespace bobby::hermeneutic::ingestion
