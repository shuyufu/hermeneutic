#pragma once

#include <cstdint>
#include <optional>

#include "bobby/hermeneutic/core/fixed_point.hpp"

// hermeneutic_aggregator_client's own local-book bookkeeping - split out of
// client_main.cpp (rather than left in its anonymous namespace) so it has a
// header a test target can include: apply_levels()'s size_raw==0-means-
// delete convention and is_book_seq_gap()'s contiguity check are the two
// pieces of real logic in that file (both diagnosed and fixed at least once
// before - see git history's "hang on gap/short-lived stream" fix), and
// neither could be unit-tested without either duplicating them into a test
// file or exposing them like this. client_main.cpp itself now only does
// argv parsing, gRPC plumbing, and printing.
namespace bobby::hermeneutic::aggregator {

// Applies one side of a snapshot or diff onto the local book. A snapshot
// level always replaces the side wholesale (hence the caller clear()s
// first, before the first side); a diff level is a replacement at that
// price (size_raw > 0) or a removal (size_raw == 0) - same per-level rule
// either way, see aggregator.proto's PriceLevel/L2Diff comment. `Levels` is
// left as a template parameter (rather than naming the protobuf
// RepeatedPtrField type) purely to avoid an extra include here.
//
// Deliberately does not validate price_raw/size_raw itself (unlike
// AggregateOrderBook::apply_batch()'s is_valid_level() check on the server
// side, which rejects a non-positive price or negative size before it ever
// reaches a book): a wire-level malformed value here would still get caught
// downstream, by price_band_depth()/volume_band_prices() erroring out on
// the next print_bands() call in client_main.cpp - printing "ERROR" for
// that side going forward rather than silently computing a wrong band.
// That's an accepted, known-limited response (the bad level stays in the
// local book forever; nothing here removes it or breaks the stream the way
// a book_seq gap does), not an oversight - this is a diagnostic client, and
// "ERROR" already surfaces the problem to whoever's watching it, which was
// this fix's actual goal. A gap-triggered break()-out-of-read-loop
// treatment for this case, if ever wanted, is future scope, not implied by
// fixing the validation gap itself.
template <typename Map, typename Levels>
void apply_levels(Map& side, const Levels& levels) {
    for (const auto& level : levels) {
        Price price = Price::from_raw(level.price_raw());
        Size size = Size::from_raw(level.size_raw());
        if (size.raw() == 0) side.erase(price);
        else side[price] = size;
    }
}

// True when `book_seq` does not contiguously follow `last_seq` -
// SubscribeL2Diff's own book_seq contiguity guarantee (aggregator.proto:
// "book_seq must be contiguous... a gap means a revision was missed").
// `last_seq` is nullopt only before this stream's very first snapshot/diff
// has been seen, which can never itself be a gap - there's nothing yet for
// it to be discontiguous with.
constexpr bool is_book_seq_gap(std::optional<std::uint64_t> last_seq, std::uint64_t book_seq) noexcept {
    return last_seq.has_value() && book_seq != *last_seq + 1;
}

}  // namespace bobby::hermeneutic::aggregator
