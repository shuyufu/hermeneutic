#include "apps/aggregator/client_book.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <optional>
#include <vector>

#include "bobby/hermeneutic/core/fixed_point.hpp"

namespace bobby::hermeneutic::aggregator {
namespace {

// Minimal stand-in for aggregator.proto's PriceLevel - just enough surface
// (price_raw()/size_raw()) for apply_levels() to consume, so this test
// doesn't need the protobuf/gRPC dependency client_book.hpp's own `Levels`
// template parameter was designed to avoid (see its doc comment).
struct FakeLevel {
    std::int64_t price_raw_;
    std::int64_t size_raw_;
    std::int64_t price_raw() const { return price_raw_; }
    std::int64_t size_raw() const { return size_raw_; }
};

using Book = std::map<Price, Size>;

TEST(ApplyLevels, PositiveSizeSetsThePriceLevel) {
    Book side;
    apply_levels(side, std::vector<FakeLevel>{{Price(100.0).raw(), Size(5.0).raw()}});
    EXPECT_EQ(side.at(Price(100.0)), Size(5.0));
}

TEST(ApplyLevels, ZeroSizeRemovesAnExistingLevel) {
    Book side;
    side[Price(100.0)] = Size(5.0);
    apply_levels(side, std::vector<FakeLevel>{{Price(100.0).raw(), 0}});
    EXPECT_EQ(side.find(Price(100.0)), side.end());
}

TEST(ApplyLevels, ZeroSizeOnAnAbsentLevelIsANoOp) {
    Book side;
    apply_levels(side, std::vector<FakeLevel>{{Price(100.0).raw(), 0}});
    EXPECT_TRUE(side.empty());
}

TEST(ApplyLevels, MultipleLevelsEachAppliedIndependently) {
    Book side;
    side[Price(100.0)] = Size(1.0);
    apply_levels(side, std::vector<FakeLevel>{
                            {Price(100.0).raw(), 0},                // removes 100.0
                            {Price(101.0).raw(), Size(2.0).raw()},  // adds 101.0
                        });
    EXPECT_EQ(side.find(Price(100.0)), side.end());
    EXPECT_EQ(side.at(Price(101.0)), Size(2.0));
}

// The for-loop in apply_levels() applies each entry in the order `levels`
// gives them, not e.g. via a bulk insert whose behavior for a duplicate key
// within one call would be unspecified - the same price appearing twice in
// one snapshot/diff message must leave the *later* entry's size in effect.
TEST(ApplyLevels, LaterEntryForTheSamePriceInOneCallWins) {
    Book side;
    apply_levels(side, std::vector<FakeLevel>{
                            {Price(100.0).raw(), Size(1.0).raw()},
                            {Price(100.0).raw(), Size(2.0).raw()},
                        });
    EXPECT_EQ(side.at(Price(100.0)), Size(2.0));
}

TEST(IsBookSeqGap, NoLastSeqIsNeverAGap) {
    // nullopt means "nothing seen yet on this stream" - there's nothing for
    // the very first snapshot/diff's book_seq to be discontiguous with.
    EXPECT_FALSE(is_book_seq_gap(std::nullopt, 0));
    EXPECT_FALSE(is_book_seq_gap(std::nullopt, 12345));
}

TEST(IsBookSeqGap, ContiguousNextSeqIsNotAGap) {
    EXPECT_FALSE(is_book_seq_gap(std::optional<std::uint64_t>(100), 101));
}

TEST(IsBookSeqGap, SkippedSeqIsAGap) {
    EXPECT_TRUE(is_book_seq_gap(std::optional<std::uint64_t>(100), 103));
}

TEST(IsBookSeqGap, RepeatedSeqIsAGap) {
    // Not contiguous either - the same book_seq arriving twice means
    // something regressed upstream, not merely "no progress yet".
    EXPECT_TRUE(is_book_seq_gap(std::optional<std::uint64_t>(100), 100));
}

TEST(IsBookSeqGap, SeqGoingBackwardIsAGap) {
    EXPECT_TRUE(is_book_seq_gap(std::optional<std::uint64_t>(100), 99));
}

}  // namespace
}  // namespace bobby::hermeneutic::aggregator
