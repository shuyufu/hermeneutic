#include "bobby/hermeneutic/book/l2_order_book.hpp"

#include <gtest/gtest.h>

namespace bobby::hermeneutic {
namespace {

TEST(L2OrderBook, AsksAreOrderedAscending) {
    L2OrderBook book;
    book.asks[Price(101.0)] = Size(1.0);
    book.asks[Price(100.5)] = Size(2.0);
    book.asks[Price(102.0)] = Size(0.5);

    ASSERT_EQ(book.asks.size(), 3u);
    auto it = book.asks.begin();
    EXPECT_EQ(it->first, Price(100.5));
    ++it;
    EXPECT_EQ(it->first, Price(101.0));
    ++it;
    EXPECT_EQ(it->first, Price(102.0));
}

TEST(L2OrderBook, BidsAreOrderedDescending) {
    L2OrderBook book;
    book.bids[Price(99.0)] = Size(1.0);
    book.bids[Price(99.5)] = Size(2.0);
    book.bids[Price(98.0)] = Size(0.5);

    ASSERT_EQ(book.bids.size(), 3u);
    auto it = book.bids.begin();
    EXPECT_EQ(it->first, Price(99.5));
    ++it;
    EXPECT_EQ(it->first, Price(99.0));
    ++it;
    EXPECT_EQ(it->first, Price(98.0));
}

TEST(L2OrderBook, BestAskAndBestBidAreFirstElement) {
    L2OrderBook book;
    book.asks[Price(101.0)] = Size(1.0);
    book.asks[Price(100.5)] = Size(2.0);
    book.bids[Price(99.0)] = Size(1.0);
    book.bids[Price(99.5)] = Size(2.0);

    EXPECT_EQ(book.asks.begin()->first, Price(100.5));
    EXPECT_EQ(book.bids.begin()->first, Price(99.5));
}

TEST(L2OrderBook, UpdatingExistingLevelReplacesSize) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(1.0);
    book.asks[Price(100.0)] = Size(3.0);

    ASSERT_EQ(book.asks.size(), 1u);
    EXPECT_EQ(book.asks.at(Price(100.0)), Size(3.0));
}

TEST(L2OrderBook, ErasingZeroSizeLevelRemovesIt) {
    L2OrderBook book;
    book.bids[Price(99.0)] = Size(1.0);
    book.bids.erase(Price(99.0));

    EXPECT_TRUE(book.bids.empty());
}

}  // namespace
}  // namespace bobby::hermeneutic
