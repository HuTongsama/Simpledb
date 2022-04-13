#pragma once
#include"pch.h"
#include"IntHistogram.h"
#include"Predicate.h"
using namespace Simpledb;
class IntHistogramTest : public ::testing::Test {};

/**
 * Test to confirm that the IntHistogram implementation is constant-space
 * (or, at least, reasonably small space; O(log(n)) might still work if
 * your constants are good).
 */
TEST_F(IntHistogramTest, OrderOfGrowthTest) {
	// Don't bother with a timeout on this test.
	// Printing debugging statements takes >> time than some inefficient algorithms.
	IntHistogram h(10000, 0, 100);

	// Feed the histogram more integers than would fit into our
	// 128mb allocated heap (4-byte integers)
	// If this fails, someone's storing every value...
	for (int c = 0; c < 33554432; c++) {
		h.addValue((c * 23) % 101);	// Pseudo-random number; at least get a distribution
	}

	// Try printing out all of the values; make sure "estimateSelectivity()"
	// cause any problems
	double selectivity = 0.0;
	for (int c = 0; c < 101; c++) {
		selectivity += h.estimateSelectivity(Predicate::Op::EQUALS, c);
	}

	// All the selectivities should add up to 1, by definition.
	// Allow considerable leeway for rounding error, though 
	// (Java double's are good to 15 or so significant figures)
	EXPECT_TRUE(selectivity > 0.99);
}
/**
 * Test with a minimum and a maximum that are both negative numbers.
 */
TEST_F(IntHistogramTest, NegativeRangeTest) {
	IntHistogram h(10, -60, -10);

	// All of the values here are negative.
	// Also, there are more of them than there are bins.
	for (int c = -60; c <= -10; c++) {
		h.addValue(c);
		h.estimateSelectivity(Predicate::Op::EQUALS, c);
	}

	// Even with just 10 bins and 50 values,
	// the selectivity for this particular value should be at most 0.2.
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::EQUALS, -33) < 0.3);

	// And it really shouldn't be 0.
	// Though, it could easily be as low as 0.02, seeing as that's
	// the fraction of elements that actually are equal to -33.
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::EQUALS, -33) > 0.001);
}
/**
 * Make sure that equality binning does something reasonable.
 */
TEST_F(IntHistogramTest, OpEqualsTest) {
	IntHistogram h(10, 1, 10);

	// Set some values
	h.addValue(3);
	h.addValue(3);
	h.addValue(3);

	// This really should return "1.0"; but,
	// be conservative in case of alternate implementations
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::EQUALS, 3) > 0.8);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::EQUALS, 8) < 0.001);
}
/**
 * Make sure that GREATER_THAN binning does something reasonable.
 */
TEST_F(IntHistogramTest, OpGreaterThanTest) {
	IntHistogram h(10, 1, 10);

	// Set some values
	h.addValue(3);
	h.addValue(3);
	h.addValue(3);
	h.addValue(1);
	h.addValue(10);

	// Be conservative in case of alternate implementations
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN, -1) > 0.999);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN, 2) > 0.6);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN, 4) < 0.4);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN, 12) < 0.001);
}
/**
 * Make sure that LESS_THAN binning does something reasonable.
 */
TEST_F(IntHistogramTest, OpLessThanTest) {
	IntHistogram h(10, 1, 10);

	// Set some values
	h.addValue(3);
	h.addValue(3);
	h.addValue(3);
	h.addValue(1);
	h.addValue(10);

	// Be conservative in case of alternate implementations
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN, -1) < 0.001);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN, 2) < 0.4);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN, 3) < 0.4);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN, 4) > 0.6);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN, 12) > 0.999);
}
/**
 * Make sure that GREATER_THAN_OR_EQ binning does something reasonable.
 */
TEST_F(IntHistogramTest, OpGreaterThanOrEqualsTest) {
	IntHistogram h(10, 1, 10);

	// Set some values
	h.addValue(3);
	h.addValue(3);
	h.addValue(3);
	h.addValue(1);
	h.addValue(10);

	// Be conservative in case of alternate implementations
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN_OR_EQ, -1) > 0.999);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN_OR_EQ, 2) > 0.6);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN_OR_EQ, 3) > 0.45);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN_OR_EQ, 4) < 0.5);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::GREATER_THAN_OR_EQ, 12) < 0.001);
}
/**
 * Make sure that LESS_THAN_OR_EQ binning does something reasonable.
 */
TEST_F(IntHistogramTest, OpLessThanOrEqualsTest) {
	IntHistogram h(10, 1, 10);

	// Set some values
	h.addValue(3);
	h.addValue(3);
	h.addValue(3);
	h.addValue(1);
	h.addValue(10);

	// Be conservative in case of alternate implementations
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN_OR_EQ, -1) < 0.001);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN_OR_EQ, 2) < 0.4);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN_OR_EQ, 3) > 0.45);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN_OR_EQ, 4) > 0.6);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::LESS_THAN_OR_EQ, 12) > 0.999);
}
/**
 * Make sure that equality binning does something reasonable.
 */
TEST_F(IntHistogramTest, OpNotEqualsTest) {
	IntHistogram h(10, 1, 10);

	// Set some values
	h.addValue(3);
	h.addValue(3);
	h.addValue(3);

	// Be conservative in case of alternate implementations
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::NOT_EQUALS, 3) < 0.001);
	EXPECT_TRUE(h.estimateSelectivity(Predicate::Op::NOT_EQUALS, 8) > 0.01);
}