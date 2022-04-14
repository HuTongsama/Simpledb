#pragma once
#include"SimpleDbTestBase.h"
#include"TableStats.h"
#include"HeapFile.h"
#include"SystemTestUtil.h"
#include"IntField.h"
#include<vector>
using namespace Simpledb;
class TableStatsTest : public SimpleDbTestBase {
protected:
	const static int IO_COST = 71;
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_f = SystemTestUtil::createRandomHeapFile(10, 10200, 32, map<int, int>(), _tuples);
		_tableName = SystemTestUtil::getUUID();
		Database::getCatalog()->addTable(_f, _tableName);
		_tableId = Database::getCatalog()->getTableId(_tableName);
	}
	vector<double> getRandomTableScanCosts(vector<int>& pageNums, vector<int>& ioCosts) {
		vector<double> ret(ioCosts.size());
		for (int i = 0; i < ioCosts.size(); ++i) {
			shared_ptr<HeapFile> hf = SystemTestUtil::createRandomHeapFile(1, 992 * pageNums[i], 32, map<int, int>(), _tuples);
			EXPECT_EQ(pageNums[i], hf->numPages());
			string tableName = SystemTestUtil::getUUID();
			Database::getCatalog()->addTable(hf, tableName);
			size_t tableId = Database::getCatalog()->getTableId(tableName);
			ret[i] = TableStats(tableId, ioCosts[i]).estimateScanCost();
		}
		return ret;
	}


	vector<vector<int>> _tuples;
	shared_ptr<HeapFile> _f;
	string _tableName;
	size_t _tableId;
};
/**
 * Verify the cost estimates of scanning various numbers of pages from a HeapFile
 * This test checks that the estimateScanCost is:
 *   +linear in numPages when IO_COST is constant
 *   +linear in IO_COST when numPages is constant
 *   +quadratic when IO_COST and numPages increase linearly.
 */
TEST_F(TableStatsTest, EstimateScanCostTest) {
	vector<Object> ret;
	size_t sz = 20;
	vector<int> ioCosts(sz,0);
	vector<int> pageNums(sz, 0);
	// IO_COST constant, numPages change
	for (int i = 0; i < sz; ++i) {
		ioCosts[i] = 1;
		pageNums[i] = 3 * (i + 1);
	}
	vector<double> stats = getRandomTableScanCosts(pageNums, ioCosts);
	ret = SystemTestUtil::checkConstant(stats);
	EXPECT_FALSE(ret[0]._bool);
	ret = SystemTestUtil::checkLinear(stats);
	EXPECT_TRUE(ret[0]._bool);
	// numPages constant, IO_COST change
	for (int i = 0; i < sz; ++i) {
		ioCosts[i] = 10 * (i + 1);
		pageNums[i] = 3;
	}
	stats = getRandomTableScanCosts(pageNums, ioCosts);
	ret = SystemTestUtil::checkConstant(stats);
	EXPECT_FALSE(ret[0]._bool);
	ret = SystemTestUtil::checkLinear(stats);
	EXPECT_TRUE(ret[0]._bool);
	//numPages & IO_COST increase linearly
	for (int i = 0; i < sz; ++i) {
		ioCosts[i] = 3 * (i + 1);
		pageNums[i] = (i + 1);
	}
	stats = getRandomTableScanCosts(pageNums, ioCosts);
	ret = SystemTestUtil::checkConstant(stats);
	EXPECT_FALSE(ret[0]._bool);
	ret = SystemTestUtil::checkLinear(stats);
	EXPECT_FALSE(ret[0]._bool);
	ret = SystemTestUtil::checkQuadratic(stats);
	EXPECT_TRUE(ret[0]._bool);
}

TEST_F(TableStatsTest, EstimateTableCardinalityTest) {
	TableStats s(_tableId, IO_COST);

	// Try a random selectivity
	EXPECT_EQ(3060, s.estimateTableCardinality(0.3));

	// Make sure we get all rows with 100% selectivity, and none with 0%
	EXPECT_EQ(10200, s.estimateTableCardinality(1.0));
	EXPECT_EQ(0, s.estimateTableCardinality(0.0));
}



/**
 * Verify that selectivity estimates do something reasonable.
 * Don't bother splitting this into N different functions for
 * each possible Op because we will probably catch any bugs here in
 * IntHistogramTest, so we hopefully don't need all the JUnit checkboxes.
 */
TEST_F(TableStatsTest, EstimateSelectivityTest) {
	static const int maxCellVal = 32;	// Tuple values are randomized between 0 and this number

	shared_ptr<Field> aboveMax = make_shared<IntField>(maxCellVal + 10);
	shared_ptr<Field> atMax = make_shared<IntField>(maxCellVal);
	shared_ptr<Field> halfMaxMin = make_shared<IntField>(maxCellVal / 2);
	shared_ptr<Field> atMin = make_shared<IntField>(0);
	shared_ptr<Field> belowMin = make_shared<IntField>(-10);

	TableStats s(_tableId, IO_COST);

	for (int col = 0; col < 10; col++) {
		
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::EQUALS, aboveMax), 0.001);
		EXPECT_EQ(1.0 / 32.0, s.estimateSelectivity(col, Predicate::Op::EQUALS, halfMaxMin), 0.015);
		EXPECT_EQ(0, s.estimateSelectivity(col, Predicate::Op::EQUALS, belowMin), 0.001);

		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::NOT_EQUALS, aboveMax), 0.001);
		EXPECT_EQ(31.0 / 32.0, s.estimateSelectivity(col, Predicate::Op::NOT_EQUALS, halfMaxMin), 0.015);
		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::NOT_EQUALS, belowMin), 0.015);

		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN, aboveMax), 0.001);
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN, atMax), 0.001);
		EXPECT_EQ(0.5, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN, halfMaxMin), 0.1);
		EXPECT_EQ(31.0 / 32.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN, atMin), 0.05);
		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN, belowMin), 0.001);

		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN, aboveMax), 0.001);
		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN, atMax), 0.015);
		EXPECT_EQ(0.5, s.estimateSelectivity(col, Predicate::Op::LESS_THAN, halfMaxMin), 0.1);
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN, atMin), 0.001);
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN, belowMin), 0.001);

		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN_OR_EQ, aboveMax), 0.001);
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN_OR_EQ, atMax), 0.015);
		EXPECT_EQ(0.5, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN_OR_EQ, halfMaxMin), 0.1);
		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN_OR_EQ, atMin), 0.015);
		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::GREATER_THAN_OR_EQ, belowMin), 0.001);

		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN_OR_EQ, aboveMax), 0.001);
		EXPECT_EQ(1.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN_OR_EQ, atMax), 0.015);
		EXPECT_EQ(0.5, s.estimateSelectivity(col, Predicate::Op::LESS_THAN_OR_EQ, halfMaxMin), 0.1);
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN_OR_EQ, atMin), 0.05);
		EXPECT_EQ(0.0, s.estimateSelectivity(col, Predicate::Op::LESS_THAN_OR_EQ, belowMin), 0.001);
	}
}