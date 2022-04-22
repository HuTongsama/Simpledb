#pragma once
#include"SimpleDbTestBase.h"
#include"HeapFile.h"
#include"TableStats.h"
#include"HeapFileEncoder.h"
#include"Utility.h"
#include"SystemTestUtil.h"
#include"JoinOptimizer.h"
using namespace Simpledb;
class JoinOptimizerTest : public SimpleDbTestBase {
protected:
    /**
     * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create
     * an identical HeapFile table
     *
     * @param tuples
     *            Tuples to create a HeapFile from
     * @param columns
     *            Each entry in tuples[] must have
     *            "columns == tuples.get(i).size()"
     * @param colPrefix
     *            String to prefix to the column names (the columns are named
     *            after their column number by default)
     * @return a new HeapFile containing the specified tuples
     * @throws runtime error
     *             if a temporary file can't be created to hand to HeapFile to
     *             open and read its data
     */
    static shared_ptr<HeapFile> createDuplicateHeapFile(
        vector<vector<int>>& tuples, int columns, const string& colPrefix){
        shared_ptr<File> temp = File::createTempFile();
        temp->deleteOnExit();
        HeapFileEncoder::convert(tuples, *temp, BufferPool::getPageSize(), columns);
        return Utility::openHeapFile(columns, colPrefix, temp);
    }

    void SetUp()override {
        SimpleDbTestBase::SetUp();
        // Create some sample tables to work with
        
        _f1 = SystemTestUtil::createRandomHeapFile(10, 1000, 20, map<int, int>(), _tuples1, string("c"));
        _tableName1 = "TA";
        Database::getCatalog()->addTable(_f1, _tableName1);
        _tableId1 = Database::getCatalog()->getTableId(_tableName1);
        cout << "tableId1: " + to_string(_tableId1) << endl;

        _stats1 = make_shared<TableStats>(_tableId1, 19);
        TableStats::setTableStats(_tableName1, _stats1);
        _f2 = SystemTestUtil::createRandomHeapFile(10, 10000, 20, map<int, int>(), _tuples2, string("c"));
        _tableName2 = "TB";
        Database::getCatalog()->addTable(_f2, _tableName2);
        _tableId2 = Database::getCatalog()->getTableId(_tableName2);
        cout << "tableId2: " + to_string(_tableId2) << endl;

        _stats2 = make_shared<TableStats>(_tableId2, 19);

        TableStats::setTableStats(_tableName2, _stats2);
    }

    vector<double> getRandomJoinCosts(shared_ptr<JoinOptimizer> jo, shared_ptr<LogicalJoinNode> js,
        vector<int>& card1s, vector<int>& card2s, vector<double>& cost1s, vector<double>& cost2s) {
        size_t sz = card1s.size();
        vector<double> ret(sz, 0);
        for (int i = 0; i < sz; ++i) {
            ret[i] = jo->estimateJoinCost(js, card1s[i], card2s[i], cost1s[i],
                cost2s[i]);
            // assert that he join cost is no less than the total cost of
            // scanning two tables
            EXPECT_TRUE(ret[i] > cost1s[i] + cost2s[i]);
        }
        return ret;
    }

    void checkJoinEstimateCosts(shared_ptr<JoinOptimizer> jo, shared_ptr<LogicalJoinNode> equalsJoinNode) {
        vector<int> card1s(20, 0);
        size_t sz = card1s.size();
        vector<int> card2s(sz, 0);
        vector<double> cost1s(sz, 0);
        vector<double> cost2s(sz, 0);
        vector<Object> ret;
        // card1s linear others constant
        for (int i = 0; i < sz; ++i) {
            card1s[i] = 3 * i + 1;
            card2s[i] = 5;
            cost1s[i] = cost2s[i] = 5.0;
        }
        vector<double> stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s,
            cost1s, cost2s);
        ret = SystemTestUtil::checkLinear(stats);
        EXPECT_TRUE(ret[0]._bool);
        // card2s linear others constant
        for (int i = 0; i < sz; ++i) {
            card1s[i] = 4;
            card2s[i] = 3 * i + 1;
            cost1s[i] = cost2s[i] = 5.0;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
            cost2s);
        ret = SystemTestUtil::checkLinear(stats);
        EXPECT_TRUE(ret[0]._bool);
        // cost1s linear others constant
        for (int i = 0; i < sz; ++i) {
            card1s[i] = card2s[i] = 7;
            cost1s[i] = 5.0 * (i + 1);
            cost2s[i] = 3.0;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
            cost2s);
        ret = SystemTestUtil::checkLinear(stats);
        EXPECT_TRUE(ret[0]._bool);
        // cost2s linear others constant
        for (int i = 0; i < sz; ++i) {
            card1s[i] = card2s[i] = 9;
            cost1s[i] = 5.0;
            cost2s[i] = 3.0 * (i + 1);
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
            cost2s);
        ret = SystemTestUtil::checkLinear(stats);
        EXPECT_TRUE(ret[0]._bool);
        // everything linear
        for (int i = 0; i < sz; ++i) {
            card1s[i] = 2 * (i + 1);
            card2s[i] = 9 * i + 1;
            cost1s[i] = 5.0 * i + 2;
            cost2s[i] = 3.0 * i + 1;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
            cost2s);
        ret = SystemTestUtil::checkQuadratic(stats);
        EXPECT_TRUE(ret[0]._bool);
    }

    vector<vector<int>> _tuples1;
    shared_ptr<HeapFile> _f1;
    string _tableName1;
    size_t _tableId1;
    shared_ptr<TableStats> _stats1;

    vector<vector<int>> _tuples2;
    shared_ptr<HeapFile> _f2;
    string _tableName2;
    size_t _tableId2;
    shared_ptr<TableStats> _stats2;
};

/**
 * Verify that the estimated join costs from estimateJoinCost() are
 * reasonable we check various order requirements for the output of
 * estimateJoinCost.
 */
TEST_F(JoinOptimizerTest, EstimateJoinCostTest) {
    // It's hard to narrow these down much at all, because students
        // may have implemented custom join algorithms.
        // So, just make sure the orders of the return values make sense.

    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    shared_ptr<JoinOptimizer> jo;
    //Parser p = new Parser();
    //jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
    //    + tableName1 + " t1, " + tableName2
    //    + " t2 WHERE t1.c1 = t2.c2;"), new ArrayList<>());
    //// 1 join 2
    //LogicalJoinNode equalsJoinNode = new LogicalJoinNode(tableName1,
    //    tableName2, Integer.toString(1), Integer.toString(2),
    //    Predicate.Op.EQUALS);
    //checkJoinEstimateCosts(jo, equalsJoinNode);
    //// 2 join 1
    //jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
    //    + tableName1 + " t1, " + tableName2
    //    + " t2 WHERE t1.c1 = t2.c2;"), new ArrayList<>());
    //equalsJoinNode = new LogicalJoinNode(tableName2, tableName1,
    //    Integer.toString(2), Integer.toString(1), Predicate.Op.EQUALS);
    //checkJoinEstimateCosts(jo, equalsJoinNode);
    //// 1 join 1
    //jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
    //    + tableName1 + " t1, " + tableName1
    //    + " t2 WHERE t1.c3 = t2.c4;"), new ArrayList<>());
    //equalsJoinNode = new LogicalJoinNode(tableName1, tableName1,
    //    Integer.toString(3), Integer.toString(4), Predicate.Op.EQUALS);
    //checkJoinEstimateCosts(jo, equalsJoinNode);
    //// 2 join 2
    //jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
    //    + tableName2 + " t1, " + tableName2
    //    + " t2 WHERE t1.c8 = t2.c7;"), new ArrayList<>());
    //equalsJoinNode = new LogicalJoinNode(tableName2, tableName2,
    //    Integer.toString(8), Integer.toString(7), Predicate.Op.EQUALS);
    //checkJoinEstimateCosts(jo, equalsJoinNode);
}