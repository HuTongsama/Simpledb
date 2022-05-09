#pragma once
#include"SimpleDbTestBase.h"
#include"HeapFile.h"
#include"TableStats.h"
#include"HeapFileEncoder.h"
#include"Utility.h"
#include"SystemTestUtil.h"
#include"JoinOptimizer.h"
#include"Parser.h"
#include<algorithm>
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
    Parser p;
    jo = make_shared<JoinOptimizer>(p.generateLogicalPlan(tid, "SELECT * FROM "
        + _tableName1 + " t1, " + _tableName2
        + " t2 WHERE t1.c1 = t2.c2;"), vector<shared_ptr<LogicalJoinNode>>());
    // 1 join 2
    shared_ptr<LogicalJoinNode> equalsJoinNode = make_shared<LogicalJoinNode>(_tableName1,
        _tableName2, to_string(1), to_string(2),
        Predicate::Op::EQUALS);
    checkJoinEstimateCosts(jo, equalsJoinNode);
    // 2 join 1
    jo = make_shared<JoinOptimizer>(p.generateLogicalPlan(tid, "SELECT * FROM "
        + _tableName1 + " t1, " + _tableName2
        + " t2 WHERE t1.c1 = t2.c2;"), vector<shared_ptr<LogicalJoinNode>>());
    equalsJoinNode = make_shared<LogicalJoinNode>(_tableName2, _tableName1,
        to_string(2), to_string(1), Predicate::Op::EQUALS);
    checkJoinEstimateCosts(jo, equalsJoinNode);
    // 1 join 1
    jo = make_shared<JoinOptimizer>(p.generateLogicalPlan(tid, "SELECT * FROM "
        + _tableName1 + " t1, " + _tableName1
        + " t2 WHERE t1.c3 = t2.c4;"), vector<shared_ptr<LogicalJoinNode>>());
    equalsJoinNode = make_shared<LogicalJoinNode>(_tableName1, _tableName1,
        to_string(3), to_string(4), Predicate::Op::EQUALS);
    checkJoinEstimateCosts(jo, equalsJoinNode);
    // 2 join 2
    jo = make_shared<JoinOptimizer>(p.generateLogicalPlan(tid, "SELECT * FROM "
        + _tableName2 + " t1, " + _tableName2
        + " t2 WHERE t1.c8 = t2.c7;"), vector<shared_ptr<LogicalJoinNode>>());
    equalsJoinNode = make_shared<LogicalJoinNode>(_tableName2, _tableName2,
        to_string(8), to_string(7), Predicate::Op::EQUALS);
    checkJoinEstimateCosts(jo, equalsJoinNode);
}
/**
 * Verify that the join cardinalities produced by estimateJoinCardinality()
 * are reasonable
 */
TEST_F(JoinOptimizerTest, EstimateJoinCardinality) {
    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    Parser p;
    shared_ptr<JoinOptimizer> j = make_shared<JoinOptimizer>(p.generateLogicalPlan(tid,
        "SELECT * FROM " + _tableName2 + " t1, " + _tableName2
        + " t2 WHERE t1.c8 = t2.c7;"),
        vector<shared_ptr<LogicalJoinNode>>());

    double cardinality;
    cardinality = j->estimateJoinCardinality(make_shared<LogicalJoinNode>("t1", "t2",
        "c" + 3, "c" + 4,
        Predicate::Op::EQUALS), _stats1->estimateTableCardinality(0.8),
        _stats2->estimateTableCardinality(0.2), true, false, TableStats::getStatsMap());

    // On a primary key, the cardinality is well-defined and exact (should
    // be size of fk table)
    // BUT we had a bug in lab 4 in 2009 that suggested should be size of pk
    // table, so accept either
    EXPECT_TRUE(cardinality == 800 || cardinality == 2000);

    cardinality = j->estimateJoinCardinality(make_shared<LogicalJoinNode>("t1", "t2",
        "c" + 3, "c" + 4,
        Predicate::Op::EQUALS), _stats1->estimateTableCardinality(0.8),
        _stats2->estimateTableCardinality(0.2), false, true, TableStats::getStatsMap());

    EXPECT_TRUE(cardinality == 800 || cardinality == 2000);
}
///**
// * Determine whether the orderJoins implementation is doing a reasonable job
// * of ordering joins, and not taking an unreasonable amount of time to do so
// */
//TEST_F(JoinOptimizerTest, OrderJoinsTest) {
//    // This test is intended to approximate the join described in the
//        // "Query Planning" section of 2009 Quiz 1,
//        // though with some minor variation due to limitations in simpledb
//        // and to only test your integer-heuristic code rather than
//        // string-heuristic code.
//
//    const static int IO_COST = 101;
//
//    // Create a whole bunch of variables that we're going to use
//    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
//    shared_ptr<JoinOptimizer> j;
//    vector<shared_ptr<LogicalJoinNode>> result;
//    vector<shared_ptr<LogicalJoinNode>> nodes;
//    map<string, shared_ptr<TableStats>> stats;
//    map<string, double> filterSelectivities;
//
//    // Create all of the tables, and add them to the catalog
//    vector<vector<int>> empTuples;
//    shared_ptr<HeapFile> emp = SystemTestUtil::createRandomHeapFile(6, 100000, map<int,int>(),
//        empTuples, string("c"));
//    Database::getCatalog()->addTable(emp, "emp");
//
//    vector<vector<int>> deptTuples;
//    shared_ptr<HeapFile> dept = SystemTestUtil::createRandomHeapFile(3, 1000, map<int,int>(),
//        deptTuples, string("c"));
//    Database::getCatalog()->addTable(dept, "dept");
//
//    vector<vector<int>> hobbyTuples;
//    shared_ptr<HeapFile> hobby = SystemTestUtil::createRandomHeapFile(6, 1000, map<int, int>(),
//        hobbyTuples, string("c"));
//    Database::getCatalog()->addTable(hobby, "hobby");
//
//    vector<vector<int>> hobbiesTuples;
//    shared_ptr<HeapFile> hobbies = SystemTestUtil::createRandomHeapFile(2, 200000, map<int, int>(),
//        hobbiesTuples, string("c"));
//    Database::getCatalog()->addTable(hobbies, "hobbies");
//
//    // Get TableStats objects for each of the tables that we just generated.
//    stats["emp"] = make_shared<TableStats>(
//        Database::getCatalog()->getTableId("emp"), IO_COST);
//    stats["dept"] =
//        make_shared<TableStats>(Database::getCatalog()->getTableId("dept"),
//            IO_COST);
//    stats["hobby"] =
//        make_shared<TableStats>(Database::getCatalog()->getTableId("hobby"),
//            IO_COST);
//    stats["hobbies"] =
//        make_shared<TableStats>(Database::getCatalog()->getTableId("hobbies"),
//            IO_COST);
//
//    // Note that your code shouldn't re-compute selectivities.
//    // If you get statistics numbers, even if they're wrong (which they are
//    // here
//    // because the data is random), you should use the numbers that you are
//    // given.
//    // Re-computing them at runtime is generally too expensive for complex
//    // queries.
//    filterSelectivities["emp"] = 0.1;
//    filterSelectivities["dept"] = 1.0;
//    filterSelectivities["hobby"] = 1.0;
//    filterSelectivities["hobbies"] = 1.0;
//
//    // Note that there's no particular guarantee that the LogicalJoinNode's
//    // will be in
//    // the same order as they were written in the query.
//    // They just have to be in an order that uses the same operators and
//    // semantically means the same thing.
//    nodes.push_back(make_shared<LogicalJoinNode>("hobbies", "hobby", "c1", "c0",
//        Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("emp", "dept", "c1", "c0",
//        Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("emp", "hobbies", "c2", "c0",
//        Predicate::Op::EQUALS));
//    Parser p;
//    j = make_shared<JoinOptimizer>(
//        p.generateLogicalPlan(
//            tid,
//            "SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND e.c3 < 1000;"),
//        nodes);
//
//    // Set the last boolean here to 'true' in order to have orderJoins()
//    // print out its logic
//    result = j->orderJoins(stats, filterSelectivities, false);
//
//    // There are only three join nodes; if you're only re-ordering the join
//    // nodes,
//    // you shouldn't end up with more than you started with
//    EXPECT_EQ(result.size(), nodes.size());
//
//    // There were a number of ways to do the query in this quiz, reasonably
//    // well;
//    // we're just doing a heuristics-based optimizer, so, only ignore the
//    // really
//    // bad case where "hobbies" is the outermost node in the left-deep tree.
//    EXPECT_TRUE("hobbies" != result[0]->t1Alias);
//
//    // Also check for some of the other silly cases, like forcing a cross
//    // join by
//    // "hobbies" only being at the two extremes, or "hobbies" being the
//    // outermost table.
//    EXPECT_FALSE(result[2]->t2Alias == "hobbies"
//        && (result[0]->t1Alias == "hobbies") || result[0]->t2Alias == "hobbies");
//   
//}
///**
// * Test a much-larger join ordering, to confirm that it executes in a
// * reasonable amount of time
// */
//TEST_F(JoinOptimizerTest, BigOrderJoinsTest) {
//    static const int timeout = 60000;
//    static const int IO_COST = 103;
//    clock_t start, end;
//    start = clock();
//    shared_ptr<JoinOptimizer> j;
//    map<string, shared_ptr<TableStats>> stats;
//    vector<shared_ptr<LogicalJoinNode>> result;
//    vector<shared_ptr<LogicalJoinNode>> nodes;
//    map<string, double> filterSelectivities;
//    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
//
//    // Create a large set of tables, and add tuples to the tables
//    vector<vector<int>> smallHeapFileTuples;
//    shared_ptr<HeapFile> smallHeapFileA = SystemTestUtil::createRandomHeapFile(2, 100,
//        INT_MAX, map<int, int>(), smallHeapFileTuples, string("c"));
//    shared_ptr<HeapFile> smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//
//    vector<vector<int>> bigHeapFileTuples;
//    for (int i = 0; i < 100000; i++) {
//        bigHeapFileTuples.push_back(smallHeapFileTuples.at(i % 100));
//    }
//    shared_ptr<HeapFile> bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2,
//        "c");
//    Database::getCatalog()->addTable(bigHeapFile, "bigTable");
//
//    // Add the tables to the database
//    Database::getCatalog()->addTable(bigHeapFile, "bigTable");
//    Database::getCatalog()->addTable(smallHeapFileA, "a");
//    Database::getCatalog()->addTable(smallHeapFileB, "b");
//    Database::getCatalog()->addTable(smallHeapFileC, "c");
//    Database::getCatalog()->addTable(smallHeapFileD, "d");
//    Database::getCatalog()->addTable(smallHeapFileE, "e");
//    Database::getCatalog()->addTable(smallHeapFileF, "f");
//    Database::getCatalog()->addTable(smallHeapFileG, "g");
//    Database::getCatalog()->addTable(smallHeapFileH, "h");
//    Database::getCatalog()->addTable(smallHeapFileI, "i");
//    Database::getCatalog()->addTable(smallHeapFileJ, "j");
//    Database::getCatalog()->addTable(smallHeapFileK, "k");
//    Database::getCatalog()->addTable(smallHeapFileL, "l");
//    Database::getCatalog()->addTable(smallHeapFileM, "m");
//    Database::getCatalog()->addTable(smallHeapFileN, "n");
//
//    // Come up with join statistics for the tables
//    stats.emplace("bigTable", new TableStats(bigHeapFile->getId(), IO_COST));
//    stats.emplace("a", new TableStats(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("b", new TableStats(smallHeapFileB->getId(), IO_COST));
//    stats.emplace("c", new TableStats(smallHeapFileC->getId(), IO_COST));
//    stats.emplace("d", new TableStats(smallHeapFileD->getId(), IO_COST));
//    stats.emplace("e", new TableStats(smallHeapFileE->getId(), IO_COST));
//    stats.emplace("f", new TableStats(smallHeapFileF->getId(), IO_COST));
//    stats.emplace("g", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("h", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("i", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("j", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("k", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("l", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("m", new TableStats(smallHeapFileG->getId(), IO_COST));
//    stats.emplace("n", new TableStats(smallHeapFileG->getId(), IO_COST));
//
//    // Put in some filter selectivities
//    filterSelectivities.emplace("bigTable", 1.0);
//    filterSelectivities.emplace("a", 1.0);
//    filterSelectivities.emplace("b", 1.0);
//    filterSelectivities.emplace("c", 1.0);
//    filterSelectivities.emplace("d", 1.0);
//    filterSelectivities.emplace("e", 1.0);
//    filterSelectivities.emplace("f", 1.0);
//    filterSelectivities.emplace("g", 1.0);
//    filterSelectivities.emplace("h", 1.0);
//    filterSelectivities.emplace("i", 1.0);
//    filterSelectivities.emplace("j", 1.0);
//    filterSelectivities.emplace("k", 1.0);
//    filterSelectivities.emplace("l", 1.0);
//    filterSelectivities.emplace("m", 1.0);
//    filterSelectivities.emplace("n", 1.0);
//
//    // Add the nodes to a collection for a query plan
//    nodes.push_back(make_shared<LogicalJoinNode>("a", "b", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("b", "c", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("c", "d", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("d", "e", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("e", "f", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("f", "g", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("g", "h", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("h", "i", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("i", "j", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("j", "k", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("k", "l", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("l", "m", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("m", "n", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("n", "bigTable", "c0", "c0",
//        Predicate::Op::EQUALS));
//
//    // Make sure we don't give the nodes to the optimizer in a nice order
//    random_shuffle(nodes.begin(), nodes.end());
//    Parser p;
//    j = make_shared<JoinOptimizer>(
//        p.generateLogicalPlan(
//            tid,
//            "SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;"),
//        nodes);
//
//    // Set the last boolean here to 'true' in order to have orderJoins()
//    // print out its logic
//    result = j->orderJoins(stats, filterSelectivities, false);
//
//    // If you're only re-ordering the join nodes,
//    // you shouldn't end up with more than you started with
//    EXPECT_EQ(result.size(), nodes.size());
//
//    // Make sure that "bigTable" is the outermost table in the join
//    EXPECT_TRUE(result[result.size() - 1]->t2Alias == "bigTable");
//    end = clock();
//    int cost = end - start;
//    EXPECT_LE(cost, timeout);
//}
///**
// * Test a join ordering with an inequality, to make sure the inequality gets
// * put as the outermost join
// */
//TEST_F(JoinOptimizerTest, NonequalityOrderJoinsTest) {
//    static const int IO_COST = 103;
//
//    shared_ptr<JoinOptimizer> j;
//    map<string, shared_ptr<TableStats>> stats;
//    vector<shared_ptr<LogicalJoinNode>> result;
//    vector<shared_ptr<LogicalJoinNode>> nodes;
//    map<string, double> filterSelectivities;
//    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
//
//    // Create a large set of tables, and add tuples to the tables
//    vector<vector<int>> smallHeapFileTuples;
//    shared_ptr<HeapFile> smallHeapFileA = SystemTestUtil::createRandomHeapFile(2, 100,
//        INT_MAX, map<int, int>(), smallHeapFileTuples, string("c"));
//    shared_ptr<HeapFile> smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//    shared_ptr<HeapFile> smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples,
//        2, "c");
//
//    // Add the tables to the database
//    Database::getCatalog()->addTable(smallHeapFileA, "a");
//    Database::getCatalog()->addTable(smallHeapFileB, "b");
//    Database::getCatalog()->addTable(smallHeapFileC, "c");
//    Database::getCatalog()->addTable(smallHeapFileD, "d");
//    Database::getCatalog()->addTable(smallHeapFileE, "e");
//    Database::getCatalog()->addTable(smallHeapFileF, "f");
//    Database::getCatalog()->addTable(smallHeapFileG, "g");
//    Database::getCatalog()->addTable(smallHeapFileH, "h");
//    Database::getCatalog()->addTable(smallHeapFileI, "i");
//
//    // Come up with join statistics for the tables
//    stats.emplace("a", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("b", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("c", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("d", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("e", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("f", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("g", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("h", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//    stats.emplace("i", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//
//    // Put in some filter selectivities
//    filterSelectivities.emplace("a", 1.0);
//    filterSelectivities.emplace("b", 1.0);
//    filterSelectivities.emplace("c", 1.0);
//    filterSelectivities.emplace("d", 1.0);
//    filterSelectivities.emplace("e", 1.0);
//    filterSelectivities.emplace("f", 1.0);
//    filterSelectivities.emplace("g", 1.0);
//    filterSelectivities.emplace("h", 1.0);
//    filterSelectivities.emplace("i", 1.0);
//
//    // Add the nodes to a collection for a query plan
//    nodes.push_back(make_shared<LogicalJoinNode>("a", "b", "c1", "c1",
//        Predicate::Op::LESS_THAN));
//    nodes.push_back(make_shared<LogicalJoinNode>("b", "c", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("c", "d", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("d", "e", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("e", "f", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("f", "g", "c0", "c0", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("g", "h", "c1", "c1", Predicate::Op::EQUALS));
//    nodes.push_back(make_shared<LogicalJoinNode>("h", "i", "c0", "c0", Predicate::Op::EQUALS));
//
//    Parser p;
//    // Run the optimizer; see what results we get back
//    j = make_shared<JoinOptimizer>(
//        p.generateLogicalPlan(
//            tid,
//            "SELECT COUNT(a.c0) FROM a, b, c, d,e,f,g,h,i WHERE a.c1 < b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0;"),
//        nodes);
//
//    // Set the last boolean here to 'true' in order to have orderJoins()
//    // print out its logic
//    result = j->orderJoins(stats, filterSelectivities, false);
//
//    // If you're only re-ordering the join nodes,
//    // you shouldn't end up with more than you started with
//    EXPECT_EQ(result.size(), nodes.size());
//
//    // Make sure that "a" is the outermost table in the join
//    EXPECT_TRUE(result[result.size() - 1]->t2Alias == "a"
//        || result[result.size() - 1]->t1Alias == "a");
//}