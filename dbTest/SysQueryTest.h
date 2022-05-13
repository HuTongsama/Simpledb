#pragma once
#include"SimpleDbTestBase.h"
#include"JoinOptimizer.h"
#include"HeapFile.h"
#include"HeapFileEncoder.h"
#include"Utility.h"
#include"SystemTestUtil.h"
#include"Transaction.h"
#include"Parser.h"
using namespace Simpledb;
class SysQueryTest : public SimpleDbTestBase {
protected:
	/**
	  * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	  * @param tuples Tuples to create a HeapFile from
	  * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	  * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	  * @return a new HeapFile containing the specified tuples
      * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	shared_ptr<HeapFile> createDuplicateHeapFile(vector<vector<int>>& tuples, int columns, string& colPrefix) {
		shared_ptr<File> temp = File::createTempFile();
		temp->deleteOnExit();
		HeapFileEncoder::convert(tuples, *temp, BufferPool::getPageSize(), columns);
		return Utility::openHeapFile(columns, colPrefix, temp);
	}
};

TEST_F(SysQueryTest, QueryTest) {
	// This test is intended to approximate the join described in the
	// "Query Planning" section of 2009 Quiz 1,
	// though with some minor variation due to limitations in simpledb
	// and to only test your integer-heuristic code rather than
	// string-heuristic code.		
	static const int IO_COST = 101;
#ifdef _DEBUG
	static const int timeout = 25000;
#else
	static const int timeout = 20000;
#endif // _DEBUG

	clock_t start, end;
	start = clock();
	// Create all of the tables, and add them to the catalog
	vector<vector<int>> empTuples;
	shared_ptr<HeapFile> emp = SystemTestUtil::createRandomHeapFile(6, 100000, map<int,int>(), empTuples, string("c"));
	Database::getCatalog()->addTable(emp, "emp");

	vector<vector<int>> deptTuples;
	shared_ptr<HeapFile> dept = SystemTestUtil::createRandomHeapFile(3, 1000, map<int, int>(), deptTuples, string("c"));
	Database::getCatalog()->addTable(dept, "dept");

	vector<vector<int>> hobbyTuples;
	shared_ptr<HeapFile> hobby = SystemTestUtil::createRandomHeapFile(6, 1000, map<int, int>(), hobbyTuples, string("c"));
	Database::getCatalog()->addTable(hobby, "hobby");

	vector<vector<int>> hobbiesTuples;
	shared_ptr<HeapFile> hobbies = SystemTestUtil::createRandomHeapFile(2, 200000, map<int,int>(), hobbiesTuples, string("c"));
	Database::getCatalog()->addTable(hobbies, "hobbies");

	// Get TableStats objects for each of the tables that we just generated.
	TableStats::setTableStats("emp", make_shared<TableStats>(Database::getCatalog()->getTableId("emp"), IO_COST));
	TableStats::setTableStats("dept", make_shared<TableStats>(Database::getCatalog()->getTableId("dept"), IO_COST));
	TableStats::setTableStats("hobby", make_shared<TableStats>(Database::getCatalog()->getTableId("hobby"), IO_COST));
	TableStats::setTableStats("hobbies", make_shared<TableStats>(Database::getCatalog()->getTableId("hobbies"), IO_COST));

	shared_ptr<Transaction> t = make_shared<Transaction>();
	t->start();
	Parser p;
	p.setTransaction(t);
	// Each of these should return around 20,000
	// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
	// So, don't bother for now; future TODO.
	// Regardless, each of the following should be optimized to run quickly,
	// even though the worst case takes a very long time.
	p.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;");
	end = clock();
	int cost = end - start;
	EXPECT_LE(cost, timeout);
}

/*
  Build a large series of tables; then run the command-line query code and execute a query.
  The number of tables is large enough that the query will only succeed within the
  specified time if a join method faster than nested-loops join is available.
  The tables are also too big for a query to be successful if its query plan isn't reasonably efficient,
  and there are too many tables for a brute-force search of all possible query plans.
 */
 // Not required for Lab 4

//TEST_F(SysQueryTest, hashJoinTest) {
//	static const int IO_COST = 103;
//	static const int timeout = 20000;
//	clock_t start, end;
//	start = clock();
//	ConcurrentMap<string, shared_ptr<TableStats>> stats;
//
//	vector<vector<int>> smallHeapFileTuples ;
//	shared_ptr<HeapFile> smallHeapFileA = SystemTestUtil::createRandomHeapFile(2, 100, INT_MAX, map<int, int>(), smallHeapFileTuples, string("c"));
//	shared_ptr<HeapFile> smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//	shared_ptr<HeapFile> smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples, 2, string("c"));
//
//	vector<vector<int>> bigHeapFileTuples;
//	for (int i = 0; i < 1000; i++) {
//		bigHeapFileTuples.push_back(smallHeapFileTuples.at(i % 100));
//	}
//	shared_ptr<HeapFile> bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2, string("c"));
//	Database::getCatalog()->addTable(bigHeapFile, "bigTable");
//
//	// We want a bunch of these guys
//	Database::getCatalog()->addTable(smallHeapFileA, "a");
//	Database::getCatalog()->addTable(smallHeapFileB, "b");
//	Database::getCatalog()->addTable(smallHeapFileC, "c");
//	Database::getCatalog()->addTable(smallHeapFileD, "d");
//	Database::getCatalog()->addTable(smallHeapFileE, "e");
//	Database::getCatalog()->addTable(smallHeapFileF, "f");
//	Database::getCatalog()->addTable(smallHeapFileG, "g");
//	Database::getCatalog()->addTable(smallHeapFileH, "h");
//	Database::getCatalog()->addTable(smallHeapFileI, "i");
//	Database::getCatalog()->addTable(smallHeapFileJ, "j");
//	Database::getCatalog()->addTable(smallHeapFileK, "k");
//	Database::getCatalog()->addTable(smallHeapFileL, "l");
//	Database::getCatalog()->addTable(smallHeapFileM, "m");
//	Database::getCatalog()->addTable(smallHeapFileN, "n");
//
//	stats.setValue("bigTable", make_shared<TableStats>(bigHeapFile->getId(), IO_COST));
//	stats.setValue("a", make_shared<TableStats>(smallHeapFileA->getId(), IO_COST));
//	stats.setValue("b", make_shared<TableStats>(smallHeapFileB->getId(), IO_COST));
//	stats.setValue("c", make_shared<TableStats>(smallHeapFileC->getId(), IO_COST));
//	stats.setValue("d", make_shared<TableStats>(smallHeapFileD->getId(), IO_COST));
//	stats.setValue("e", make_shared<TableStats>(smallHeapFileE->getId(), IO_COST));
//	stats.setValue("f", make_shared<TableStats>(smallHeapFileF->getId(), IO_COST));
//	stats.setValue("g", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("h", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("i", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("j", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("k", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("l", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("m", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//	stats.setValue("n", make_shared<TableStats>(smallHeapFileG->getId(), IO_COST));
//
//	//Parser::setStatsMap(stats);
//	Parser p;
//	shared_ptr<Transaction> t = make_shared<Transaction>();
//	t->start();
//	//Parser::setTransaction(t);
//
//	// Each of these should return around 20,000
//	// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
//	// So, don't bother for now; future TODO.
//	// Regardless, each of the following should be optimized to run quickly,
//	// even though the worst case takes a very long time.
//	p.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;");
//	p.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1 AND bigTable.c0 = n.c0;");
//	p.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE k.c1 = l.c1 AND a.c1 = b.c1 AND f.c0 = g.c0 AND bigTable.c0 = n.c0 AND d.c0 = e.c0 AND c.c1 = d.c1 AND e.c1 = f.c1 AND i.c1 = j.c1 AND b.c0 = c.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND j.c0 = k.c0 AND m.c1 = n.c1 AND l.c0 = m.c0;");
//	
//	end = clock();
//	int cost = end - start;
//	EXPECT_LE(cost, timeout);
//}