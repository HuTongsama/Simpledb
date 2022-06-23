#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"HeapFile.h"
#include<random>
#include<vector>
using namespace std;
using namespace Simpledb;
class InstrumentedHeapFile : public HeapFile {
public:
    InstrumentedHeapFile(shared_ptr<File> f, shared_ptr<TupleDesc> td) :
        HeapFile(f, td), readCount(0) {}
    shared_ptr<Page> readPage(shared_ptr<PageId> pid)override {
        readCount++;
        return HeapFile::readPage(pid);
    }
    int readCount;
};
class ScanTest : public SimpleDbTestBase {
protected:
    ScanTest() {
      _r = default_random_engine(time(0));
    }
	void validateScan(const vector<int>& columnSizes,const vector<int>& rowSizes) {
        for (int columns : columnSizes) {
            for (int rows : rowSizes) {
                vector<vector<int>> tuples;;
                shared_ptr<HeapFile> f = SystemTestUtil::createRandomHeapFile(columns, rows, map<int, int>(), tuples);
                SystemTestUtil::matchTuples(f, tuples);
                Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
            }
        }
	}

	default_random_engine _r;
};
//TEST_F(ScanTest, TestSmall) {
//    vector<int> columnSizes{ 1, 2, 3, 4 };
//    vector<int> rowSizes = {0, 1, 2, 511, 512, 513, 1023, 1024, 1025, int(4096 + _r()%4096 + 1)};
//    validateScan(columnSizes, rowSizes);
//}
//TEST_F(ScanTest, TestRewind) {
//    vector<vector<int>> tuples;
//    shared_ptr<HeapFile> f = SystemTestUtil::createRandomHeapFile(2, 1000, map<int, int>(), tuples);
//
//    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
//    shared_ptr<SeqScan> scan = make_shared<SeqScan>(tid, f->getId(), "table");
//    scan->open();
//    for (int i = 0; i < 100; ++i) {
//        EXPECT_TRUE(scan->hasNext());
//        Tuple& t = scan->next();
//        EXPECT_TRUE(equal(tuples[i].begin(), tuples[i].end(), SystemTestUtil::tupleToVector(t).begin()));
//    }
//
//    scan->rewind();
//    for (int i = 0; i < 100; ++i) {
//        EXPECT_TRUE(scan->hasNext());
//        Tuple& t = scan->next();
//        EXPECT_TRUE(equal(tuples[i].begin(), tuples[i].end(), SystemTestUtil::tupleToVector(t).begin()));
//    }
//    scan->close();
//    Database::getBufferPool()->transactionComplete(tid);
//}
TEST_F(ScanTest, TestCache) {
    int PAGES = 30;
    vector<vector<int>> tuples;
    shared_ptr<File> f = SystemTestUtil::createRandomHeapFileUnopened(1, 992 * PAGES, 1000, map<int, int>(), tuples);
    shared_ptr<TupleDesc> td = Utility::getTupleDesc(1);
    shared_ptr<InstrumentedHeapFile> table = make_shared<InstrumentedHeapFile>(f, td);
    Database::getCatalog()->addTable(table, SystemTestUtil::getUUID());

    // Scan the table once
    SystemTestUtil::matchTuples(table, tuples);
    EXPECT_EQ(PAGES, table->readCount);
    table->readCount = 0;

    // Scan the table again: all pages should be cached
    SystemTestUtil::matchTuples(table, tuples);
    EXPECT_EQ(0, table->readCount);
}
TEST_F(ScanTest, TestTupleDesc) {
    vector<vector<int>> tuples;
    string test("test");
    shared_ptr<HeapFile> f = SystemTestUtil::createRandomHeapFile(2, 1000, map<int, int>(), tuples, test);

    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    string prefix = "table_alias";
    shared_ptr<SeqScan> scan = make_shared<SeqScan>(tid, f->getId(), prefix);

    shared_ptr<TupleDesc> original = f->getTupleDesc();
    shared_ptr<TupleDesc> prefixed = scan->getTupleDesc();
    EXPECT_EQ(prefix, scan->getAlias());

    // Sanity check the number of fields
    EXPECT_EQ(original->numFields(), prefixed->numFields());

    // Check each field for the appropriate tableAlias. prefix
    for (int i = 0; i < original->numFields(); i++) {
        EXPECT_EQ(prefix + "." + original->getFieldName(i), prefixed->getFieldName(i));
    }
}