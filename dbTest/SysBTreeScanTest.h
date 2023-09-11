#pragma once
#include"SimpleDbTestBase.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include"Utility.h"
#include"BTreeScan.h"
#include"SystemTestUtil.h"
#include<random>
#include<vector>
using namespace Simpledb;

/**
 * Dumps the contents of a table.
 * args[1] is the number of columns.  E.g., if it's 5, then BTreeScanTest will end
 * up dumping the contents of f4.0.txt.
 */
class SysBTreeScanTest : public SimpleDbTestBase {
protected:
    std::random_device _randomDevice;
    std::default_random_engine _randomEngine;
    struct TupleComparator
    {
        TupleComparator(int keyField) : _keyField(keyField) {}
        bool operator()(const vector<int>& t1, const vector<int>& t2) const {
            if (t1[_keyField] < t2[_keyField])return true;
            return false;
        }
    private:
        int _keyField;
    };

    /** Counts the number of readPage operations. */
    class InstrumentedBTreeFile : public BTreeFile {
    public:
        int readCount = 0;

        InstrumentedBTreeFile(shared_ptr<File> f, int keyField, shared_ptr<TupleDesc> td)
            :BTreeFile(f, keyField, td) {}
        shared_ptr<Page> readPage(shared_ptr<PageId> pid) {
            readCount += 1;
            return BTreeFile::readPage(pid);
        }
    };

	void validateScan(const vector<int>& columnSizes, const vector<int>& rowSizes) {
        shared_ptr<TransactionId> tid = make_shared<TransactionId>();
        
        _randomEngine.seed(_randomDevice());
        for (int columns : columnSizes) {
            std::uniform_int_distribution<int> uniform_dist(0, columns - 1);
            int keyField = uniform_dist(_randomEngine);
            for (int rows : rowSizes) {
                vector<vector<int>> tuples;
                shared_ptr<BTreeFile> f = BTreeUtility::createRandomBTreeFile(columns, rows, map<int,int>(), tuples, keyField);
                shared_ptr<BTreeScan> scan = make_shared<BTreeScan>(tid, f->getId(), "table", nullptr);
                SystemTestUtil::matchTuples(scan, tuples);
                Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
            }
        }
        Database::getBufferPool()->transactionComplete(tid);
	}
};

/** Scan 1-4 columns. */
TEST_F(SysBTreeScanTest, TestSmall) {
    _randomEngine.seed(_randomDevice());
    std::uniform_int_distribution<int> uniform_dist(0, 4096 - 1);
    vector<int> columnSizes {1, 2, 3, 4};
    vector<int> rowSizes = {0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + uniform_dist(_randomEngine)};
    validateScan(columnSizes, rowSizes);
}

/** Test that rewinding a BTreeScan iterator works. */
TEST_F(SysBTreeScanTest, TestRewind) {
    _randomEngine.seed(_randomDevice());
    std::uniform_int_distribution<int> uniform_dist(0, 1);
    vector<vector<int>> tuples;
    int keyField = uniform_dist(_randomEngine);
    shared_ptr<BTreeFile> f = BTreeUtility::createRandomBTreeFile(2, 1000, map<int, int>(), tuples, keyField);
    TupleComparator tupleComparator(keyField);
    sort(tuples.begin(), tuples.end(), tupleComparator);

    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    shared_ptr<BTreeScan> scan = make_shared<BTreeScan>(tid, f->getId(), "table", nullptr);
    scan->open();
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(tuples[i].begin(), tuples[i].end(), tmp.begin(), tmp.end()));
    }

    scan->rewind();
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(tuples[i].begin(), tuples[i].end(), tmp.begin(), tmp.end()));
    }
    scan->close();
    Database::getBufferPool()->transactionComplete(tid);
}

/** Test that rewinding a BTreeScan iterator works with predicates. */
TEST_F(SysBTreeScanTest, TestRewindPredicates) {
    // Create the table
    _randomEngine.seed(_randomDevice());
    std::uniform_int_distribution<int> uniform_dist(0, 2);
    vector<vector<int>> tuples;
    int keyField = uniform_dist(_randomEngine);
    shared_ptr<BTreeFile> f = BTreeUtility::createRandomBTreeFile(3, 1000, map<int, int>(), tuples, keyField);
    TupleComparator tupleComparator(keyField);
    sort(tuples.begin(), tuples.end(), tupleComparator);

    std::uniform_int_distribution<int> randomInt(0, BTreeUtility::MAX_RAND_VALUE - 1);
    // EQUALS
    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    vector<vector<int>> tuplesFiltered;
    shared_ptr<IndexPredicate> ipred = make_shared<IndexPredicate>(
        Predicate::Op::EQUALS, make_shared<IntField>(randomInt(_randomDevice)));
    for (auto& tuple : tuples) {
        if (tuple[keyField] == dynamic_pointer_cast<IntField>(ipred->getField())->getValue()) {
            tuplesFiltered.push_back(tuple);
        }
    }

    shared_ptr<BTreeScan> scan = make_shared<BTreeScan>(tid, f->getId(), "table", ipred);
    scan->open();
    for (auto& item : tuplesFiltered) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(item.begin(), item.end(), tmp.begin(), tmp.end()));
    }

    scan->rewind();
    for (auto& item : tuplesFiltered) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(item.begin(), item.end(), tmp.begin(), tmp.end()));
    }
    scan->close();

    // LESS_THAN
    tuplesFiltered.clear();
    ipred = make_shared<IndexPredicate>(Predicate::Op::LESS_THAN, make_shared<IntField>(randomInt(_randomDevice)));
    for (auto& tuple : tuples) {
        if (tuple[keyField] < dynamic_pointer_cast<IntField>(ipred->getField())->getValue()) {
            tuplesFiltered.push_back(tuple);
        }
    }

    scan = make_shared<BTreeScan>(tid, f->getId(), "table", ipred);
    scan->open();
    for (auto& item : tuplesFiltered) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(item.begin(), item.end(), tmp.begin(), tmp.end()));
    }

    scan->rewind();
    for (auto& item : tuplesFiltered) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(item.begin(), item.end(), tmp.begin(), tmp.end()));
    }
    scan->close();

    // GREATER_THAN
    tuplesFiltered.clear();
    ipred = make_shared<IndexPredicate>(Predicate::Op::GREATER_THAN_OR_EQ, make_shared<IntField>(randomInt(_randomDevice)));
    for (auto& tuple : tuples) {
        if (tuple[keyField] >= dynamic_pointer_cast<IntField>(ipred->getField())->getValue()) {
            tuplesFiltered.push_back(tuple);
        }
    }

    scan = make_shared<BTreeScan>(tid, f->getId(), "table", ipred);
    scan->open();
    for (auto& item : tuplesFiltered) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(item.begin(), item.end(), tmp.begin(), tmp.end()));
    }

    scan->rewind();
    for (auto& item : tuplesFiltered) {
        EXPECT_TRUE(scan->hasNext());
        Tuple* t = scan->next();
        vector<int> tmp = SystemTestUtil::tupleToVector(*t);
        EXPECT_TRUE(equal(item.begin(), item.end(), tmp.begin(), tmp.end()));
    }
    scan->close();
    Database::getBufferPool()->transactionComplete(tid);
}

/** Test that scanning the BTree for predicates does not read all the pages */
TEST_F(SysBTreeScanTest, TestReadPage) {
    // Create the table
    int LEAF_PAGES = 30;

    vector<vector<int>> tuples;
    int keyField = 0;
    shared_ptr<BTreeFile> f = BTreeUtility::createBTreeFile(2, LEAF_PAGES * 502, map<int,int>(), tuples, keyField);
    TupleComparator tupleComparator(keyField);
    sort(tuples.begin(), tuples.end(), tupleComparator);
    shared_ptr<TupleDesc> td = Utility::getTupleDesc(2);
    shared_ptr<InstrumentedBTreeFile> table = make_shared<InstrumentedBTreeFile>(f->getFile(), keyField, td);
    Database::getCatalog()->addTable(table, SystemTestUtil::getUUID());
    std::uniform_int_distribution<int> randomInt(0, LEAF_PAGES * 502 - 1);
    // EQUALS
    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    vector<vector<int>> tuplesFiltered;
    shared_ptr<IndexPredicate> ipred = make_shared<IndexPredicate>(Predicate::Op::EQUALS, make_shared<IntField>(randomInt(_randomDevice)));
    for (auto& tuple : tuples) {
        if (tuple[keyField] == dynamic_pointer_cast<IntField>(ipred->getField())->getValue()) {
            tuplesFiltered.push_back(tuple);
        }
    }

    Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
    table->readCount = 0;
    shared_ptr<BTreeScan> scan = make_shared<BTreeScan>(tid, f->getId(), "table", ipred);
    SystemTestUtil::matchTuples(scan, tuplesFiltered);
    // root pointer page + root + leaf page (possibly 2 leaf pages)
    EXPECT_TRUE(table->readCount == 3 || table->readCount == 4);

    // LESS_THAN
    tuplesFiltered.clear();
    ipred = make_shared<IndexPredicate>(Predicate::Op::LESS_THAN, make_shared<IntField>(randomInt(_randomDevice)));
    for (auto& tuple : tuples) {
        if (tuple[keyField] < dynamic_pointer_cast<IntField>(ipred->getField())->getValue()) {
            tuplesFiltered.push_back(tuple);
        }
    }

    Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
    table->readCount = 0;
    scan = make_shared<BTreeScan>(tid, f->getId(), "table", ipred);
    SystemTestUtil::matchTuples(scan, tuplesFiltered);
    // root pointer page + root + leaf pages
    int leafPageCount = tuplesFiltered.size() / 502;
    if (leafPageCount < LEAF_PAGES)
        leafPageCount++; // +1 for next key locking
    EXPECT_EQ(leafPageCount + 2, table->readCount);

    // GREATER_THAN
    tuplesFiltered.clear();
    ipred = make_shared<IndexPredicate>(Predicate::Op::GREATER_THAN_OR_EQ, make_shared<IntField>(randomInt(_randomDevice)));
    for (auto& tuple : tuples) {
        if (tuple[keyField] >= dynamic_pointer_cast<IntField>(ipred->getField())->getValue()) {
            tuplesFiltered.push_back(tuple);
        }
    }

    Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
    table->readCount = 0;
    scan = make_shared<BTreeScan>(tid, f->getId(), "table", ipred);
    SystemTestUtil::matchTuples(scan, tuplesFiltered);
    // root pointer page + root + leaf pages
    leafPageCount = tuplesFiltered.size() / 502;
    if (leafPageCount < LEAF_PAGES)
        leafPageCount++; // +1 for next key locking
    EXPECT_EQ(leafPageCount + 2, table->readCount);

    Database::getBufferPool()->transactionComplete(tid);
}