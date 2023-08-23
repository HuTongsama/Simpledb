#pragma once
#include"SimpleDbTestBase.h"
#include"HeapFile.h"
#include"Transaction.h"
#include"Utility.h"
#include"IntField.h"
#include"Insert.h"
#include"SystemTestUtil.h"
#include<windows.h>
#include<Psapi.h>
#pragma comment(lib,"psapi.lib")
using namespace Simpledb;
class EvictionTest :public SimpleDbTestBase {
protected:
#ifdef _DEBUG
    const static int MEMORY_LIMIT_IN_MB = 6;
#else
    const static int MEMORY_LIMIT_IN_MB = 5;
#endif // _DEBUG

	
	const static int BUFFER_PAGES = 16;

	static void insertRow(shared_ptr<HeapFile> f, shared_ptr<Transaction> t) {
	
        shared_ptr<TupleDesc> twoIntColumns = Utility::getTupleDesc(2);
        shared_ptr<Tuple> value = make_shared<Tuple>(twoIntColumns);
        value->setField(0, make_shared<IntField>(-42));
        value->setField(1, make_shared<IntField>(-43));
        shared_ptr<TupleIterator> insertRow =  make_shared<TupleIterator>(Utility::getTupleDesc(2), vector<shared_ptr<Tuple>>(1,value));

        // Insert the row
        shared_ptr<Insert> insert = make_shared<Insert>(t->getId(), insertRow, f->getId());
        insert->open();
        Tuple* result = insert->next();
        EXPECT_TRUE(SystemTestUtil::SINGLE_INT_DESCRIPTOR->equals(*result->getTupleDesc()));
        EXPECT_EQ(1, dynamic_pointer_cast<IntField>(result->getField(0))->getValue());
        EXPECT_FALSE(insert->hasNext());
        insert->close();
	}
    static bool findMagicTuple(shared_ptr<HeapFile> f, shared_ptr<Transaction> t) {
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(t->getId(), f->getId(), "");
        bool found = false;
        ss->open();
        while (ss->hasNext()) {
            Tuple* v = ss->next();
            int v0 = dynamic_pointer_cast<IntField>(v->getField(0))->getValue();
            int v1 = dynamic_pointer_cast<IntField>(v->getField(1))->getValue();
            if (v0 == -42 && v1 == -43) {
                EXPECT_FALSE(found);
                found = true;
            }
        }
        ss->close();
        return found;
    }
};

TEST_F(EvictionTest, TestHeapFileScanWithManyPages) {
    cout << "EvictionTest creating large table" << endl;
    shared_ptr<HeapFile> f = SystemTestUtil::createRandomHeapFile(2, 1024 * 500, map<int, int>(), vector<vector<int>>());
    cout << "EvictionTest scanning large table" << endl;
    Database::resetBufferPool(BUFFER_PAGES);
    HANDLE handle = GetCurrentProcess();
    PROCESS_MEMORY_COUNTERS pmc;
    GetProcessMemoryInfo(handle, &pmc, sizeof(pmc));
    long beginMem = pmc.WorkingSetSize;
    shared_ptr<TransactionId> tid =  make_shared<TransactionId>();
    shared_ptr<SeqScan> scan = make_shared<SeqScan>(tid, f->getId(), "");
    scan->open();
    while (scan->hasNext()) {
        scan->next();
    }
    cout << "EvictionTest scan complete, testing memory usage of scan" << endl;
    GetProcessMemoryInfo(handle, &pmc, sizeof(pmc));
    size_t endMem = pmc.WorkingSetSize;
    size_t memDiff = (endMem - beginMem) / (1024 * 1024);
    cout << "Memory: " << memDiff << endl;
    EXPECT_LE(memDiff, MEMORY_LIMIT_IN_MB);
}