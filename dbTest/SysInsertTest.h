#pragma once
#include"SimpleDbTestBase.h"
#include"Insert.h"
#include"TestUtil.h"
#include"SystemTestUtil.h"
using namespace Simpledb;
class SysInsertTest : public SimpleDbTestBase {
protected:
	void validateInsert(int columns, int sourceRows, int destinationRows) {
        vector<vector<int>> sourceTuples;
        shared_ptr<HeapFile> source = SystemTestUtil::createRandomHeapFile(
            columns, sourceRows, map<int,int>(), sourceTuples);
        EXPECT_EQ(sourceTuples.size(), sourceRows);
        vector<vector<int>> destinationTuples;
        shared_ptr<HeapFile> destination = SystemTestUtil::createRandomHeapFile(
            columns, destinationRows, map<int, int>(), destinationTuples);
        EXPECT_EQ(destinationTuples.size(), destinationRows);

        // Insert source into destination
        shared_ptr<TransactionId> tid = make_shared<TransactionId>();
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(tid, source->getId(), "");
        shared_ptr<Insert> insOp = make_shared<Insert>(tid, ss, destination->getId());

        //Query q = new Query(insOp, tid);
        insOp->open();
        bool hasResult = false;
        while (insOp->hasNext()) {
            Tuple* tup = insOp->next();
            EXPECT_FALSE(hasResult);
            hasResult = true;
            EXPECT_TRUE(SystemTestUtil::SINGLE_INT_DESCRIPTOR->equals(*tup->getTupleDesc()));
            EXPECT_EQ(sourceRows, dynamic_pointer_cast<IntField>(tup->getField(0))->getValue());
        }
        EXPECT_TRUE(hasResult);
        insOp->close();

        // As part of the same transaction, scan the table
        sourceTuples.insert(sourceTuples.end(), destinationTuples.begin(), destinationTuples.end());
        SystemTestUtil::matchTuples(destination, tid, sourceTuples);

        // As part of a different transaction, scan the table
        Database::getBufferPool()->transactionComplete(tid);
        Database::getBufferPool()->flushAllPages();
        SystemTestUtil::matchTuples(destination, sourceTuples);
	}
};

TEST_F(SysInsertTest, TestEmptyToEmpty) {
    validateInsert(3, 0, 0);
}

TEST_F(SysInsertTest, TestEmptyToOne) {
    validateInsert(8, 0, 1);
}

TEST_F(SysInsertTest, TestOneToEmpty) {
    validateInsert(3, 1, 0);
}

TEST_F(SysInsertTest, TestOneToOne) {
    validateInsert(1, 1, 1);
}