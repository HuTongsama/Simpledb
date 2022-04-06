#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"SystemTestUtil.h"
#include"HeapFile.h"
class HeapFileWriteTest :
	public SimpleDbTestBase, public TestUtil::CreateHeapFile {
protected:
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		CreateHeapFile::SetUp();
		_tid = make_shared<TransactionId>();
	}
	void TearDown()override {
		Database::getBufferPool()->transactionComplete(_tid);
	}
	shared_ptr<TransactionId> _tid;
};

TEST_F(HeapFileWriteTest, AddTuple) {
    // we should be able to add 504 tuples on an empty page.
    for (int i = 0; i < 504; ++i) {
        _empty->insertTuple(_tid, Utility::getHeapTuple(i, 2));
        EXPECT_EQ(1, _empty->numPages());
    }

    // the next 504 additions should live on a new page
    for (int i = 0; i < 504; ++i) {
        _empty->insertTuple(_tid, Utility::getHeapTuple(i, 2));
        EXPECT_EQ(2, _empty->numPages());
    }

    // and one more, just for fun...
    _empty->insertTuple(_tid, Utility::getHeapTuple(0, 2));
    EXPECT_EQ(3, _empty->numPages());
}

TEST_F(HeapFileWriteTest, TestAlternateEmptyAndFullPagesThenIterate) {
    // Create HeapFile/Table
    shared_ptr<HeapFile> smallFile = SystemTestUtil::createRandomHeapFile(2, 3, map<int, int>(),
        vector<vector<int>>());
    // Grab table id
    size_t tableId = smallFile->getId();
    int tdSize = 8;
    int numTuples = (BufferPool::getPageSize() * 8) / (tdSize * 8 + 1);
    int headerSize = (int)ceil((double)numTuples / 8.0);
    // Leave these as all zeroes so this entire page is empty
    vector<unsigned char> empty(numTuples * 8 + headerSize, 0);
    // Since every bit is marked as used, every tuple should be used,
    // and all should be set to -1.
    vector<unsigned char> full(numTuples * 8 + headerSize, 0xff);


    // The first two pages and the fourth page are empty and should be skipped
    // while still continuing on to read the third and fifth page.
    // Hint: You can see this in HeapFile's iterator right after assigning the
    // next HeapPage's iterator and checking if it's empty (hasNext()), making
    // sure it moves onto the next page until hitting the final page.
    smallFile->writePage(make_shared<HeapPage>(make_shared<HeapPageId>(tableId, 0), empty));
    smallFile->writePage(make_shared<HeapPage>(make_shared<HeapPageId>(tableId, 1), empty));
    smallFile->writePage(make_shared<HeapPage>(make_shared<HeapPageId>(tableId, 2), full));
    smallFile->writePage(make_shared<HeapPage>(make_shared<HeapPageId>(tableId, 3), empty));
    smallFile->writePage(make_shared<HeapPage>(make_shared<HeapPageId>(tableId, 4), full));
    shared_ptr<DbFileIterator> it = smallFile->iterator(_tid);
    it->open();
    int count = 0;
    while (it->hasNext()) {
        EXPECT_NO_THROW(it->next(), runtime_error);
        count += 1;
    }
    // Since we have two full pages, we should see all of 2*numTuples.
    EXPECT_EQ(2 * numTuples, count);
    it->close();
}