#pragma once
#include"SimpleDbTestBase.h"
#include"HeapFile.h"
#include"HeapPage.h"
#include"SystemTestUtil.h"
using namespace Simpledb;
class HeapFileReadTest :public SimpleDbTestBase {
protected:
	void SetUp()override {
		map<int, int> tempMap;
		vector<vector<int>> tempVec;
		_hf = SystemTestUtil::createRandomHeapFile(2, 20, tempMap, tempVec);
		_td = Utility::getTupleDesc(2);
		_tid = make_shared<TransactionId>();
	}
	void TearDown() override {
		Database::getBufferPool()->transactionComplete(*_tid);
	}

	shared_ptr<HeapFile> _hf;
	shared_ptr<TransactionId> _tid;
	shared_ptr<TupleDesc> _td;
};

TEST_F(HeapFileReadTest, GetId) {
	size_t id = _hf->getId();

	// NOTE: the value could be anything. test determinism, at least.
	EXPECT_EQ(id, _hf->getId());
	EXPECT_EQ(id, _hf->getId());
	
	shared_ptr<HeapFile> other = SystemTestUtil::
		createRandomHeapFile(1, 1, map<int,int>(), vector<vector<int>>());
	EXPECT_TRUE(id != other->getId());
}
TEST_F(HeapFileReadTest, GetTupleDesc) {
	EXPECT_TRUE(_td->equals(*_hf->getTupleDesc()));
}
TEST_F(HeapFileReadTest, NumPages) {
	EXPECT_EQ(1, _hf->numPages());
}
TEST_F(HeapFileReadTest, ReadPage) {
	shared_ptr<HeapPageId> pid = make_shared<HeapPageId>(_hf->getId(), 0);
	shared_ptr<HeapPage> page = dynamic_pointer_cast<HeapPage>(_hf->readPage(pid));

	// NOTE: we try not to dig too deeply into the Page API here; we
	// rely on HeapPageTest for that. perform some basic checks.
	EXPECT_EQ(484, page->getNumEmptySlots());
	EXPECT_TRUE(page->isSlotUsed(1));
	EXPECT_FALSE(page->isSlotUsed(20));
}
TEST_F(HeapFileReadTest, TestIteratorBasic) {
	shared_ptr<HeapFile> smallFile = SystemTestUtil::
		createRandomHeapFile(2, 3, map<int,int>(),
		vector<vector<int>>());

	shared_ptr<DbFileIterator> it = smallFile->iterator(_tid);
	// Not open yet
	EXPECT_FALSE(it->hasNext());
	EXPECT_THROW(it->next(), runtime_error);

	it->open();
	int count = 0;
	while (it->hasNext()) {
		EXPECT_NO_THROW(it->next());
		count += 1;
	}
	EXPECT_EQ(3, count);
	it->close();
}

TEST_F(HeapFileReadTest, TestIteratorClose) {
	// make more than 1 page. Previous closed iterator would start fetching
	// from page 1.
	shared_ptr<HeapFile> twoPageFile = SystemTestUtil::
		createRandomHeapFile(2, 520, map<int, int>(), vector<vector<int>>());

	shared_ptr<DbFileIterator> it = twoPageFile->iterator(_tid);
	it->open();
	EXPECT_TRUE(it->hasNext());
	it->close();
	EXPECT_THROW(it->next(), runtime_error);
	// close twice is harmless
	it->close();
}