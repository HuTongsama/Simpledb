#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"SystemTestUtil.h"
#include"HeapFile.h"
class BufferPoolWriteTest :
	public SimpleDbTestBase, public TestUtil::CreateHeapFile {
protected:
	class HeapFileDuplicates :public HeapFile {
	public:
		HeapFileDuplicates(shared_ptr<File> f, shared_ptr<TupleDesc> td, int duplicates)
			:HeapFile(f, td), _duplicates(duplicates) {}
		// this version of insertTuple inserts duplicate copies of the same tuple,
		// each on a new page
		vector<shared_ptr<Page>> insertTuple(shared_ptr<TransactionId> tid, shared_ptr<Tuple> t)override {
			vector<shared_ptr<Page>> dirtypages;
			for (int i = 0; i < _duplicates; i++) {
				// create a blank page
				/*BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(super.getFile(), true));
				byte[] emptyData = HeapPage.createEmptyPageData();
				bw.write(emptyData);
				bw.close();*/
				vector<unsigned char> emptyData = HeapPage::createEmptyPageData();
				shared_ptr<File> f = getFile();
				f->writeBytes(emptyData.data(), emptyData.size());
				shared_ptr<HeapPageId> pid = make_shared<HeapPageId>(HeapFile::getId(), HeapFile::numPages() - 1);
				shared_ptr<HeapPage> hp = dynamic_pointer_cast<HeapPage>(
					Database::getBufferPool()->getPage(tid, pid, Permissions::READ_WRITE));
				hp->insertTuple(t);
				dirtypages.push_back(hp);
			}
			return dirtypages;
		}
	private:
		int _duplicates;
	};

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

TEST_F(BufferPoolWriteTest, InsertTuple) {
	// we should be able to add 504 tuples on an empty page.
	for (int i = 0; i < 504; ++i) {
		shared_ptr<Tuple> t = Utility::getHeapTuple(i, 2);
		Database::getBufferPool()->insertTuple(_tid, _empty->getId(), t);
		shared_ptr<HeapPage> p = dynamic_pointer_cast<HeapPage>(
			Database::getBufferPool()->getPage(_tid, t->getRecordId()->getPageId(), Permissions::READ_ONLY));
		EXPECT_EQ(504 - i - 1, p->getNumEmptySlots());
	}

	// the next 504 additions should live on a new page
	for (int i = 0; i < 504; ++i) {
		shared_ptr<Tuple> t = Utility::getHeapTuple(i, 2);
		Database::getBufferPool()->insertTuple(_tid, _empty->getId(), t);
		shared_ptr<HeapPage> p = dynamic_pointer_cast<HeapPage>(
			Database::getBufferPool()->getPage(_tid, t->getRecordId()->getPageId(), Permissions::READ_ONLY));
		EXPECT_EQ(504 - i - 1, p->getNumEmptySlots());
	}
}

TEST_F(BufferPoolWriteTest, DeleteTuple) {
	// heap file should have ~10 pages
	shared_ptr<HeapFile> hf = SystemTestUtil::createRandomHeapFile(2, 504 * 10, map<int,int>(), vector<vector<int>>());
	shared_ptr<DbFileIterator> it = hf->iterator(_tid);
	it->open();

	vector<shared_ptr<Tuple>> tuples;
	while (it->hasNext()) {
		Tuple* t1 = it->next();
		shared_ptr<Tuple> t2 = make_shared<Tuple>(t1->getTupleDesc());
		t1->copyTo(*t2);
		tuples.push_back(t2);
	}

	// clear the cache
	Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
	// delete 504 tuples from the first page
	for (int i = 0; i < 504; ++i) {
		Tuple& t = *tuples.at(i);
		Database::getBufferPool()->deleteTuple(_tid, t);
		shared_ptr<HeapPage> p = dynamic_pointer_cast<HeapPage>(
			Database::getBufferPool()->getPage(_tid, t.getRecordId()->getPageId(), Permissions::READ_ONLY));
		EXPECT_EQ(i + 1, p->getNumEmptySlots());
	}

	// delete 504 tuples from the second page
	for (int i = 0; i < 504; ++i) {
		Tuple& t = *tuples.at(i + 504);
		Database::getBufferPool()->deleteTuple(_tid, t);
		shared_ptr<HeapPage> p = dynamic_pointer_cast<HeapPage>(
			Database::getBufferPool()->getPage(_tid, t.getRecordId()->getPageId(), Permissions::READ_ONLY));
		EXPECT_EQ(i + 1, p->getNumEmptySlots());
	}
}

TEST_F(BufferPoolWriteTest, HandleManyDirtyPages) {
	shared_ptr<HeapFileDuplicates> hfd = make_shared<HeapFileDuplicates>(_empty->getFile(), _empty->getTupleDesc(), 10);
	Database::getCatalog()->addTable(hfd, SystemTestUtil::getUUID());
	Database::getBufferPool()->insertTuple(_tid, hfd->getId(), Utility::getHeapTuple(1, 2));

	// there should now be 10 tuples (on 10 different pages) in the buffer pool
	shared_ptr<DbFileIterator> it = hfd->iterator(_tid);
	it->open();

	int count = 0;
	while (it->hasNext()) {
		it->next();
		count++;
	}
	EXPECT_EQ(10, count);
}