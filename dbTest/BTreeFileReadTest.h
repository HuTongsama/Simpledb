#pragma once
#include"SimpleDbTestBase.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include"Utility.h"
using namespace Simpledb;
class BTreeFileReadTest : public SimpleDbTestBase {
protected:
	shared_ptr<BTreeFile> _f;
	shared_ptr<TransactionId> _tid;
	shared_ptr<TupleDesc> _td;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		vector<vector<int>> tuples;
		_f = BTreeUtility::createRandomBTreeFile(2, 20, map<int, int>(), tuples, 0);
		_td = Utility::getTupleDesc(2);
		_tid = make_shared<TransactionId>();
	}
	void TearDown()override {
		Database::getBufferPool()->transactionComplete(_tid);
	}
};

TEST_F(BTreeFileReadTest, GetId) {
	int id = _f->getId();

	// NOTE(ghuo): the value could be anything. test determinism, at least.
	EXPECT_EQ(id, _f->getId());
	EXPECT_EQ(id, _f->getId());
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> other = BTreeUtility::createRandomBTreeFile(1, 1, map<int, int>(), tuples, 0);
	EXPECT_TRUE(id != other->getId());
}

TEST_F(BTreeFileReadTest, GetTupleDesc) {
	EXPECT_TRUE(_td->equals(*_f->getTupleDesc()));
}

TEST_F(BTreeFileReadTest, NumPages) {
	EXPECT_EQ(1, _f->numPages());
}

TEST_F(BTreeFileReadTest, ReadPage) {
	shared_ptr<BTreePageId> rootPtrPid = make_shared<BTreePageId>(_f->getId(), 0, BTreePageId::ROOT_PTR);
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>(_f->readPage(rootPtrPid));

	EXPECT_EQ(1, rootPtr->getRootId()->getPageNumber());
	EXPECT_EQ(BTreePageId::LEAF, rootPtr->getRootId()->pgcateg());

	shared_ptr<BTreePageId> pid = make_shared<BTreePageId>(_f->getId(), 1, BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> page = dynamic_pointer_cast<BTreeLeafPage>(_f->readPage(pid));

	// NOTE(ghuo): we try not to dig too deeply into the Page API here; we
	// rely on BTreePageTest for that. perform some basic checks.
	EXPECT_EQ(482, page->getNumEmptySlots());
	EXPECT_TRUE(page->isSlotUsed(1));
	EXPECT_FALSE(page->isSlotUsed(20));
}

TEST_F(BTreeFileReadTest, TestIteratorBasic) {
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> smallFile = BTreeUtility::createRandomBTreeFile(2, 3, map<int, int>(),
		tuples, 0);

	shared_ptr<DbFileIterator> it = smallFile->iterator(_tid);
	// Not open yet
	EXPECT_FALSE(it->hasNext());

	try {
		it->next();
		EXPECT_THROW(it->next(), runtime_error);
	}
	catch (const std::exception&) {
	
	}

	it->open();
	int count = 0;
	while (it->hasNext()) {
		EXPECT_TRUE(it->next() != nullptr);
		count += 1;
	}
	EXPECT_EQ(3, count);
	it->close();
}

TEST_F(BTreeFileReadTest, TestIteratorClose) {
	// make more than 1 page. Previous closed iterator would start fetching
	// from page 1.
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> twoLeafPageFile = BTreeUtility::createRandomBTreeFile(2, 520,
		map<int,int>(), tuples, 0);

	// there should be 3 pages - two leaf pages and one internal page (the root)
	EXPECT_EQ(3, twoLeafPageFile->numPages());

	shared_ptr<DbFileIterator> it = twoLeafPageFile->iterator(_tid);
	it->open();
	EXPECT_TRUE(it->hasNext());
	it->close();
	try {
		EXPECT_THROW(it->next(), runtime_error);
	}
	catch (const std::exception&) {
	}
	// close twice is harmless
	it->close();
}

TEST_F(BTreeFileReadTest, IndexIterator) {
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> twoLeafPageFile = BTreeUtility::createBTreeFile(2, 520,
		map<int, int>(), tuples, 0);
	shared_ptr<Field> f = make_shared<IntField>(5);

	// greater than
	IndexPredicate ipred(Predicate::Op::GREATER_THAN, f);
	shared_ptr<DbFileIterator> it = twoLeafPageFile->indexIterator(_tid, ipred);
	it->open();
	int count = 0;
	while (it->hasNext()) {
		Tuple* t = it->next();
		EXPECT_TRUE(t->getField(0)->compare(Predicate::Op::GREATER_THAN, *f));
		count++;
	}
	EXPECT_EQ(515, count);
	it->close();

	// less than or equal to
	ipred = IndexPredicate(Predicate::Op::LESS_THAN_OR_EQ, f);
	it = twoLeafPageFile->indexIterator(_tid, ipred);
	it->open();
	count = 0;
	while (it->hasNext()) {
		Tuple* t = it->next();
		EXPECT_TRUE(t->getField(0)->compare(Predicate::Op::LESS_THAN_OR_EQ, *f));
		count++;
	}
	EXPECT_EQ(5, count);
	it->close();

	// equal to
	ipred = IndexPredicate(Predicate::Op::EQUALS, f);
	it = twoLeafPageFile->indexIterator(_tid, ipred);
	it->open();
	count = 0;
	while (it->hasNext()) {
		Tuple* t = it->next();
		EXPECT_TRUE(t->getField(0)->compare(Predicate::Op::EQUALS, *f));
		count++;
	}
	EXPECT_EQ(1, count);
	it->close();

	// now insert a record and ensure EQUALS returns both records
	twoLeafPageFile->insertTuple(_tid, BTreeUtility::getBTreeTuple(5, 2));
	ipred = IndexPredicate(Predicate::Op::EQUALS, f);
	it = twoLeafPageFile->indexIterator(_tid, ipred);
	it->open();
	count = 0;
	while (it->hasNext()) {
		Tuple* t = it->next();
		EXPECT_TRUE(t->getField(0)->compare(Predicate::Op::EQUALS, *f));
		count++;
	}
	EXPECT_EQ(2, count);
	it->close();

	// search for a non-existent record
	f = make_shared<IntField>(1000);
	ipred = IndexPredicate(Predicate::Op::GREATER_THAN, f);
	it = twoLeafPageFile->indexIterator(_tid, ipred);
	it->open();
	EXPECT_FALSE(it->hasNext());
	it->close();
}