#pragma once
#include"SimpleDbTestBase.h"
#include"Database.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include"BTreeChecker.h"
#include"File.h"
#include<random>

using namespace Simpledb;
class SysBTreeFileInsertTest : public SimpleDbTestBase {
protected:
	shared_ptr<TransactionId> _tid;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_tid = make_shared<TransactionId>();
	}
	void TearDown()override {
		Database::getBufferPool()->transactionComplete(_tid);

		// set the page size back to the default
		BufferPool::resetPageSize();
		Database::reset();
	}

};

TEST_F(SysBTreeFileInsertTest, AddTuple) {
	// create an empty B+ tree file keyed on the second field of a 2-field tuple
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 1);

	shared_ptr<Tuple> tup;
	// we should be able to add 502 tuples on one page
	for (int i = 0; i < 502; ++i) {
		tup = BTreeUtility::getBTreeTuple(i, 2);
		empty->insertTuple(_tid, tup);
		EXPECT_EQ(1, empty->numPages());
	}

	// the next 251 tuples should live on page 2 since they are greater than
	// all existing tuples in the file
	for (int i = 502; i < 753; ++i) {
		tup = BTreeUtility::getBTreeTuple(i, 2);
		empty->insertTuple(_tid, tup);
		EXPECT_EQ(3, empty->numPages());
	}

	// one more insert greater than 502 should cause page 2 to split
	tup = BTreeUtility::getBTreeTuple(753, 2);
	empty->insertTuple(_tid, tup);
	EXPECT_EQ(4, empty->numPages());

	// now make sure the records are sorted on the key field
	shared_ptr<DbFileIterator> it = empty->iterator(_tid);
	it->open();
	int prev = -1;
	while (it->hasNext()) {
		Tuple* t = it->next();
		int value = (dynamic_pointer_cast<IntField>(t->getField(0)))->getValue();
		EXPECT_TRUE(value >= prev);
		prev = value;
	}
}

TEST_F(SysBTreeFileInsertTest, AddDuplicateTuples) {
	// create an empty B+ tree file keyed on the second field of a 2-field tuple
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 1);

	shared_ptr<Tuple> tup;
	map<shared_ptr<PageId>, shared_ptr<Page>> tmpMap;
	BTreeChecker::checkRep(empty, _tid, tmpMap, true);

	// add a bunch of identical tuples
	for (int i = 0; i < 5; ++i) {
		for (int j = 0; j < 600; ++j) {
			tup = BTreeUtility::getBTreeTuple(i, 2);
			empty->insertTuple(_tid, tup);
		}

	}

	BTreeChecker::checkRep(empty, _tid, tmpMap, true);

	// now search for some ranges and make sure we find all the tuples
	IndexPredicate ipred(Predicate::Op::EQUALS, make_shared<IntField>(3));
	shared_ptr<DbFileIterator> it = empty->indexIterator(_tid, ipred);
	it->open();
	int count = 0;
	while (it->hasNext()) {
		it->next();
		count++;
	}
	EXPECT_EQ(600, count);

	ipred = IndexPredicate(Predicate::Op::GREATER_THAN_OR_EQ, make_shared<IntField>(2));
	it = empty->indexIterator(_tid, ipred);
	it->open();
	count = 0;
	while (it->hasNext()) {
		it->next();
		count++;
	}
	EXPECT_EQ(1800, count);

	ipred = IndexPredicate(Predicate::Op::LESS_THAN, make_shared<IntField>(2));
	it = empty->indexIterator(_tid, ipred);
	it->open();
	count = 0;
	while (it->hasNext()) {
		it->next();
		count++;
	}
	EXPECT_EQ(1200, count);
}

TEST_F(SysBTreeFileInsertTest, TestSplitLeafPage) {
	vector<vector<int>> tuples;
	// This should create a B+ tree with one full page
	shared_ptr<BTreeFile> onePageFile = BTreeUtility::createRandomBTreeFile(2, 502,
		map<int,int>(), tuples, 0);

	// there should be 1 leaf page
	EXPECT_EQ(1, onePageFile->numPages());

	// now insert a tuple
	Database::getBufferPool()->insertTuple(_tid, onePageFile->getId(), BTreeUtility::getBTreeTuple(5000, 2));

	// there should now be 2 leaf pages + 1 internal node
	EXPECT_EQ(3, onePageFile->numPages());

	// the root node should be an internal node and have 2 children (1 entry)
	shared_ptr<BTreePageId> rootPtrPid = make_shared<BTreePageId>(onePageFile->getId(), 0, BTreePageId::ROOT_PTR);
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
		(Database::getBufferPool()->getPage(_tid, rootPtrPid, Permissions::READ_ONLY));
	shared_ptr<BTreePageId> rootId = rootPtr->getRootId();
	EXPECT_EQ(rootId->pgcateg(), BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>
		(Database::getBufferPool()->getPage(_tid, rootId, Permissions::READ_ONLY));
	EXPECT_EQ(502, root->getNumEmptySlots());

	// each child should have half of the records
	shared_ptr<Iterator<BTreeEntry>> it = root->iterator();
	EXPECT_TRUE(it->hasNext());
	BTreeEntry* e = it->next();
	shared_ptr<BTreeLeafPage> leftChild = dynamic_pointer_cast<BTreeLeafPage>
		(Database::getBufferPool()->getPage(_tid, e->getLeftChild(), Permissions::READ_ONLY));
	shared_ptr<BTreeLeafPage> rightChild = dynamic_pointer_cast<BTreeLeafPage>
		(Database::getBufferPool()->getPage(_tid, e->getRightChild(), Permissions::READ_ONLY));
	EXPECT_TRUE(leftChild->getNumEmptySlots() <= 251);
	EXPECT_TRUE(rightChild->getNumEmptySlots() <= 251);
}

TEST_F(SysBTreeFileInsertTest, TestSplitRootPage) {
	// This should create a packed B+ tree with no empty slots
	// There are 503 keys per internal page (504 children) and 502 tuples per leaf page
	// 504 * 502 = 253008
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> bigFile = BTreeUtility::createRandomBTreeFile(2, 253008,
		map<int, int>(), tuples, 0);

	// we will need more room in the buffer pool for this test
	Database::resetBufferPool(500);

	// there should be 504 leaf pages + 1 internal node
	EXPECT_EQ(505, bigFile->numPages());

	// now insert a tuple
	Database::getBufferPool()->insertTuple(_tid, bigFile->getId(), BTreeUtility::getBTreeTuple(10, 2));

	// there should now be 505 leaf pages + 3 internal nodes
	EXPECT_EQ(508, bigFile->numPages());

	// the root node should be an internal node and have 2 children (1 entry)
	shared_ptr<BTreePageId> rootPtrPid = make_shared<BTreePageId>(bigFile->getId(), 0, BTreePageId::ROOT_PTR);
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
		(Database::getBufferPool()->getPage(_tid, rootPtrPid, Permissions::READ_ONLY));
	shared_ptr<BTreePageId> rootId = rootPtr->getRootId();
	EXPECT_EQ(rootId->pgcateg(), BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>
		(Database::getBufferPool()->getPage(_tid, rootId, Permissions::READ_ONLY));
	EXPECT_EQ(502, root->getNumEmptySlots());

	// each child should have half of the entries
	shared_ptr<Iterator<BTreeEntry>> it = root->iterator();
	EXPECT_TRUE(it->hasNext());
	BTreeEntry* e = it->next();
	shared_ptr<BTreeInternalPage> leftChild = dynamic_pointer_cast<BTreeInternalPage>
		(Database::getBufferPool()->getPage(_tid, e->getLeftChild(), Permissions::READ_ONLY));
	shared_ptr<BTreeInternalPage> rightChild = dynamic_pointer_cast<BTreeInternalPage>
		(Database::getBufferPool()->getPage(_tid, e->getRightChild(), Permissions::READ_ONLY));
	EXPECT_TRUE(leftChild->getNumEmptySlots() <= 252);
	EXPECT_TRUE(rightChild->getNumEmptySlots() <= 252);

	// now insert some random tuples and make sure we can find them
	std::random_device randomDevice;
	std::default_random_engine randomEngine(randomDevice());
	std::uniform_int_distribution<int> uniform_dist(0, BTreeUtility::MAX_RAND_VALUE);
	for (int i = 0; i < 100; i++) {
		int item = uniform_dist(randomEngine);
		shared_ptr<Tuple> t = BTreeUtility::getBTreeTuple(item, 2);
		Database::getBufferPool()->insertTuple(_tid, bigFile->getId(), t);

		IndexPredicate ipred(Predicate::Op::EQUALS, t->getField(0));
		shared_ptr<DbFileIterator> fit = bigFile->indexIterator(_tid, ipred);
		fit->open();
		bool found = false;
		while (fit->hasNext()) {
			if (fit->next()->equals(*t)) {
				found = true;
				break;
			}
		}
		fit->close();
		EXPECT_TRUE(found);
	}
}

TEST_F(SysBTreeFileInsertTest, TestSplitInternalPage) {
	// For this test we will decrease the size of the Buffer Pool pages
	BufferPool::setPageSize(1024);

	// This should create a B+ tree with a packed second tier of internal pages
	// and packed third tier of leaf pages
	// (124 entries per internal/leaf page, 125 children per internal page ->
	// 125*2*124 = 31000)
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> bigFile = BTreeUtility::createRandomBTreeFile(2, 31000,
		map<int, int>(), tuples, 0);

	// we will need more room in the buffer pool for this test
	Database::resetBufferPool(1000);

	// there should be 250 leaf pages + 3 internal nodes
	EXPECT_EQ(253, bigFile->numPages());

	// now insert some random tuples and make sure we can find them
	std::random_device randomDevice;
	std::default_random_engine randomEngine(randomDevice());
	std::uniform_int_distribution<int> uniform_dist(0, BTreeUtility::MAX_RAND_VALUE);
	for (int i = 0; i < 100; i++) {
		int item = uniform_dist(randomEngine);
		shared_ptr<Tuple> t = BTreeUtility::getBTreeTuple(item, 2);
		Database::getBufferPool()->insertTuple(_tid, bigFile->getId(), t);

		IndexPredicate ipred(Predicate::Op::EQUALS, t->getField(0));
		shared_ptr<DbFileIterator> fit = bigFile->indexIterator(_tid, ipred);
		fit->open();
		bool found = false;
		while (fit->hasNext()) {
			if (fit->next()->equals(*t)) {
				found = true;
				break;
			}
		}
		fit->close();
		EXPECT_TRUE(found);
	}

	// now make sure we have 31100 records and they are all in sorted order
	shared_ptr<DbFileIterator> fit = bigFile->iterator(_tid);
	int count = 0;
	Tuple* prev = nullptr;
	fit->open();
	while (fit->hasNext()) {
		Tuple* tup = fit->next();
		if (prev != nullptr)
			EXPECT_TRUE(tup->getField(0)->compare(Predicate::Op::GREATER_THAN_OR_EQ, *prev->getField(0)));
		prev = tup;
		count++;
	}
	fit->close();
	EXPECT_EQ(31100, count);
}