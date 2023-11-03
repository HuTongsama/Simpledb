#pragma once
#include"SimpleDbTestBase.h"
#include"Database.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include"BTreeChecker.h"
#include"File.h"

using namespace Simpledb;
class SysBTreeFileDeleteTest :public SimpleDbTestBase {
protected:
	shared_ptr<TransactionId> _tid;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_tid = make_shared<TransactionId>();
	}
	void TearDown()override {
		Database::getBufferPool()->transactionComplete(_tid);
		BufferPool::resetPageSize();
		Database::reset();
	}

};

TEST_F(SysBTreeFileDeleteTest, TestRedistributeLeafPages) {
	vector<vector<int>> tuples;
	map<BTreePageId, shared_ptr<Page>> dirtyPages;
	// This should create a B+ tree with two partially full leaf pages
	shared_ptr<BTreeFile> twoLeafPageFile = BTreeUtility::createRandomBTreeFile(2, 600,
		map<int, int>(), tuples, 0);	
	BTreeChecker::checkRep(twoLeafPageFile.get(), _tid, dirtyPages, true);

	// Delete some tuples from the first page until it gets to minimum occupancy
	shared_ptr<DbFileIterator> it = twoLeafPageFile->iterator(_tid);
	it->open();
	int count = 0;
	while (it->hasNext() && count < 49) {
		Tuple* t = it->next();
		shared_ptr<BTreePageId> pid = dynamic_pointer_cast<BTreePageId>(t->getRecordId()->getPageId());
		shared_ptr<BTreeLeafPage> p = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(
			_tid, pid, Permissions::READ_ONLY));
		EXPECT_EQ(202 + count, p->getNumEmptySlots());
		twoLeafPageFile->deleteTuple(_tid, *t);
		count++;
	}
	BTreeChecker::checkRep(twoLeafPageFile.get(), _tid, dirtyPages, true);

	// deleting a tuple now should bring the page below minimum occupancy and cause 
	// the tuples to be redistributed
	Tuple* t = it->next();
	it->close();
	shared_ptr<BTreePageId> pid = dynamic_pointer_cast<BTreePageId>(t->getRecordId()->getPageId());
	shared_ptr<BTreeLeafPage> p = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(
		_tid, pid, Permissions::READ_ONLY));
	EXPECT_EQ(251, p->getNumEmptySlots());
	twoLeafPageFile->deleteTuple(_tid, *t);
	EXPECT_TRUE(p->getNumEmptySlots() <= 251);

	shared_ptr<BTreePageId> rightSiblingId = p->getRightSiblingId();
	shared_ptr<BTreeLeafPage> rightSibling = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(
		_tid, rightSiblingId, Permissions::READ_ONLY));
	EXPECT_TRUE(rightSibling->getNumEmptySlots() > 202);
}

TEST_F(SysBTreeFileDeleteTest, TestMergeLeafPages) {
	vector<vector<int>> tuples;
	map<BTreePageId, shared_ptr<Page>> dirtyPages;
	// This should create a B+ tree with one full page and two half-full leaf pages
	shared_ptr<BTreeFile> threeLeafPageFile = BTreeUtility::createRandomBTreeFile(2, 1005,
		map<int, int>(), tuples, 0);

	BTreeChecker::checkRep(threeLeafPageFile.get(),
		_tid, dirtyPages, true);
	// there should be one internal node and 3 leaf nodes
	EXPECT_EQ(4, threeLeafPageFile->numPages());

	// delete the last two tuples
	shared_ptr<DbFileIterator> it = threeLeafPageFile->iterator(_tid);
	it->open();
	Tuple* secondToLast = nullptr;
	Tuple* last = nullptr;
	while (it->hasNext()) {
		secondToLast = last;
		last = it->next();
	}
	it->close();
	threeLeafPageFile->deleteTuple(_tid, *secondToLast);
	threeLeafPageFile->deleteTuple(_tid, *last);
	BTreeChecker::checkRep(threeLeafPageFile.get(), _tid, dirtyPages, true);

	// confirm that the last two pages have merged successfully
	shared_ptr<BTreePageId> rootPtrId = BTreeRootPtrPage::getId(threeLeafPageFile->getId());
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>(Database::getBufferPool()->getPage(
		_tid, rootPtrId, Permissions::READ_ONLY));
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, rootPtr->getRootId(), Permissions::READ_ONLY));
	EXPECT_EQ(502, root->getNumEmptySlots());
	BTreeEntry e = *root->iterator()->next();
	shared_ptr<BTreeLeafPage> leftChild = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(
		_tid, e.getLeftChild(), Permissions::READ_ONLY));
	shared_ptr<BTreeLeafPage> rightChild = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(
		_tid, e.getRightChild(), Permissions::READ_ONLY));
	EXPECT_EQ(0, leftChild->getNumEmptySlots());
	EXPECT_EQ(1, rightChild->getNumEmptySlots());
	EXPECT_EQ(e.getKey()->hashCode(), rightChild->iterator()->next()->getField(0)->hashCode());
}

TEST_F(SysBTreeFileDeleteTest, TestDeleteRootPage) {
	vector<vector<int>> tuples;
	map<BTreePageId, shared_ptr<Page>> dirtyPages;
	// This should create a B+ tree with two half-full leaf pages
	shared_ptr<BTreeFile> twoLeafPageFile = BTreeUtility::createRandomBTreeFile(2, 503,
		map<int,int>(), tuples, 0);
	// there should be one internal node and 2 leaf nodes
	EXPECT_EQ(3, twoLeafPageFile->numPages());
	BTreeChecker::checkRep(twoLeafPageFile.get(),
		_tid, dirtyPages, true);

	// delete the first two tuples
	shared_ptr<DbFileIterator> it = twoLeafPageFile->iterator(_tid);
	it->open();
	Tuple* first = it->next();
	Tuple* second = it->next();
	it->close();
	twoLeafPageFile->deleteTuple(_tid, *first);
	BTreeChecker::checkRep(twoLeafPageFile.get(), _tid, dirtyPages, false);
	twoLeafPageFile->deleteTuple(_tid, *second);
	BTreeChecker::checkRep(twoLeafPageFile.get(), _tid, dirtyPages, false);

	// confirm that the last two pages have merged successfully and replaced the root
	shared_ptr<BTreePageId> rootPtrId = BTreeRootPtrPage::getId(twoLeafPageFile->getId());
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>(Database::getBufferPool()->getPage(
		_tid, rootPtrId, Permissions::READ_ONLY));
	EXPECT_EQ(rootPtr->getRootId()->pgcateg(), BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> root = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(
		_tid, rootPtr->getRootId(), Permissions::READ_ONLY));
	EXPECT_EQ(1, root->getNumEmptySlots());
	EXPECT_EQ(root->getParentId()->hashCode(), rootPtrId->hashCode());
}

TEST_F(SysBTreeFileDeleteTest, TestReuseDeletedPages) {
	vector<vector<int>> tuples;
	map<BTreePageId, shared_ptr<Page>> dirtyPages;
	// this should create a B+ tree with 3 leaf nodes
	shared_ptr<BTreeFile> threeLeafPageFile = BTreeUtility::createRandomBTreeFile(2, 1005,
		map<int, int>(), tuples, 0);
	BTreeChecker::checkRep(threeLeafPageFile.get(), _tid, dirtyPages, true);

	// 3 leaf pages, 1 internal page
	EXPECT_EQ(4, threeLeafPageFile->numPages());

	// delete enough tuples to ensure one page gets deleted
	shared_ptr<DbFileIterator> it = threeLeafPageFile->iterator(_tid);
	it->open();
	for (int i = 0; i < 502; ++i) {
		Database::getBufferPool()->deleteTuple(_tid, *it->next());
		it->rewind();
	}
	it->close();

	// now there should be 2 leaf pages, 1 internal page, 1 unused leaf page, 1 header page
	EXPECT_EQ(5, threeLeafPageFile->numPages());
	// insert enough tuples to ensure one of the leaf pages splits
	for (int i = 0; i < 502; ++i) {
		Database::getBufferPool()->insertTuple(_tid, threeLeafPageFile->getId(),
			BTreeUtility::getBTreeTuple(i, 2));
		BTreeChecker::checkRep(threeLeafPageFile.get(), _tid, dirtyPages, true);
	}	
	// now there should be 3 leaf pages, 1 internal page, and 1 header page
	EXPECT_EQ(5, threeLeafPageFile->numPages());
	BTreeChecker::checkRep(threeLeafPageFile.get(), _tid, dirtyPages, true);
}

TEST_F(SysBTreeFileDeleteTest, TestRedistributeInternalPages) {
	vector<vector<int>> tuples;
	map<BTreePageId, shared_ptr<Page>> dirtyPages;
	// This should create a B+ tree with two nodes in the second tier
	// and 602 nodes in the third tier
	shared_ptr<BTreeFile> bf = BTreeUtility::createRandomBTreeFile(2, 302204,
		map<int, int>(), tuples, 0);
	BTreeChecker::checkRep(bf.get(), _tid, dirtyPages, true);

	Database::resetBufferPool(512); // we need more pages for this test

	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>(Database::getBufferPool()->getPage(
		_tid, BTreeRootPtrPage::getId(bf->getId()), Permissions::READ_ONLY));
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, rootPtr->getRootId(), Permissions::READ_ONLY));
	EXPECT_EQ(502, root->getNumEmptySlots());

	BTreeEntry rootEntry = *root->iterator()->next();
	shared_ptr<BTreeInternalPage> leftChild = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, rootEntry.getLeftChild(), Permissions::READ_ONLY));
	shared_ptr<BTreeInternalPage> rightChild = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, rootEntry.getRightChild(), Permissions::READ_ONLY));

	// delete from the right child to test redistribution from the left
	shared_ptr<Iterator<BTreeEntry>> it = rightChild->iterator();
	int count = 0;
	// bring the right internal page to minimum occupancy
	while (it->hasNext() && count < 49 * 502 + 1) {
		shared_ptr<BTreeLeafPage> leaf = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(_tid,
			it->next()->getLeftChild(), Permissions::READ_ONLY));		
		Tuple* t = leaf->iterator()->next();
		Database::getBufferPool()->deleteTuple(_tid, *t);
		it = rightChild->iterator();
		count++;
	}

	// deleting a page of tuples should bring the internal page below minimum 
	// occupancy and cause the entries to be redistributed
	EXPECT_EQ(252, rightChild->getNumEmptySlots());
	count = 0;
	while (it->hasNext() && count < 502) {
		shared_ptr<BTreeLeafPage> leaf = dynamic_pointer_cast<BTreeLeafPage>(Database::getBufferPool()->getPage(_tid,
			it->next()->getLeftChild(), Permissions::READ_ONLY));
		Tuple* t = leaf->iterator()->next();
		Database::getBufferPool()->deleteTuple(_tid, *t);
		it = rightChild->iterator();
		count++;
	}
	EXPECT_TRUE(leftChild->getNumEmptySlots() > 203);
	EXPECT_TRUE(rightChild->getNumEmptySlots() <= 252);
	BTreeChecker::checkRep(bf.get(), _tid, dirtyPages, true);

	// sanity check that the entries make sense
	BTreeEntry* lastLeftEntry = nullptr;
	it = leftChild->iterator();
	while (it->hasNext()) {
		lastLeftEntry = it->next();
	}
	rootEntry = *root->iterator()->next();
	BTreeEntry* firstRightEntry = rightChild->iterator()->next();
	EXPECT_TRUE(lastLeftEntry->getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ, *rootEntry.getKey()));
	EXPECT_TRUE(rootEntry.getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ, *firstRightEntry->getKey()));
}

TEST_F(SysBTreeFileDeleteTest, TestDeleteInternalPages) {
	vector<vector<int>> tuples;
	map<BTreePageId, shared_ptr<Page>> dirtyPages;
	// For this test we will decrease the size of the Buffer Pool pages
	BufferPool::setPageSize(1024);

	// This should create a B+ tree with three nodes in the second tier
	// and 252 nodes in the third tier
	// (124 entries per internal/leaf page, 125 children per internal page ->
	// 251*124 + 1 = 31125)
	shared_ptr<BTreeFile> bigFile = BTreeUtility::createRandomBTreeFile(2, 31125,
		map<int,int>(), tuples, 0);

	BTreeChecker::checkRep(bigFile.get(), _tid, dirtyPages, true);

	Database::resetBufferPool(500); // we need more pages for this test

	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>(Database::getBufferPool()->getPage(
		_tid, BTreeRootPtrPage::getId(bigFile->getId()), Permissions::READ_ONLY));
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, rootPtr->getRootId(), Permissions::READ_ONLY));
	EXPECT_EQ(122, root->getNumEmptySlots());

	BTreeEntry e = *root->iterator()->next();
	shared_ptr<BTreeInternalPage> leftChild = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, e.getLeftChild(), Permissions::READ_ONLY));
	shared_ptr<BTreeInternalPage> rightChild = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, e.getRightChild(), Permissions::READ_ONLY));

	// Delete tuples causing leaf pages to merge until the first internal page 
	// gets to minimum occupancy
	shared_ptr<DbFileIterator> it = bigFile->iterator(_tid);
	it->open();
	int count = 0;
	Database::getBufferPool()->deleteTuple(_tid, *it->next());
	it->rewind();
	while (count < 62) {
		EXPECT_EQ(count, leftChild->getNumEmptySlots());
		for (int i = 0; i < 124; ++i) {
			Database::getBufferPool()->deleteTuple(_tid, *it->next());
			it->rewind();
		}
		count++;
	}

	BTreeChecker::checkRep(bigFile.get(), _tid, dirtyPages, true);

	// deleting a page of tuples should bring the internal page below minimum 
	// occupancy and cause the entries to be redistributed
	EXPECT_EQ(62, leftChild->getNumEmptySlots());
	for (int i = 0; i < 124; ++i) {
		Database::getBufferPool()->deleteTuple(_tid, *it->next());
		it->rewind();
	}

	BTreeChecker::checkRep(bigFile.get(), _tid, dirtyPages, true);

	EXPECT_EQ(62, leftChild->getNumEmptySlots());
	EXPECT_EQ(62, rightChild->getNumEmptySlots());

	// deleting another page of tuples should bring the page below minimum occupancy 
	// again but this time cause it to merge with its right sibling 
	for (int i = 0; i < 124; ++i) {
		Database::getBufferPool()->deleteTuple(_tid, *it->next());
		it->rewind();
	}

	// confirm that the pages have merged
	EXPECT_EQ(123, root->getNumEmptySlots());
	e = *root->iterator()->next();
	leftChild = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, e.getLeftChild(), Permissions::READ_ONLY));
	rightChild = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, e.getRightChild(), Permissions::READ_ONLY));
	EXPECT_EQ(0, leftChild->getNumEmptySlots());
	EXPECT_TRUE(e.getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ, *rightChild->iterator()->next()->getKey()));

	// Delete tuples causing leaf pages to merge until the first internal page 
	// gets below minimum occupancy and causes the entries to be redistributed
	count = 0;
	while (count < 62) {
		EXPECT_EQ(count, leftChild->getNumEmptySlots());
		for (int i = 0; i < 124; ++i) {
			Database::getBufferPool()->deleteTuple(_tid, *it->next());
			it->rewind();
		}
		count++;
	}

	// deleting another page of tuples should bring the page below minimum occupancy 
	// and cause it to merge with the right sibling to replace the root
	for (int i = 0; i < 124; ++i) {
		Database::getBufferPool()->deleteTuple(_tid, *it->next());
		it->rewind();
	}

	// confirm that the last two internal pages have merged successfully and 
	// replaced the root
	shared_ptr<BTreePageId> rootPtrId = BTreeRootPtrPage::getId(bigFile->getId());
	rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>(Database::getBufferPool()->getPage(
		_tid, rootPtrId, Permissions::READ_ONLY));
	EXPECT_EQ(rootPtr->getRootId()->pgcateg(), BTreePageId::INTERNAL);
	root = dynamic_pointer_cast<BTreeInternalPage>(Database::getBufferPool()->getPage(
		_tid, rootPtr->getRootId(), Permissions::READ_ONLY));
	EXPECT_EQ(0, root->getNumEmptySlots());
	EXPECT_EQ(root->getParentId()->hashCode(), rootPtrId->hashCode());

	it->close();
}