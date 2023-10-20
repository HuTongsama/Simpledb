#pragma once
#include"SimpleDbTestBase.h"
#include"Database.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include"File.h"

using namespace Simpledb;
class BTreeFileDeleteTest : public SimpleDbTestBase {
protected:
	shared_ptr<TransactionId> _tid;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_tid = make_shared<TransactionId>();
	}
	void TearDown()override {
		Database::getBufferPool()->transactionComplete(_tid);
	}
};

TEST_F(BTreeFileDeleteTest, DeleteTuple) {
	shared_ptr<BTreeFile> f;
	vector<vector<int>> tuples;
	f = BTreeUtility::createRandomBTreeFile(2, 20, map<int, int>(), tuples, 0);
	shared_ptr<DbFileIterator> it = f->iterator(_tid);
	it->open();
	while (it->hasNext()) {
		Tuple* t = it->next();
		f->deleteTuple(_tid, *t);
	}
	it->rewind();
	EXPECT_FALSE(it->hasNext());

	// insert a couple of tuples
	f->insertTuple(_tid, BTreeUtility::getBTreeTuple(5, 2));
	f->insertTuple(_tid, BTreeUtility::getBTreeTuple(17, 2));

	it->rewind();
	EXPECT_TRUE(it->hasNext());
}

TEST_F(BTreeFileDeleteTest, TestStealFromLeftLeafPage) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the leaf pages
	shared_ptr<BTreePageId> pageId = make_shared<BTreePageId>(tableid, 1, BTreePageId::LEAF);
	shared_ptr<BTreePageId> siblingId = make_shared<BTreePageId>(tableid, 2, BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> page = BTreeUtility::createRandomLeafPage(pageId, 2, keyField,
		BTreeUtility::getNumTuplesPerPage(2) / 2 - 1, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE);
	shared_ptr<BTreeLeafPage> sibling = BTreeUtility::createRandomLeafPage(siblingId, 2, keyField, 0, BTreeUtility::MAX_RAND_VALUE / 2);

	// create the parent page and the new entry
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 3, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(parentId, BTreeInternalPage::createEmptyPageData(), keyField);
	shared_ptr<Field> key = page->iterator()->next()->getField(keyField);
	shared_ptr<BTreeEntry> entry = make_shared<BTreeEntry>(key, siblingId, pageId);
	parent->insertEntry(entry.get());

	// set all the pointers
	page->setParentId(parentId);
	sibling->setParentId(parentId);
	page->setLeftSiblingId(siblingId);
	sibling->setRightSiblingId(pageId);

	int totalTuples = page->getNumTuples() + sibling->getNumTuples();

	empty->stealFromLeafPage(page, sibling, parent, entry.get(), false);
	EXPECT_EQ(totalTuples, page->getNumTuples() + sibling->getNumTuples());
	EXPECT_TRUE(page->getNumTuples() == totalTuples / 2 || page->getNumTuples() == totalTuples / 2 + 1);
	EXPECT_TRUE(sibling->getNumTuples() == totalTuples / 2 || sibling->getNumTuples() == totalTuples / 2 + 1);
	EXPECT_TRUE(sibling->reverseIterator()->next()->getField(keyField)->compare(Predicate::Op::LESS_THAN_OR_EQ,
		*page->iterator()->next()->getField(keyField)));
}

TEST_F(BTreeFileDeleteTest, TestStealFromRightLeafPage) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the leaf pages
	shared_ptr<BTreePageId> pageId = make_shared<BTreePageId>(tableid, 1, BTreePageId::LEAF);
	shared_ptr<BTreePageId> siblingId = make_shared<BTreePageId>(tableid, 2, BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> page = BTreeUtility::createRandomLeafPage(pageId, 2, keyField,
		BTreeUtility::getNumTuplesPerPage(2) / 2 - 1, 0, BTreeUtility::MAX_RAND_VALUE / 2);
	shared_ptr<BTreeLeafPage> sibling = BTreeUtility::createRandomLeafPage(
		siblingId, 2, keyField, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE);

	// create the parent page and the new entry
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 3, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(
		parentId, BTreeInternalPage::createEmptyPageData(), keyField);
	shared_ptr<Field> key = sibling->iterator()->next()->getField(keyField);
	shared_ptr<BTreeEntry> entry = make_shared<BTreeEntry>(key, pageId, siblingId);
	parent->insertEntry(entry.get());

	// set all the pointers
	page->setParentId(parentId);
	sibling->setParentId(parentId);
	page->setRightSiblingId(siblingId);
	sibling->setLeftSiblingId(pageId);

	int totalTuples = page->getNumTuples() + sibling->getNumTuples();

	empty->stealFromLeafPage(page, sibling, parent, entry.get(), true);
	EXPECT_EQ(totalTuples, page->getNumTuples() + sibling->getNumTuples());
	EXPECT_TRUE(page->getNumTuples() == totalTuples / 2 || page->getNumTuples() == totalTuples / 2 + 1);
	EXPECT_TRUE(sibling->getNumTuples() == totalTuples / 2 || sibling->getNumTuples() == totalTuples / 2 + 1);
	EXPECT_TRUE(page->reverseIterator()->next()->getField(keyField)->compare(Predicate::Op::LESS_THAN_OR_EQ,
		*sibling->iterator()->next()->getField(keyField)));
}

TEST_F(BTreeFileDeleteTest, TestMergeLeafPages) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 3);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the leaf pages
	shared_ptr<BTreePageId> leftPageId = make_shared<BTreePageId>(tableid, 2, BTreePageId::LEAF);
	shared_ptr<BTreePageId> rightPageId = make_shared<BTreePageId>(tableid, 3, BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> leftPage = BTreeUtility::createRandomLeafPage(leftPageId, 2, keyField,
		BTreeUtility::getNumTuplesPerPage(2) / 2 - 1, 0, BTreeUtility::MAX_RAND_VALUE / 2);
	shared_ptr<BTreeLeafPage> rightPage = BTreeUtility::createRandomLeafPage(rightPageId, 2, keyField,
		BTreeUtility::getNumTuplesPerPage(2) / 2 - 1, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE);

	// create the parent page and the new entry
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = BTreeUtility::createRandomInternalPage(parentId, keyField,
		BTreePageId::LEAF, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE, 2);
	BTreeEntry* entry = parent->iterator()->next();
	shared_ptr<Field> siblingKey = rightPage->iterator()->next()->getField(keyField);
	shared_ptr<Field> parentKey = entry->getKey();
	shared_ptr<Field> minKey = (siblingKey->compare(Predicate::Op::LESS_THAN, *parentKey) ? siblingKey : parentKey);
	entry->setKey(minKey);
	parent->updateEntry(entry);
	int numEntries = parent->getNumEntries();

	// set all the pointers
	leftPage->setParentId(parentId);
	rightPage->setParentId(parentId);
	leftPage->setRightSiblingId(rightPageId);
	rightPage->setLeftSiblingId(leftPageId);

	int totalTuples = leftPage->getNumTuples() + rightPage->getNumTuples();

	map<BTreePageId, shared_ptr<Page>> dirtypages;
	dirtypages[*leftPageId] = leftPage;
	dirtypages[*rightPageId] = rightPage;
	dirtypages[*parentId] = parent;
	empty->mergeLeafPages(_tid, dirtypages, leftPage, rightPage, parent, entry);
	EXPECT_EQ(totalTuples, leftPage->getNumTuples());
	EXPECT_EQ(0, rightPage->getNumTuples());
	EXPECT_EQ(nullptr, leftPage->getRightSiblingId());
	EXPECT_EQ(numEntries - 1, parent->getNumEntries());
	EXPECT_EQ(rightPageId->getPageNumber(), empty->getEmptyPageNo(_tid, dirtypages));
}

TEST_F(BTreeFileDeleteTest, TestStealFromLeftInternalPage) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	int entriesPerPage = BTreeUtility::getNumEntriesPerPage();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 5 + 3 * entriesPerPage / 2);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the internal pages
	shared_ptr<BTreePageId> pageId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreePageId> siblingId = make_shared<BTreePageId>(tableid, 2, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> page = BTreeUtility::createRandomInternalPage(pageId, keyField, BTreePageId::LEAF,
		entriesPerPage / 2 - 1, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE, 5 + entriesPerPage);
	shared_ptr<BTreeInternalPage> sibling = BTreeUtility::createRandomInternalPage(siblingId, keyField,
		BTreePageId::LEAF, 0, BTreeUtility::MAX_RAND_VALUE / 2, 4);

	// create the parent page and the new entry
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 3, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(parentId, BTreeInternalPage::createEmptyPageData(), keyField);
	shared_ptr<Field> key = page->iterator()->next()->getKey();
	shared_ptr<BTreeEntry> entry = make_shared<BTreeEntry>(key, siblingId, pageId);
	parent->insertEntry(entry.get());

	// set all the pointers
	page->setParentId(parentId);
	sibling->setParentId(parentId);

	int totalEntries = page->getNumEntries() + sibling->getNumEntries();
	int entriesToSteal = totalEntries / 2 - page->getNumEntries();

	map<BTreePageId, shared_ptr<Page>> dirtypages;
	dirtypages[*pageId] = page;
	dirtypages[*siblingId] = sibling;
	dirtypages[*parentId] = parent;
	empty->stealFromLeftInternalPage(_tid, dirtypages, page, sibling, parent, entry.get());

	// are all the entries still there?
	EXPECT_EQ(totalEntries, page->getNumEntries() + sibling->getNumEntries());

	// have the entries been evenly distributed?
	EXPECT_TRUE(page->getNumEntries() == totalEntries / 2 || page->getNumEntries() == totalEntries / 2 + 1);
	EXPECT_TRUE(sibling->getNumEntries() == totalEntries / 2 || sibling->getNumEntries() == totalEntries / 2 + 1);

	// are the keys in the left page less than the keys in the right page?
	EXPECT_TRUE(sibling->reverseIterator()->next()->getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ,
		*page->iterator()->next()->getKey()));

	// is the parent key reasonable?
	EXPECT_TRUE(parent->iterator()->next()->getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ, *page->iterator()->next()->getKey()));
	EXPECT_TRUE(parent->iterator()->next()->getKey()->compare(Predicate::Op::GREATER_THAN_OR_EQ, *sibling->reverseIterator()->next()->getKey()));

	// are all the parent pointers set?
	shared_ptr<Iterator<BTreeEntry>> it = page->iterator();
	BTreeEntry* e = nullptr;
	int count = 0;
	while (count < entriesToSteal) {
		EXPECT_TRUE(it->hasNext());
		e = it->next();
		shared_ptr<BTreePage> p = dynamic_pointer_cast<BTreePage>(dirtypages[*e->getLeftChild()]);
		EXPECT_EQ(pageId->hashCode(), p->getParentId()->hashCode());
		++count;
	}
}

TEST_F(BTreeFileDeleteTest, TestStealFromRightInternalPage) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	int entriesPerPage = BTreeUtility::getNumEntriesPerPage();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 5 + 3 * entriesPerPage / 2);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the internal pages
	shared_ptr<BTreePageId> pageId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreePageId> siblingId = make_shared<BTreePageId>(tableid, 2, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> page = BTreeUtility::createRandomInternalPage(pageId, keyField, BTreePageId::LEAF,
		entriesPerPage / 2 - 1, 0, BTreeUtility::MAX_RAND_VALUE / 2, 4);
	shared_ptr<BTreeInternalPage> sibling = BTreeUtility::createRandomInternalPage(siblingId, keyField,
		BTreePageId::LEAF, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE, 4 + entriesPerPage / 2);

	// create the parent page and the new entry
	shared_ptr<BTreePageId> parentId =  make_shared<BTreePageId>(tableid, 3, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(parentId, BTreeInternalPage::createEmptyPageData(), keyField);
	shared_ptr<Field> key = sibling->iterator()->next()->getKey();
	shared_ptr<BTreeEntry> entry = make_shared<BTreeEntry>(key, pageId, siblingId);
	parent->insertEntry(entry.get());

	// set all the pointers
	page->setParentId(parentId);
	sibling->setParentId(parentId);

	int totalEntries = page->getNumEntries() + sibling->getNumEntries();
	int entriesToSteal = totalEntries / 2 - page->getNumEntries();

	map<BTreePageId, shared_ptr<Page>> dirtypages;
	dirtypages[*pageId] = page;
	dirtypages[*siblingId] = sibling;
	dirtypages[*parentId] = parent;
	empty->stealFromRightInternalPage(_tid, dirtypages, page, sibling, parent, entry.get());

	// are all the entries still there?
	EXPECT_EQ(totalEntries, page->getNumEntries() + sibling->getNumEntries());

	// have the entries been evenly distributed?
	EXPECT_TRUE(page->getNumEntries() == totalEntries / 2 || page->getNumEntries() == totalEntries / 2 + 1);
	EXPECT_TRUE(sibling->getNumEntries() == totalEntries / 2 || sibling->getNumEntries() == totalEntries / 2 + 1);

	// are the keys in the left page less than the keys in the right page?
	EXPECT_TRUE(page->reverseIterator()->next()->getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ,
		*sibling->iterator()->next()->getKey()));

	// is the parent key reasonable?
	EXPECT_TRUE(parent->iterator()->next()->getKey()->compare(Predicate::Op::LESS_THAN_OR_EQ, *sibling->iterator()->next()->getKey()));
	EXPECT_TRUE(parent->iterator()->next()->getKey()->compare(Predicate::Op::GREATER_THAN_OR_EQ, *page->reverseIterator()->next()->getKey()));

	// are all the parent pointers set?
	shared_ptr<Iterator<BTreeEntry>> it = page->reverseIterator();
	BTreeEntry* e = nullptr;
	int count = 0;
	while (count < entriesToSteal) {
		EXPECT_TRUE(it->hasNext());
		e = it->next();
		shared_ptr<BTreePage> p = dynamic_pointer_cast<BTreePage>(dirtypages[*e->getRightChild()]);
		EXPECT_EQ(pageId->hashCode(), p->getParentId()->hashCode());
		++count;
	}
}

TEST_F(BTreeFileDeleteTest, TestMergeInternalPages) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	int entriesPerPage = BTreeUtility::getNumEntriesPerPage();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 1 + 2 * entriesPerPage);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the internal pages
	shared_ptr<BTreePageId> leftPageId = make_shared<BTreePageId>(tableid, 2, BTreePageId::INTERNAL);
	shared_ptr<BTreePageId> rightPageId = make_shared<BTreePageId>(tableid, 3, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> leftPage = BTreeUtility::createRandomInternalPage(leftPageId, keyField, BTreePageId::LEAF,
		entriesPerPage / 2 - 1, 0, BTreeUtility::MAX_RAND_VALUE / 2, 3 + entriesPerPage);
	shared_ptr<BTreeInternalPage> rightPage = BTreeUtility::createRandomInternalPage(rightPageId, keyField, BTreePageId::LEAF,
		entriesPerPage / 2 - 1, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE, 2 + 3 * entriesPerPage / 2);

	// create the parent page and the new entry
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = BTreeUtility::createRandomInternalPage(parentId, keyField,
		BTreePageId::LEAF, BTreeUtility::MAX_RAND_VALUE / 2, BTreeUtility::MAX_RAND_VALUE, 2);
	BTreeEntry entry = *parent->iterator()->next();
	shared_ptr<Field> siblingKey = rightPage->iterator()->next()->getKey();
	shared_ptr<Field> parentKey = entry.getKey();
	shared_ptr<Field> minKey = (siblingKey->compare(Predicate::Op::LESS_THAN, *parentKey) ? siblingKey : parentKey);
	entry.setKey(minKey);
	parent->updateEntry(&entry);
	int numParentEntries = parent->getNumEntries();

	// set all the pointers
	leftPage->setParentId(parentId);
	rightPage->setParentId(parentId);

	int totalEntries = leftPage->getNumEntries() + rightPage->getNumEntries();

	map<BTreePageId, shared_ptr<Page>> dirtypages;
	dirtypages[*leftPageId] = leftPage;
	dirtypages[*rightPageId] = rightPage;
	dirtypages[*parentId] = parent;
	empty->mergeInternalPages(_tid, dirtypages, leftPage, rightPage, parent, &entry);
	EXPECT_EQ(totalEntries + 1, leftPage->getNumEntries());
	EXPECT_EQ(0, rightPage->getNumEntries());
	EXPECT_EQ(numParentEntries - 1, parent->getNumEntries());
	EXPECT_EQ(rightPageId->getPageNumber(), empty->getEmptyPageNo(_tid, dirtypages));

	// are all the parent pointers set?
	shared_ptr<Iterator<BTreeEntry>> it = leftPage->reverseIterator();
	BTreeEntry* e = nullptr;
	int count = 0;
	while (count < entriesPerPage / 2 - 1) {
		EXPECT_TRUE(it->hasNext());
		e = it->next();
		shared_ptr<BTreePage> p = dynamic_pointer_cast<BTreePage>(dirtypages[*e->getRightChild()]);
		EXPECT_EQ(leftPageId->hashCode(), p->getParentId()->hashCode());
		++count;
	}
}