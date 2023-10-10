#pragma once
#include"SimpleDbTestBase.h"
#include"Database.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include"File.h"

using namespace Simpledb;
class BTreeFileInsertTest : public SimpleDbTestBase {
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

TEST_F(BTreeFileInsertTest, TestSplitLeafPages) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 3);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the leaf page
	shared_ptr<BTreePageId> leftPageId = make_shared<BTreePageId>(tableid, 2, BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> leftPage = BTreeUtility::createRandomLeafPage(leftPageId, 2, keyField,
		0, BTreeUtility::MAX_RAND_VALUE);

	// create the parent page
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(parentId,
		BTreeInternalPage::createEmptyPageData(), keyField);

	// set the pointers
	leftPage->setParentId(parentId);

	shared_ptr<Field> field = make_shared<IntField>(BTreeUtility::MAX_RAND_VALUE / 2);
	map<shared_ptr<PageId>, shared_ptr<Page>> dirtypages;
	dirtypages[leftPageId] = leftPage;
	dirtypages[parentId] =  parent;
	shared_ptr<BTreeLeafPage> page = empty->splitLeafPage(_tid, dirtypages, leftPage, field);
	EXPECT_TRUE(page->getLeftSiblingId() != nullptr || page->getRightSiblingId() != nullptr);
	shared_ptr<BTreeLeafPage> otherPage;
	if (page->getLeftSiblingId() != nullptr) {
		otherPage = dynamic_pointer_cast<BTreeLeafPage>(dirtypages[page->getLeftSiblingId()]);
		EXPECT_TRUE(field->compare(Predicate::Op::GREATER_THAN_OR_EQ,
			*(otherPage->reverseIterator()->next()->getField(keyField))));
	}
	else { // page.getRightSiblingId() != null
		otherPage = dynamic_pointer_cast<BTreeLeafPage>(dirtypages[page->getRightSiblingId()]);
		EXPECT_TRUE(field->compare(Predicate::Op::LESS_THAN_OR_EQ,
			*(otherPage->iterator()->next()->getField(keyField))));
	}

	int totalTuples = page->getNumTuples() + otherPage->getNumTuples();
	EXPECT_EQ(BTreeUtility::getNumTuplesPerPage(2), totalTuples);
	EXPECT_TRUE(BTreeUtility::getNumTuplesPerPage(2) / 2 == page->getNumTuples() ||
		BTreeUtility::getNumTuplesPerPage(2) / 2 + 1 == page->getNumTuples());
	EXPECT_TRUE(BTreeUtility::getNumTuplesPerPage(2) / 2 == otherPage->getNumTuples() ||
		BTreeUtility::getNumTuplesPerPage(2) / 2 + 1 == otherPage->getNumTuples());
	EXPECT_EQ(1, parent->getNumEntries());
}

TEST_F(BTreeFileInsertTest, TestSplitInternalPages) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	int entriesPerPage = BTreeUtility::getNumEntriesPerPage();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 3 + entriesPerPage);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the internal page
	shared_ptr<BTreePageId> leftPageId = make_shared<BTreePageId>(tableid, 2, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> leftPage = BTreeUtility::createRandomInternalPage(leftPageId, keyField, BTreePageId::LEAF,
		0, BTreeUtility::MAX_RAND_VALUE, 3);

	// create the parent page
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(parentId,
		BTreeInternalPage::createEmptyPageData(), keyField);

	// set the pointers
	leftPage->setParentId(parentId);

	shared_ptr<Field> field = make_shared<IntField>(BTreeUtility::MAX_RAND_VALUE / 2);
	map<shared_ptr<PageId>, shared_ptr<Page>> dirtypages;
	dirtypages[leftPageId] = leftPage;
	dirtypages[parentId] = parent;
	shared_ptr<BTreeInternalPage> page = empty->splitInternalPage(_tid, dirtypages, leftPage, field);
	shared_ptr<BTreeInternalPage> otherPage;
	EXPECT_EQ(1, parent->getNumEntries());
	BTreeEntry* parentEntry = parent->iterator()->next();
	if (parentEntry->getLeftChild()->equals(*page->getId())) {
		otherPage = dynamic_pointer_cast<BTreeInternalPage>(dirtypages[parentEntry->getRightChild()]);
		EXPECT_TRUE(field->compare(Predicate::Op::LESS_THAN_OR_EQ,
			*(otherPage->iterator()->next()->getKey())));
	}
	else { // parentEntry.getRightChild().equals(page.getId())
		otherPage = dynamic_pointer_cast<BTreeInternalPage>(dirtypages[parentEntry->getLeftChild()]);
		EXPECT_TRUE(field->compare(Predicate::Op::GREATER_THAN_OR_EQ,
			*(otherPage->reverseIterator()->next()->getKey())));
	}

	int totalEntries = page->getNumEntries() + otherPage->getNumEntries();
	EXPECT_EQ(entriesPerPage - 1, totalEntries);
	EXPECT_TRUE(entriesPerPage / 2 == page->getNumEntries() ||
		entriesPerPage / 2 - 1 == page->getNumEntries());
	EXPECT_TRUE(entriesPerPage / 2 == otherPage->getNumEntries() ||
		entriesPerPage / 2 - 1 == otherPage->getNumEntries());
}

TEST_F(BTreeFileInsertTest, TestReusePage) {
	shared_ptr<File> emptyFile = File::createTempFile("empty.dat");
	emptyFile->deleteOnExit();
	Database::reset();
	shared_ptr<BTreeFile> empty = BTreeUtility::createEmptyBTreeFile(emptyFile->fileName(), 2, 0, 3);
	size_t tableid = empty->getId();
	int keyField = 0;

	// create the leaf page
	map<shared_ptr<PageId>, shared_ptr<Page>> dirtypages;
	empty->setEmptyPage(_tid, dirtypages, 2);
	shared_ptr<BTreePageId> leftPageId = make_shared<BTreePageId>(tableid, 3, BTreePageId::LEAF);
	shared_ptr<BTreeLeafPage> leftPage = BTreeUtility::createRandomLeafPage(leftPageId, 2, keyField,
		0, BTreeUtility::MAX_RAND_VALUE);

	// create the parent page
	shared_ptr<BTreePageId> parentId = make_shared<BTreePageId>(tableid, 1, BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> parent = make_shared<BTreeInternalPage>(parentId,
		BTreeInternalPage::createEmptyPageData(), keyField);

	// set the pointers
	leftPage->setParentId(parentId);

	shared_ptr<Field> field = make_shared<IntField>(BTreeUtility::MAX_RAND_VALUE / 2);
	dirtypages[leftPageId] = leftPage;
	dirtypages[parentId] = parent;
	shared_ptr<BTreeLeafPage> page = empty->splitLeafPage(_tid, dirtypages, leftPage, field);
	EXPECT_TRUE(page->getLeftSiblingId() != nullptr || page->getRightSiblingId() != nullptr);
	shared_ptr<BTreeLeafPage> otherPage;
	if (page->getLeftSiblingId() != nullptr) {
		otherPage = dynamic_pointer_cast<BTreeLeafPage>(dirtypages[page->getLeftSiblingId()]);
	}
	else { // page.getRightSiblingId() != null
		otherPage = dynamic_pointer_cast<BTreeLeafPage>(dirtypages[page->getRightSiblingId()]);
	}

	EXPECT_TRUE(page->getId()->getPageNumber() == 2 || otherPage->getId()->getPageNumber() == 2);
}