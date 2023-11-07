#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"TestUtil.h"
#include"Utility.h"
#include"BTreeUtility.h"
#include"BTreeInternalPage.h"
#include"BTreeFileEncoder.h"
using namespace Simpledb;
class BTreeInternalPageTest : public SimpleDbTestBase {
protected:
	shared_ptr<BTreePageId> _pid;
	// these entries have been carefully chosen to be valid entries when
	// inserted in order. Be careful if you change them!
	vector<vector<int>> EXAMPLE_VALUES;
	vector<unsigned char> EXAMPLE_DATA;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_pid = make_shared<BTreePageId>(-1, -1, BTreePageId::INTERNAL);
		Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(-1, Utility::getTupleDesc(2)), SystemTestUtil::getUUID());
		EXAMPLE_VALUES = {
		{ 2, 6350, 4 },
		{ 4, 9086, 5 },
		{ 5, 17197, 7 },
		{ 7, 22064, 9 },
		{ 9, 22189, 10 },
		{ 10, 28617, 11 },
		{ 11, 31933, 13 },
		{ 13, 33549, 14 },
		{ 14, 34784, 15 },
		{ 15, 42878, 17 },
		{ 17, 45569, 19 },
		{ 19, 56462, 20 },
		{ 20, 62778, 21 },
		{ 15, 42812, 16 },
		{ 2, 3596, 3 },
		{ 6, 17876, 7 },
		{ 1, 1468, 2 },
		{ 11, 29402, 12 },
		{ 18, 51440, 19 },
		{ 7, 19209, 8 }
		};
		vector<shared_ptr<BTreeEntry>> entries;
		for (auto& entry : EXAMPLE_VALUES) {
			shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(-1, entry[0], BTreePageId::LEAF);
			shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(-1, entry[2], BTreePageId::LEAF);
			shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(entry[1]), leftChild, rightChild);
			entries.push_back(e);
		}
		EXAMPLE_DATA = BTreeFileEncoder::convertToInternalPage(entries,
			BufferPool::getPageSize(), Int_Type::INT_TYPE(), BTreePageId::LEAF);
	}
};
TEST_F(BTreeInternalPageTest, GetId) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(_pid->hashCode(), page->getId()->hashCode());
}

TEST_F(BTreeInternalPageTest, GetParentId) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(make_shared<BTreePageId>(_pid->getTableId(), 0, BTreePageId::ROOT_PTR)->hashCode(), page->getParentId()->hashCode());
}

TEST_F(BTreeInternalPageTest, SetParentId) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::INTERNAL);
	page->setParentId(id);
	EXPECT_EQ(id->hashCode(), page->getParentId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::LEAF);
	EXPECT_THROW(page->setParentId(id), runtime_error);

	id = make_shared<BTreePageId>(_pid->getTableId() + 1, 1, BTreePageId::INTERNAL);
	EXPECT_THROW(page->setParentId(id), runtime_error);
}

TEST_F(BTreeInternalPageTest, TestIterator) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	vector<shared_ptr<BTreeEntry>> entries;
	for (auto& entry : EXAMPLE_VALUES) {
		shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(-1, entry[0], BTreePageId::LEAF);
		shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(-1, entry[2], BTreePageId::LEAF);
		shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(entry[1]), leftChild, rightChild);
		entries.push_back(e);
	}

	BTreeFileEncoder::EntryComparator comparator;
	sort(entries.begin(), entries.end(), comparator);

	auto it = page->iterator();
	int row = 0;
	while (it->hasNext()) {
		BTreeEntry* e = it->next();
		EXPECT_EQ(entries[row]->getKey()->hashCode(), e->getKey()->hashCode());
		row++;
	}
}

TEST_F(BTreeInternalPageTest, TestReverseIterator) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	vector<shared_ptr<BTreeEntry>> entries;
	for (auto& entry : EXAMPLE_VALUES) {
		shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(-1, entry[0], BTreePageId::LEAF);
		shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(-1, entry[2], BTreePageId::LEAF);
		shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(entry[1]), leftChild, rightChild);
		entries.push_back(e);
	}
	BTreeFileEncoder::ReverseEntryComparator comparator;
	sort(entries.begin(), entries.end(), comparator);

	auto it = page->reverseIterator();
	int row = 0;
	while (it->hasNext()) {
		BTreeEntry* e = it->next();
		EXPECT_EQ(entries[row]->getKey()->hashCode(), e->getKey()->hashCode());
		row++;
	}
}

TEST_F(BTreeInternalPageTest, GetNumEmptySlots) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(483, page->getNumEmptySlots());
}

TEST_F(BTreeInternalPageTest, GetSlot) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);

	// assuming the first slot is used for the extra child pointer
	for (int i = 0; i < 21; ++i)
		EXPECT_TRUE(page->isSlotUsed(i));

	for (int i = 21; i < 504; ++i)
		EXPECT_FALSE(page->isSlotUsed(i));
}

TEST_F(BTreeInternalPageTest, TestDirty) {
	shared_ptr<TransactionId> tid = make_shared<TransactionId>();
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	page->markDirty(true, tid);
	shared_ptr<TransactionId> dirtier = page->isDirty();
	EXPECT_TRUE(dirtier != nullptr);
	EXPECT_TRUE(dirtier == tid);

	page->markDirty(false, tid);
	dirtier = page->isDirty();
	EXPECT_FALSE(dirtier != nullptr);
}

TEST_F(BTreeInternalPageTest, AddEntry) {
	// create a blank page
	vector<unsigned char> data = BTreeInternalPage::createEmptyPageData();
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, data, 0);

	// insert entries into the page
	vector<shared_ptr<BTreeEntry>> entries;
	for (auto& entry : EXAMPLE_VALUES) {
		shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(-1, entry[0], BTreePageId::LEAF);
		shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(-1, entry[2], BTreePageId::LEAF);
		shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(entry[1]), leftChild, rightChild);
		entries.push_back(e);
		page->insertEntry(e.get());
	}

	// check that the entries are ordered by the key and
	// all child pointers are present
	BTreeFileEncoder::EntryComparator comparator;
	sort(entries.begin(), entries.end(), comparator);
	auto it0 = page->iterator();
	int childPtr = 1;
	for (auto e : entries) {
		BTreeEntry* next = it0->next();
		EXPECT_TRUE(e->getKey()->equals(*next->getKey()));
		EXPECT_TRUE(next->getLeftChild()->getPageNumber() == childPtr);
		EXPECT_TRUE(next->getRightChild()->getPageNumber() == ++childPtr);
	}

	// now insert entries until the page fills up
	int free = page->getNumEmptySlots();

	// NOTE(ghuo): this nested loop existence check is slow, but it
	// shouldn't make a difference for n = 503 slots.

	for (int i = 0; i < free; ++i) {
		shared_ptr<BTreeEntry> addition = BTreeUtility::getBTreeEntry(i + 21, 70000 + i, _pid->getTableId());
		page->insertEntry(addition.get());
		EXPECT_EQ(free - i - 1, page->getNumEmptySlots());

		// loop through the iterator to ensure that the entry actually exists
		// on the page
		auto it = page->iterator();
		bool found = false;
		while (it->hasNext()) {
			BTreeEntry* e = it->next();
			if (e->getKey()->equals(*addition->getKey()) && e->getLeftChild()->equals(*addition->getLeftChild()) &&
				e->getRightChild()->equals(*addition->getRightChild())) {
				found = true;
				// verify that the RecordId is sane
				EXPECT_TRUE(page->getId()->equals(*e->getRecordId()->getPageId()));
				break;
			}
		}
		EXPECT_TRUE(found);
	}
	auto tmp = BTreeUtility::getBTreeEntry(0, 5, _pid->getTableId());
	// now, the page should be full.
	EXPECT_THROW(page->insertEntry(tmp.get()), runtime_error);
}

TEST_F(BTreeInternalPageTest, DeleteNonexistentEntry) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	auto tmp = BTreeUtility::getBTreeEntry(2);
	EXPECT_THROW(page->deleteKeyAndRightChild(tmp.get()), runtime_error);
}

TEST_F(BTreeInternalPageTest, DeleteEntry) {
	shared_ptr<BTreeInternalPage> page = make_shared<BTreeInternalPage>(_pid, EXAMPLE_DATA, 0);
	int free = page->getNumEmptySlots();

	// first, build a list of the entries on the page.
	auto it = page->iterator();
	deque<BTreeEntry*> entries;
	while (it->hasNext())
		entries.push_back(it->next());
	BTreeEntry* first = entries[0];

	// now, delete them one-by-one from both the front and the end.
	int deleted = 0;
	
	while (entries.size() > 0) {
		auto front = entries.front();
		entries.pop_front();
		auto back = entries.back();
		entries.pop_back();
		page->deleteKeyAndRightChild(front);
		page->deleteKeyAndRightChild(back);
		deleted += 2;
		EXPECT_EQ(free + deleted, page->getNumEmptySlots());
	}

	// now, the page should be empty.
	EXPECT_THROW(page->deleteKeyAndRightChild(first), runtime_error);
}