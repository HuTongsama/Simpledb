#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"TestUtil.h"
#include"Utility.h"
#include"BTreeUtility.h"
#include"BTreeLeafPage.h"
#include"BTreeFileEncoder.h"

using namespace Simpledb;
class BTreeLeafPageTest : public SimpleDbTestBase {
protected:
	shared_ptr<BTreePageId> _pid;
	// these entries have been carefully chosen to be valid entries when
	// inserted in order. Be careful if you change them!
	vector<vector<int>> EXAMPLE_VALUES;
	vector<unsigned char> EXAMPLE_DATA;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_pid = make_shared<BTreePageId>(-1, -1, BTreePageId::LEAF);
		Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(-1, Utility::getTupleDesc(2)), SystemTestUtil::getUUID());
		EXAMPLE_VALUES = {
		{ 31933, 862 },
		{ 29402, 56883 },
		{ 1468, 5825 },
		{ 17876, 52278 },
		{ 6350, 36090 },
		{ 34784, 43771 },
		{ 28617, 56874 },
		{ 19209, 23253 },
		{ 56462, 24979 },
		{ 51440, 56685 },
		{ 3596, 62307 },
		{ 45569, 2719 },
		{ 22064, 43575 },
		{ 42812, 44947 },
		{ 22189, 19724 },
		{ 33549, 36554 },
		{ 9086, 53184 },
		{ 42878, 33394 },
		{ 62778, 21122 },
		{ 17197, 16388 }
		};
		// Build the input table
		vector<shared_ptr<Tuple>> tuples;
		for (auto& tuple : EXAMPLE_VALUES) {
			shared_ptr<Tuple> tup = make_shared<Tuple>(Utility::getTupleDesc(2));
			for (int i = 0; i < tuple.size(); i++) {
				tup->setField(i, make_shared<IntField>(tuple[i]));
			}
			tuples.push_back(tup);
		}
		EXAMPLE_DATA = BTreeFileEncoder::convertToLeafPage(tuples,
			BufferPool::getPageSize(), 2, { Int_Type::INT_TYPE(), Int_Type::INT_TYPE() }, 0);
	}
};

TEST_F(BTreeLeafPageTest, GetId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(_pid->hashCode(), page->getId()->hashCode());
}

TEST_F(BTreeLeafPageTest, GetParentId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(make_shared<BTreePageId>(_pid->getTableId(), 0, BTreePageId::ROOT_PTR)->hashCode(), page->getParentId()->hashCode());
}

TEST_F(BTreeLeafPageTest, GetLeftSiblingId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(page->getLeftSiblingId(), nullptr);
}

TEST_F(BTreeLeafPageTest, GetRightSiblingId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(page->getRightSiblingId(), nullptr);
}

TEST_F(BTreeLeafPageTest, SetParentId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::INTERNAL);
	page->setParentId(id);
	EXPECT_EQ(id->hashCode(), page->getParentId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::LEAF);
	EXPECT_THROW(page->setParentId(id), runtime_error);
}

TEST_F(BTreeLeafPageTest, SetLeftSiblingId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::LEAF);
	page->setLeftSiblingId(id);
	EXPECT_EQ(id->hashCode(), page->getLeftSiblingId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::INTERNAL);
	EXPECT_THROW(page->setLeftSiblingId(id), runtime_error);
}

TEST_F(BTreeLeafPageTest, SetRightSiblingId) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::LEAF);
	page->setRightSiblingId(id);
	EXPECT_EQ(id->hashCode(), page->getRightSiblingId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId() + 1, 1, BTreePageId::LEAF);
	EXPECT_THROW(page->setRightSiblingId(id), runtime_error);
}

TEST_F(BTreeLeafPageTest, TestIterator) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	vector<shared_ptr<Tuple>> tuples;
	for (auto tuple : EXAMPLE_VALUES) {
		shared_ptr<Tuple> tup =  make_shared<Tuple>(Utility::getTupleDesc(2));
		for (int i = 0; i < tuple.size(); i++) {
			tup->setField(i, make_shared<IntField>(tuple[i]));
		}
		tuples.push_back(tup);
	}
	BTreeFileEncoder::TupleComparator comparator(0);
	sort(tuples.begin(), tuples.end(), comparator);
	auto it = page->iterator();
	int row = 0;
	while (it->hasNext()) {
		Tuple* tup = it->next();
		EXPECT_EQ(tuples[row]->getField(0)->hashCode(), tup->getField(0)->hashCode());
		EXPECT_EQ(tuples[row]->getField(1)->hashCode(), tup->getField(1)->hashCode());
		row++;
	}
}

TEST_F(BTreeLeafPageTest, GetNumEmptySlots) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_EQ(482, page->getNumEmptySlots());
}

TEST_F(BTreeLeafPageTest, GetSlot) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);

	for (int i = 0; i < 20; ++i)
		EXPECT_TRUE(page->isSlotUsed(i));

	for (int i = 20; i < 502; ++i)
		EXPECT_FALSE(page->isSlotUsed(i));
}

TEST_F(BTreeLeafPageTest, TestDirty) {
	shared_ptr<TransactionId> tid = make_shared<TransactionId>();
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	page->markDirty(true, tid);
	shared_ptr<TransactionId> dirtier = page->isDirty();
	EXPECT_TRUE(dirtier != nullptr);
	EXPECT_TRUE(dirtier == tid);

	page->markDirty(false, tid);
	dirtier = page->isDirty();
	EXPECT_FALSE(dirtier != nullptr);
}

TEST_F(BTreeLeafPageTest, AddTuple) {
	// create two blank pages -- one keyed on the first field, 
	// the second keyed on the second field
	vector<unsigned char> data = BTreeLeafPage::createEmptyPageData();
	shared_ptr<BTreeLeafPage> page0 = make_shared<BTreeLeafPage>(_pid, data, 0);
	shared_ptr<BTreeLeafPage> page1 = make_shared<BTreeLeafPage>(_pid, data, 1);

	// insert tuples into both pages
	vector<shared_ptr<Tuple>> tuples;
	for (auto& tuple : EXAMPLE_VALUES) {
		shared_ptr<Tuple> tup = make_shared<Tuple>(Utility::getTupleDesc(2));
		for (int i = 0; i < tuple.size(); i++) {
			tup->setField(i, make_shared<IntField>(tuple[i]));
		}
		tuples.push_back(tup);
		page0->insertTuple(tup);
		page1->insertTuple(tup);
	}

	BTreeFileEncoder::TupleComparator comparator(0);
	// check that the tuples are ordered by field 0 in page0
	sort(tuples.begin(), tuples.end(), comparator);
	auto it0 = page0->iterator();
	for (auto& tup : tuples) {
		EXPECT_TRUE(tup->equals(*it0->next()));
	}

	// check that the tuples are ordered by field 1 in page1
	comparator = BTreeFileEncoder::TupleComparator(1);
	sort(tuples.begin(), tuples.end(), comparator);
	auto it1 = page1->iterator();
	for (auto& tup : tuples) {
		EXPECT_TRUE(tup->equals(*it1->next()));
	}

	// now insert tuples until the page fills up
	int free = page0->getNumEmptySlots();

	// NOTE(ghuo): this nested loop existence check is slow, but it
	// shouldn't make a difference for n = 502 slots.

	for (int i = 0; i < free; ++i) {
		shared_ptr<Tuple> addition = BTreeUtility::getBTreeTuple(i, 2);
		page0->insertTuple(addition);
		EXPECT_EQ(free - i - 1, page0->getNumEmptySlots());

		// loop through the iterator to ensure that the tuple actually exists
		// on the page
		auto it = page0->iterator();
		bool found = false;
		while (it->hasNext()) {
			Tuple* tup = it->next();
			if (TestUtil::compareTuples(*addition, *tup)) {
				found = true;

				// verify that the RecordId is sane
				EXPECT_EQ(page0->getId()->hashCode(), tup->getRecordId()->getPageId()->hashCode());
				break;
			}
		}
		EXPECT_TRUE(found);
	}

	// now, the page should be full.
	EXPECT_THROW(page0->insertTuple(BTreeUtility::getBTreeTuple(0, 2)), runtime_error);
}

TEST_F(BTreeLeafPageTest, DeleteNonexistentTuple) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	EXPECT_THROW(page->deleteTuple(*BTreeUtility::getBTreeTuple(2, 2)), runtime_error);
}

TEST_F(BTreeLeafPageTest, DeleteTuple) {
	shared_ptr<BTreeLeafPage> page = make_shared<BTreeLeafPage>(_pid, EXAMPLE_DATA, 0);
	int free = page->getNumEmptySlots();

	// first, build a list of the tuples on the page.
	auto it = page->iterator();
	deque<shared_ptr<Tuple>> tuples;
	while (it->hasNext())
		tuples.push_back(make_shared<Tuple>(it->next()));
	shared_ptr<Tuple> first = tuples.front();

	// now, delete them one-by-one from both the front and the end.
	int deleted = 0;
	while (tuples.size() > 0) {
		auto front = tuples.front();
		tuples.pop_front();
		auto back = tuples.back();
		tuples.pop_back();
		page->deleteTuple(*front);
		page->deleteTuple(*back);
		deleted += 2;
		EXPECT_EQ(free + deleted, page->getNumEmptySlots());
	}

	// now, the page should be empty.
	EXPECT_THROW(page->deleteTuple(*first), runtime_error);
}