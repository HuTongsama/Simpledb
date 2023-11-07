#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"TestUtil.h"
#include"Utility.h"
#include"BTreeHeaderPage.h"

using namespace Simpledb;
class BTreeHeaderPageTest : public SimpleDbTestBase {
protected:
	shared_ptr<BTreePageId> _pid;
	vector<unsigned char> EXAMPLE_DATA;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_pid = make_shared<BTreePageId>(-1, -1, BTreePageId::HEADER);
		Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(-1, Utility::getTupleDesc(2)), SystemTestUtil::getUUID());
		EXAMPLE_DATA = BTreeHeaderPage::createEmptyPageData();
	}
};

TEST_F(BTreeHeaderPageTest, GetId) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(_pid->hashCode(), page->getId()->hashCode());
}

TEST_F(BTreeHeaderPageTest, GetPrevPageId) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(page->getPrevPageId(), nullptr);
}

TEST_F(BTreeHeaderPageTest, GetNextPageId) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(page->getNextPageId(), nullptr);
}

TEST_F(BTreeHeaderPageTest, SetPrevPageId) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::HEADER);
	page->setPrevPageId(id);
	EXPECT_EQ(id->hashCode(), page->getPrevPageId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::INTERNAL);
	EXPECT_THROW(page->setPrevPageId(id), runtime_error);
}

TEST_F(BTreeHeaderPageTest, SetNextPageId) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::HEADER);
	page->setNextPageId(id);
	EXPECT_EQ(id->hashCode(), page->getNextPageId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId() + 1, 1, BTreePageId::HEADER);
	EXPECT_THROW(page->setNextPageId(id), runtime_error);
}

TEST_F(BTreeHeaderPageTest, NumSlots) {
	EXPECT_EQ(32704, BTreeHeaderPage::getNumSlots());
}

TEST_F(BTreeHeaderPageTest, GetEmptySlot) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(0, page->getEmptySlot());
	page->init();
	EXPECT_EQ(-1, page->getEmptySlot());
	page->markSlotUsed(50, false);
	EXPECT_EQ(50, page->getEmptySlot());
}

TEST_F(BTreeHeaderPageTest, GetSlot) {
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	page->init();
	for (int i = 0; i < 20; ++i) {
		page->markSlotUsed(i, false);
	}

	for (int i = 0; i < 20; i += 2) {
		page->markSlotUsed(i, true);
	}

	for (int i = 0; i < 20; ++i) {
		if (i % 2 == 0)
			EXPECT_TRUE(page->isSlotUsed(i));
		else
			EXPECT_FALSE(page->isSlotUsed(i));
	}

	for (int i = 20; i < 32704; ++i)
		EXPECT_TRUE(page->isSlotUsed(i));

	EXPECT_EQ(1, page->getEmptySlot());
}

TEST_F(BTreeHeaderPageTest, GetPageData) {
	shared_ptr<BTreeHeaderPage> page0 = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	page0->init();
	for (int i = 0; i < 20; ++i) {
		page0->markSlotUsed(i, false);
	}

	for (int i = 0; i < 20; i += 2) {
		page0->markSlotUsed(i, true);
	}

	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, page0->getPageData());

	for (int i = 0; i < 20; ++i) {
		if (i % 2 == 0)
			EXPECT_TRUE(page->isSlotUsed(i));
		else
			EXPECT_FALSE(page->isSlotUsed(i));
	}

	for (int i = 20; i < 32704; ++i)
		EXPECT_TRUE(page->isSlotUsed(i));

	EXPECT_EQ(1, page->getEmptySlot());
}

TEST_F(BTreeHeaderPageTest, TestDirty) {
	shared_ptr<TransactionId> tid = make_shared<TransactionId>();
	shared_ptr<BTreeHeaderPage> page = make_shared<BTreeHeaderPage>(_pid, EXAMPLE_DATA);
	page->markDirty(true, tid);
	shared_ptr<TransactionId> dirtier = page->isDirty();
	EXPECT_TRUE(dirtier != nullptr);
	EXPECT_TRUE(dirtier == tid);

	page->markDirty(false, tid);
	dirtier = page->isDirty();
	EXPECT_FALSE(dirtier != nullptr);
}