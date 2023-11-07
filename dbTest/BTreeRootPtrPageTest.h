#pragma once
#include"SimpleDbTestBase.h"
#include"BTreeRootPtrPage.h"
#include"TestUtil.h"
#include"SystemTestUtil.h"
#include"BTreeFileEncoder.h"

using namespace Simpledb;
class BTreeRootPtrPageTest :public SimpleDbTestBase {
protected:
	shared_ptr<BTreePageId> _pid;
	vector<unsigned char> EXAMPLE_DATA;
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_pid = make_shared<BTreePageId>(-1, -1, BTreePageId::ROOT_PTR);
		Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(-1, Utility::getTupleDesc(2)), SystemTestUtil::getUUID());
		EXAMPLE_DATA = BTreeFileEncoder::convertToRootPtrPage(1, BTreePageId::LEAF, 2);
	}
};

TEST_F(BTreeRootPtrPageTest, GetId) {
	shared_ptr<BTreeRootPtrPage> page = make_shared<BTreeRootPtrPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(_pid->hashCode(), page->getId()->hashCode());
}

TEST_F(BTreeRootPtrPageTest, GetRootId) {
	shared_ptr<BTreeRootPtrPage> page = make_shared<BTreeRootPtrPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::LEAF)->hashCode(), page->getRootId()->hashCode());
}

TEST_F(BTreeRootPtrPageTest, SetRootId) {
	shared_ptr<BTreeRootPtrPage> page = make_shared<BTreeRootPtrPage>(_pid, EXAMPLE_DATA);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::INTERNAL);
	page->setRootId(id);
	EXPECT_EQ(id->hashCode(), page->getRootId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId(), 1, BTreePageId::ROOT_PTR);
	EXPECT_THROW(page->setRootId(id), runtime_error);
	id = make_shared<BTreePageId>(_pid->getTableId() + 1, 1, BTreePageId::INTERNAL);
	EXPECT_THROW(page->setRootId(id), runtime_error);
}

TEST_F(BTreeRootPtrPageTest, GetHeaderId) {
	shared_ptr<BTreeRootPtrPage> page = make_shared<BTreeRootPtrPage>(_pid, EXAMPLE_DATA);
	EXPECT_EQ(make_shared<BTreePageId>(_pid->getTableId(), 2, BTreePageId::HEADER)->hashCode(), page->getHeaderId()->hashCode());
}

TEST_F(BTreeRootPtrPageTest, SetHeaderId) {
	shared_ptr<BTreeRootPtrPage> page = make_shared<BTreeRootPtrPage>(_pid, EXAMPLE_DATA);
	shared_ptr<BTreePageId> id = make_shared<BTreePageId>(_pid->getTableId(), 3, BTreePageId::HEADER);
	page->setHeaderId(id);
	EXPECT_EQ(id->hashCode(), page->getHeaderId()->hashCode());

	id = make_shared<BTreePageId>(_pid->getTableId(), 2, BTreePageId::ROOT_PTR);
	EXPECT_THROW(page->setHeaderId(id), runtime_error);

	id = make_shared<BTreePageId>(_pid->getTableId() + 1, 1, BTreePageId::HEADER);
	EXPECT_THROW(page->setHeaderId(id), runtime_error);
}

TEST_F(BTreeRootPtrPageTest, TestDirty) {
	shared_ptr<TransactionId> tid = make_shared<TransactionId>();
	shared_ptr<BTreeRootPtrPage> page = make_shared<BTreeRootPtrPage>(_pid, EXAMPLE_DATA);
	page->markDirty(true, tid);
	shared_ptr<TransactionId> dirtier = page->isDirty();
	EXPECT_TRUE(dirtier != nullptr);
	EXPECT_TRUE(dirtier == tid);

	page->markDirty(false, tid);
	dirtier = page->isDirty();
	EXPECT_FALSE(dirtier != nullptr);
}