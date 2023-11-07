#pragma once
#include"SimpleDbTestBase.h"
#include"BTreePageId.h"

using namespace Simpledb;
class BTreePageIdTest :public SimpleDbTestBase {
protected:
	shared_ptr<BTreePageId> _rootPtrId;
	shared_ptr<BTreePageId> _internalId;
	shared_ptr<BTreePageId> _leafId;
	shared_ptr<BTreePageId> _headerId;
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_rootPtrId = make_shared<BTreePageId>(1, 0, BTreePageId::ROOT_PTR);
		_internalId = make_shared<BTreePageId>(1, 1, BTreePageId::INTERNAL);
		_leafId = make_shared<BTreePageId>(1, 2, BTreePageId::LEAF);
		_headerId = make_shared<BTreePageId>(1, 3, BTreePageId::HEADER);
	}
};

TEST_F(BTreePageIdTest, GetTableId) {
	EXPECT_EQ(1, _rootPtrId->getTableId());
	EXPECT_EQ(1, _internalId->getTableId());
	EXPECT_EQ(1, _leafId->getTableId());
	EXPECT_EQ(1, _headerId->getTableId());
}

TEST_F(BTreePageIdTest, Pageno) {
	EXPECT_EQ(0, _rootPtrId->getPageNumber());
	EXPECT_EQ(1, _internalId->getPageNumber());
	EXPECT_EQ(2, _leafId->getPageNumber());
	EXPECT_EQ(3, _headerId->getPageNumber());
}

TEST_F(BTreePageIdTest, TestHashCode) {
	size_t code1, code2, code3, code4;

	// NOTE(ghuo): the hashCode could be anything. test determinism,
	// at least.
	code1 = _rootPtrId->hashCode();
	EXPECT_EQ(code1, _rootPtrId->hashCode());
	EXPECT_EQ(code1, _rootPtrId->hashCode());

	code2 = _internalId->hashCode();
	EXPECT_EQ(code2, _internalId->hashCode());
	EXPECT_EQ(code2, _internalId->hashCode());

	code3 = _leafId->hashCode();
	EXPECT_EQ(code3, _leafId->hashCode());
	EXPECT_EQ(code3, _leafId->hashCode());

	code4 = _headerId->hashCode();
	EXPECT_EQ(code4, _headerId->hashCode());
	EXPECT_EQ(code4, _headerId->hashCode());
}

TEST_F(BTreePageIdTest, Equals) {
	shared_ptr<BTreePageId> pid1 = make_shared<BTreePageId>(1, 1, BTreePageId::LEAF);
	shared_ptr<BTreePageId> pid1Copy = make_shared<BTreePageId>(1, 1, BTreePageId::LEAF);
	shared_ptr<BTreePageId> pid2 = make_shared<BTreePageId>(2, 2, BTreePageId::LEAF);
	shared_ptr<BTreePageId> pid3 = make_shared<BTreePageId>(1, 1, BTreePageId::INTERNAL);

	// .equals() with null should return false
	EXPECT_FALSE(nullptr == pid1);

	// .equals() with the wrong type should return false
	//EXPECT_FALSE(pid1 == make_shared<int>());

	EXPECT_TRUE(pid1->equals(*pid1));
	EXPECT_TRUE(pid1->equals(*pid1Copy));
	EXPECT_TRUE(pid1Copy->equals(*pid1));
	EXPECT_TRUE(pid2->equals(*pid2));
	EXPECT_TRUE(pid3->equals(*pid3));

	EXPECT_FALSE(pid1->equals(*pid2));
	EXPECT_FALSE(pid1Copy->equals(*pid2));
	EXPECT_FALSE(pid2->equals(*pid1));
	EXPECT_FALSE(pid2->equals(*pid1Copy));
	EXPECT_FALSE(pid1->equals(*pid3));
	EXPECT_FALSE(pid3->equals(*pid1));
}