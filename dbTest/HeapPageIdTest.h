#pragma once
#include"SimpleDbTestBase.h"
#include"HeapPageId.h"
using namespace Simpledb;
class HeapPageIdTest : public SimpleDbTestBase {

protected:
	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_pid = make_shared<HeapPageId>(1, 1);
	}
	shared_ptr<HeapPageId> _pid;
};

TEST_F(HeapPageIdTest, GetTableId) {
	EXPECT_EQ(1, _pid->getTableId());
}

TEST_F(HeapPageIdTest, Pageno) {
	EXPECT_EQ(1, _pid->getPageNumber());
}

TEST_F(HeapPageIdTest, TestHashCode) {
    size_t code1, code2;

    // NOTE(ghuo): the hashCode could be anything. test determinism,
    // at least.
    _pid = make_shared<HeapPageId>(1, 1);
    code1 = _pid->hashCode();
    EXPECT_EQ(code1, _pid->hashCode());
    EXPECT_EQ(code1, _pid->hashCode());

    _pid = make_shared<HeapPageId>(2, 2);
    code2 = _pid->hashCode();
    EXPECT_EQ(code2, _pid->hashCode());
    EXPECT_EQ(code2, _pid->hashCode());
}

TEST_F(HeapPageIdTest, Equals) {
    HeapPageId pid1(1, 1);
    HeapPageId pid1Copy(1, 1);
    HeapPageId pid2(2, 2);

    EXPECT_TRUE(pid1.equals(pid1));
    EXPECT_TRUE(pid1.equals(pid1Copy));
    EXPECT_TRUE(pid1Copy.equals(pid1));
    EXPECT_TRUE(pid2.equals(pid2));
    
    EXPECT_FALSE(pid1.equals(pid2));
    EXPECT_FALSE(pid1Copy.equals(pid2));
    EXPECT_FALSE(pid2.equals(pid1));
    EXPECT_FALSE(pid2.equals(pid1Copy));
}