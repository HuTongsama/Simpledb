#pragma once
#include"HeapPageTestBase.h"
class HeapPageReadTest : public HeapPageTestBase {};
TEST_F(HeapPageReadTest, GetId) {
    HeapPage page(_pid, EXAMPLE_DATA);
    EXPECT_EQ(_pid, page.getId());
}

TEST_F(HeapPageReadTest, TestIterator) {
    HeapPage page(_pid, EXAMPLE_DATA);
    auto& it = page.iterator();

    int row = 0;
    while (it->hasNext()) {
        Tuple& tup = it->next();
        shared_ptr<IntField> f0 = dynamic_pointer_cast<IntField>(tup.getField(0));
        shared_ptr<IntField> f1 = dynamic_pointer_cast<IntField>(tup.getField(1));
        EXPECT_EQ(EXAMPLE_VALUES[row][0], f0->getValue());
        EXPECT_EQ(EXAMPLE_VALUES[row][1], f1->getValue());
        row++;
    }
}

TEST_F(HeapPageReadTest, GetNumEmptySlots) {
    HeapPage page(_pid, EXAMPLE_DATA);
    EXPECT_EQ(484, page.getNumEmptySlots());
}

TEST_F(HeapPageReadTest, GetSlot) {
    HeapPage page(_pid, EXAMPLE_DATA);
    for (int i = 0; i < 20; ++i) {
        EXPECT_TRUE(page.isSlotUsed(i));
    }

    for (int i = 20; i < 504; ++i) {
        EXPECT_FALSE(page.isSlotUsed(i));
    }
}