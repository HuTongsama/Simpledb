#pragma once
#include "SimpleDbTestBase.h"
#include "HeapPageId.h"
#include "HeapFileEncoder.h"
#include "Utility.h"
#include "HeapPage.h"
#include "IntField.h"
#include "TestUtil.h"
#include "SystemTestUtil.h"
#include "boost/filesystem.hpp"
using namespace Simpledb;
namespace fs = boost::filesystem;
class HeapPageReadTest : public SimpleDbTestBase {
protected:
	void SetUp()override {
    
        vector<vector<int>> table;
        for (auto& tuple : EXAMPLE_VALUES) {
            vector<int> listTuple;
            for (int value : tuple) {
                listTuple.push_back(value);
            }
            table.push_back(listTuple);
        }

        // Convert it to a HeapFile and read in the bytes
        fs::path p = fs::current_path();
        string filename = p.string() + "\\" + "table.dat";
        File temp(filename.c_str(), "wb+");
        temp.deleteOnExit();
        HeapFileEncoder::convert(table, temp, BufferPool::getPageSize(), 2);
        EXAMPLE_DATA = TestUtil::readFileBytes(temp.fileName());

        _pid = make_shared<HeapPageId>(-1, -1);
        Database::getCatalog()->addTable(
            make_shared<TestUtil::SkeletonFile>(-1, Utility::getTupleDesc(2)), SystemTestUtil::getUUID());

    }
    const vector<vector<int>> EXAMPLE_VALUES = {
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
    vector<unsigned char> EXAMPLE_DATA;
	shared_ptr<HeapPageId> _pid;
};
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