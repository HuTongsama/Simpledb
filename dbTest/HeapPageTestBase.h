#pragma once
#include"SimpleDbTestBase.h"
#include"HeapPage.h"
#include"Utility.h"
#include"TestUtil.h"
#include"SystemTestUtil.h"
#include "boost/filesystem.hpp"
using namespace Simpledb;
namespace fs = boost::filesystem;
class HeapPageTestBase : public SimpleDbTestBase {
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
        File temp(filename.c_str());
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