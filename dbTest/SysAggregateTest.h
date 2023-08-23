#pragma once
#include"SimpleDbTestBase.h"
#include"Aggregate.h"
#include"DbFile.h"
#include"SeqScan.h"
#include"SystemTestUtil.h"
using namespace Simpledb;
class SysAggregateTest : public SimpleDbTestBase {
protected:
    void validateAggregate(shared_ptr<DbFile> table, Aggregator::Op operation,
        int aggregateColumn, int groupColumn, vector<vector<int>>& expectedResult) {
        shared_ptr<TransactionId> tid =  make_shared<TransactionId>();
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(tid, table->getId(), "");
        shared_ptr<Aggregate> ag = make_shared<Aggregate>(ss, aggregateColumn, groupColumn, operation);

        SystemTestUtil::matchTuples(ag, expectedResult);
        Database::getBufferPool()->transactionComplete(tid);
    }

    int computeAggregate(vector<int>& values, Aggregator::Op operation) {
        if (operation == Aggregator::Op::COUNT)
            return static_cast<int>(values.size());

        int value = 0;
        if (operation == Aggregator::Op::MIN)
            value = INT_MAX;
        else if (operation == Aggregator::Op::MAX)
            value = INT_MIN;

        for (int v : values) {
            switch (operation)
            {
            case Simpledb::Aggregator::Op::MIN:
                if (v < value) value = v;
                break;
            case Simpledb::Aggregator::Op::MAX:
                if (v > value) value = v;
                break;
            case Simpledb::Aggregator::Op::SUM:
            case Simpledb::Aggregator::Op::AVG:
                value += v;
                break;
            default:
                throw runtime_error("Unsupported operation" + Aggregator::opToString(operation));
            }
        }

        if (operation == Aggregator::Op::AVG)
            value /= static_cast<int>(values.size());
        return value;
    }
    
    vector<vector<int>> aggregate(vector<vector<int>>& tuples, Aggregator::Op operation, int groupColumn) {
        // Group the values
        map<int, vector<int>> values;
        for (auto& t : tuples) {
            int key = -1;
            if (groupColumn != Aggregator::NO_GROUPING)
                key = t.at(groupColumn);
            int value = t.at(1);

            values[key].push_back(value);
        }

        vector<vector<int>> results;
        for (auto& e : values) {
            vector<int> result;
            if (groupColumn != Aggregator::NO_GROUPING)
                result.push_back(e.first);
            result.push_back(computeAggregate(e.second, operation));
            results.push_back(result);
        }
        return results;
    }

    void doAggregate(Aggregator::Op operation, int groupColumn) {
        // Create the table
        vector<vector<int>> createdTuples;
        shared_ptr<HeapFile> table = SystemTestUtil::createRandomHeapFile(
            COLUMNS, ROWS, MAX_VALUE, map<int, int>(), createdTuples);

        // Compute the expected answer
        vector<vector<int>> expected =
            aggregate(createdTuples, operation, groupColumn);

        // validate that we get the answer
        validateAggregate(table, operation, 1, groupColumn, expected);
    }
    
    
    
    
    const int ROWS = 1024;
    const int MAX_VALUE = 64;
    const int COLUMNS = 3;
};

TEST_F(SysAggregateTest, TestSum) {
    doAggregate(Aggregator::Op::SUM, 0);
}

TEST_F(SysAggregateTest, TestMin) {
    doAggregate(Aggregator::Op::MIN, 0);
}

TEST_F(SysAggregateTest, TestMax) {
    doAggregate(Aggregator::Op::MAX, 0);
}

TEST_F(SysAggregateTest, TestCount) {
    doAggregate(Aggregator::Op::COUNT, 0);
}

TEST_F(SysAggregateTest, TestAverage) {
    doAggregate(Aggregator::Op::AVG, 0);
}

TEST_F(SysAggregateTest, TestAverageNoGroup) {
    doAggregate(Aggregator::Op::AVG, Aggregator::NO_GROUPING);
}
