#pragma once
#include<string>
#include<vector>
#include<map>
#include<random>
#include<algorithm>
#include<chrono>
#include"Utility.h"
#include"File.h"
#include"Database.h"
#include"HeapFileEncoder.h"
#include"OpIterator.h"
#include"SeqScan.h"
#include"IntField.h"
#include"boost/uuid/uuid.hpp"
#include"boost/uuid/uuid_io.hpp"
#include"boost/uuid/uuid_generators.hpp"
#include"boost/lexical_cast.hpp"

using namespace std;
using namespace Simpledb;
class SystemTestUtil {
private:
    static int MAX_RAND_VALUE;
public:
    
    /** @param columnSpecification Mapping between column index and value. */
    static shared_ptr<HeapFile> createRandomHeapFile(
        int columns, int rows,
        map<int,int>& columnSpecification,
        vector<vector<int>>& tuples){
        return createRandomHeapFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples);
    }

    /** @param columnSpecification Mapping between column index and value. */
    static shared_ptr<HeapFile> createRandomHeapFile(
        int columns, int rows, int maxValue, map<int,int>& columnSpecification,
        vector<vector<int>>& tuples){
        shared_ptr<File> temp = createRandomHeapFileUnopened(columns, rows, maxValue,
            columnSpecification, tuples);
        return Utility::openHeapFile(columns, temp);
    }

    static shared_ptr<HeapFile> createRandomHeapFile(
        int columns, int rows, map<int,int>& columnSpecification,
        vector<vector<int>>& tuples, string& colPrefix){
        return createRandomHeapFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples, colPrefix);
    }

    static shared_ptr<HeapFile> createRandomHeapFile(
        int columns, int rows, int maxValue, map<int,int>& columnSpecification,
        vector<vector<int>>& tuples, string& colPrefix){
        shared_ptr<File> temp = createRandomHeapFileUnopened(columns, rows, maxValue,
            columnSpecification, tuples);
        return Utility::openHeapFile(columns, colPrefix, temp);
    }

    static shared_ptr<File> createRandomHeapFileUnopened(int columns, int rows,
        int maxValue, map<int,int>& columnSpecification,
        vector<vector<int>>& tuples){
        tuples.resize(rows);
        default_random_engine r = default_random_engine(chrono::system_clock::now().time_since_epoch().count());

        // Fill the tuples list with generated values
        for (int i = 0; i < rows; ++i) {
            vector<int> tuple(columns, 0);
            for (int j = 0; j < columns; ++j) {
                // Generate random values, or use the column specification
                int columnValue = 0;
                if (columnSpecification.find(j) != columnSpecification.end()) {
                    columnValue = columnSpecification[j];
                }
                else {
                    columnValue = r() % maxValue + 1;
                }
                tuple[j] = columnValue;
            }
            tuples[i] = tuple;
        }

        // Convert the tuples list to a heap file and open it
        char nameBuf[L_tmpnam_s] = { 0 };
        tmpnam_s(nameBuf);
        shared_ptr<File> temp = make_shared<File>(nameBuf);
        temp->deleteOnExit();
        HeapFileEncoder::convert(tuples, *temp, BufferPool::getPageSize(), columns);
        return temp;
    }

    static vector<int> tupleToVector(Tuple& tuple) {
        vector<int> vec;
        size_t numFields = tuple.getTupleDesc()->numFields();
        for (int i = 0; i < numFields; ++i) {
            int value = dynamic_pointer_cast<IntField>(tuple.getField(i))->getValue();
            vec.push_back(value);
        }
        return vec;
    }
    static void matchTuples(shared_ptr<DbFile> f, vector<vector<int>>& tuples) {
    
        shared_ptr<TransactionId> tid = make_shared<TransactionId>();
        matchTuples(f, tid, tuples);
        Database::getBufferPool()->transactionComplete(tid);
    }
    static void matchTuples(shared_ptr<DbFile> f,
        shared_ptr<TransactionId> tid, vector<vector<int>>& tuples) {
        shared_ptr<SeqScan> scan = make_shared<SeqScan>(tid, f->getId(), "");
        matchTuples(scan, tuples);
    }
    static void matchTuples(shared_ptr<OpIterator> iterator, vector<vector<int>>& tuples) {
    
        vector<vector<int>> copy(tuples);

        iterator->open();
        size_t findCount = 0;
        while (iterator->hasNext()) {
            Tuple& t = iterator->next();
            vector<int> vec = tupleToVector(t);
            auto isExpected = find_if(copy.begin(), copy.end(), [&vec](vector<int>& aim) {
                return equal(vec.begin(), vec.end(), aim.begin());
                }) != copy.end();
            //printf("scanned tuple: %s (%s)", t.toString().data(), isExpected ? "expected" : "not expected");
            if (!isExpected) {
                throw runtime_error("expected tuples does not contain: " + t.toString());
            }
            else {
                findCount++;
            }
        }
        iterator->close();

        if (findCount != copy.size()) {
            stringstream ss;
            ss << "expected to find the following tuples:\n";
            int MAX_TUPLES_OUTPUT = 10;
            int count = 0;
            for (auto& t : copy) {
                if (count == MAX_TUPLES_OUTPUT) {
                    ss << "[" << copy.size() - MAX_TUPLES_OUTPUT << " more tuples]";                  
                    break;
                }
                ss << "\t" << Utility::vectorToString(t) << "\n";
                count += 1;
            }
            throw runtime_error(ss.str());
        }
    }

	/**
	* Generates a unique string each time it is called.
	* @return a new unique UUID as a string, using boost
	*/
	static string getUUID() {
		return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
	}

};