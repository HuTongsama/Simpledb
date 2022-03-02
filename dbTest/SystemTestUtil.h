#pragma once
#include<string>
#include<vector>
#include<map>
#include<random>
#include"Utility.h"
#include"File.h"
#include"Database.h"
#include"HeapFileEncoder.h"
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
        default_random_engine r = default_random_engine(time(0));

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
        shared_ptr<File> temp = make_shared<File>("table.dat");
        temp->deleteOnExit();
        HeapFileEncoder::convert(tuples, *temp, BufferPool::getPageSize(), columns);
        return temp;
    }


	/**
	* Generates a unique string each time it is called.
	* @return a new unique UUID as a string, using boost
	*/
	static string getUUID() {
		return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
	}

};