#pragma once
#include"Type.h"
#include"Tuple.h"
#include"HeapFile.h"
#include<vector>
#include<list>
namespace Simpledb {
	
	/** Helper methods used for testing and implementing random features. */
	class Utility {
	public:
		/**
		* @return a Type array of length len populated with Type.INT_TYPE()
		*/
		static vector<shared_ptr<Type>> getTypes(int len);
		/**
		* @return a String array of length len populated with the (possibly null) strings in val,
		* and an appended increasing integer at the end (val1, val2, etc.).
		*/
		static vector<string> getStrings(int len, const string& val);
		/**
		* @return a TupleDesc with n fields of type Type.INT_TYPE(), each named
		* name + n (name1, name2, etc.).
		*/
		static shared_ptr<TupleDesc> getTupleDesc(int n, const string& name);
		/**
		* @return a TupleDesc with n fields of type Type.INT_TYPE()
		*/
		static shared_ptr<TupleDesc> getTupleDesc(int n);
		/**
		* @return a Tuple with a single IntField with value n and with
		*   RecordId(HeapPageId(1,2), 3)
		*/
		static shared_ptr<Tuple> getHeapTuple(int n);
		/**
		* @return a Tuple with an IntField for every element of tupdata
		*   and RecordId(HeapPageId(1, 2), 3)
		*/
		static shared_ptr<Tuple> getHeapTuple(const vector<int>& tupdata);
		/**
		* @return a Tuple with a 'width' IntFields each with value n and
		*   with RecordId(HeapPageId(1, 2), 3)
		*/
		static shared_ptr<Tuple> getHeapTuple(int n, int width);
		/**
		* @return a Tuple with a 'width' IntFields with the value tupledata[i]
		*         in each field.
		*         do not set it's RecordId, hence do not distinguish which
		*         sort of file it belongs to.
		*/
		static shared_ptr<Tuple> getTuple(const vector<int>& tupledata, int width);
		/**
		* A utility method to create a new HeapFile with a single empty page,
		* assuming the path does not already exist. If the path exists, the file
		* will be overwritten. The new table will be added to the Catalog with
		* the specified number of columns as IntFields.
		*/
		static shared_ptr<HeapFile> createEmptyHeapFile(const string& path, int cols);
		/** Opens a HeapFile and adds it to the catalog.
		*
		* @param cols number of columns in the table.
		* @param f location of the file storing the table.
		* @return the opened table.
		*/
		static shared_ptr<HeapFile> openHeapFile(int cols, shared_ptr<File> f);
		static shared_ptr<HeapFile> openHeapFile(int cols, const string& colPrefix,shared_ptr<File> f, shared_ptr<TupleDesc> td);
		static shared_ptr<HeapFile> openHeapFile(int cols, const string& colPrefix, shared_ptr<File> f);
		static string vectorToString(const vector<int>& list);
	};
}