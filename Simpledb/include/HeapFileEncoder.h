#pragma once
#include"Type.h"
#include"DbFile.h"
namespace Simpledb {
	/**
	* HeapFileEncoder reads a comma delimited text file or accepts
	* an array of tuples and converts it to
	* pages of binary data in the appropriate format for simpledb heap pages
	* Pages are padded out to a specified length, and written consecutive in a
	* data file.
	*/
	class HeapFileEncoder {
	public:
		/** Convert the specified tuple list (with only integer fields) into a binary
		* page file. <br>
		*
		* The format of the output file will be as specified in HeapPage and
		* HeapFile.
		*
		* @see HeapPage
		* @see HeapFile
		* @param tuples the tuples - a list of tuples, each represented by a list of integers that are
		*        the field values for that tuple.
		* @param outFile The output file to write data to
		* @param npagebytes The number of bytes per page in the output file
		* @param numFields the number of fields in each input tuple
		* @returns false if failed
		*/
		static bool convert(list<list<int>> tuples, File& outFile, int npagebytes, int numFields);
		static bool convert(File& inFile, File& outFile, int npagebytes, int numFields);
		static bool convert(File& inFile, File& outFile, int npagebytes, int numFields,
			const vector<shared_ptr<Type>>& typeVec);
		/** Convert the specified input text file into a binary
		* page file. <br>
		* Assume format of the input file is (note that only integer fields are
		* supported):<br>
		* int,...,int\n<br>
		* int,...,int\n<br>
		* ...<br>
		* where each row represents a tuple.<br>
		* <p>
		* The format of the output file will be as specified in HeapPage and
		* HeapFile.
		*
		* @see HeapPage
		* @see HeapFile
		* @param inFile The input file to read data from
		* @param outFile The output file to write data to
		* @param npagebytes The number of bytes per page in the output file
		* @param numFields the number of fields in each input line/output tuple
		* @returns false if failed
		*/
		static bool convert(File& inFile, File& outFile, int npagebytes,
			int numFields, const vector<shared_ptr<Type>>& typeVec, char fieldSeparator);
	};
}