#pragma once
#include"PageId.h"
#include<string>
#include<stdexcept>
#include<functional>
#include<sstream>
namespace Simpledb {

	/** Unique identifier for BTreeInternalPage, BTreeLeafPage, BTreeHeaderPage
	 *  and BTreeRootPtrPage objects.
	 */
	class BTreePageId : public PageId {

	public:
		static const int ROOT_PTR = 0;
		static const int INTERNAL = 1;
		static const int LEAF = 2;
		static const int HEADER = 3;

		

	static string categToString(int categ) {
		switch (categ) {
			case ROOT_PTR:
				return "ROOT_PTR";
			case INTERNAL:
				return "INTERNAL";
			case LEAF:
				return "LEAF";
			case HEADER:
				return "HEADER";
			default:
				throw runtime_error("categ");
			}
		}

	/**
	 * Constructor. Create a page id structure for a specific page of a
	 * specific table.
	 *
	 * @param tableId The table that is being referenced
	 * @param pgNo The page number in that table.
	 * @param pgcateg which kind of page it is
	 */
	BTreePageId(size_t tableId, size_t pgNo, size_t pgcateg)
		: _tableId(tableId), _pgNo(pgNo),_pgcateg(pgcateg) {
		stringstream ss;
		ss << tableId << pgNo << pgcateg;
		string s = ss.str();
		hash<string> hashFunc;
		_hashCode = hashFunc(s);
	}

	/** @return the table associated with this PageId */
	size_t getTableId()const override {
		return _tableId;
	}

	/**
	 * @return the page number in the table getTableId() associated with
	 *   this PageId
	 */
	size_t getPageNumber()const override {
		return _pgNo;
	}

	/**
	 * @return the category of this page
	 */
	int pgcateg() {
		return _pgcateg;
	}

	/**
	 * @return a hash code for this page, represented by the combination of
	 *   the table number, page number, and pgcateg (needed if a PageId is used as a
	 *   key in a hash table in the BufferPool, for example.)
	 * @see BufferPool
	 */
	size_t hashCode()const override {
		return _hashCode;
	}

	/**
	 * Compares one PageId to another.
	 *
	 * @param o The object to compare against (must be a PageId)
	 * @return true if the objects are equal (e.g., page numbers, table
	 *   ids and pgcateg are the same)
	 */
	bool equals(const PageId& pId)const override {
		try
		{
			const BTreePageId& bId = dynamic_cast<const BTreePageId&>(pId);
			return bId._hashCode == _hashCode;
		}
		catch (const std::exception&)
		{
			return false;
		}
	}

	string toString() {
		return "(tableId: " + to_string(_tableId) +
			", pgNo: " + to_string(_pgNo) +
			", pgcateg: " + categToString(_pgcateg) +
			")";
	}

	/**
	 *  Return a representation of this object as an array of
	 *  integers, for writing to disk.  Size of returned array must contain
	 *  number of integers that corresponds to number of args to one of the
	 *  constructors.
	 */
	vector<size_t> serialize() {
		vector<size_t> data = {
			_tableId,_pgNo,_pgcateg
		};
		return data;
	}
private:
		size_t _tableId;
		size_t _pgNo;
		size_t _pgcateg;
		size_t _hashCode;
	};
}