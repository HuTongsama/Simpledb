#pragma once
#include<memory>
#include<vector>
using namespace std;
namespace Simpledb 
{
	/** PageId is an interface to a specific page of a specific table. */
	class PageId
	{
	public:
		virtual ~PageId() {}
		/** Return a representation of this page id object as a collection of
		integers (used for logging)

		This class MUST have a constructor that accepts n integer parameters,
		where n is the number of integers returned in the array from serialize.
		*/
		virtual vector<int> serialize()const = 0;
		/** @return the unique tableid hashcode with this PageId */
		virtual size_t getTableId()const = 0;
		virtual size_t getPageNumber()const = 0;
		/**
		* @return a hash code for this page, represented by the concatenation of
		*   the table number and the page number (needed if a PageId is used as a
		*   key in a hash table in the BufferPool, for example.)
		* @see BufferPool
		*/
		virtual size_t hashCode() = 0;
		/**
		* Compares one PageId to another.
		*
		* @param o The object to compare against (must be a PageId)
		* @return true if the objects are equal (e.g., page numbers and table
		*   ids are the same)
		*/
		virtual bool equals(PageId& pId) = 0;	
	};
}