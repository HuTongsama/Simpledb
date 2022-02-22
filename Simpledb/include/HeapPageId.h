#pragma once
#include"PageId.h"
namespace Simpledb {
	/** Unique identifier for HeapPage objects. */
	class HeapPageId : public PageId {
	public:
		/**
		* Constructor. Create a page id structure for a specific page of a
		* specific table.
		*
		* @param tableId The table that is being referenced
		* @param pgNo The page number in that table.
		*/
		HeapPageId(size_t tableId, size_t pgNo);
		/**
		*  Return a representation of this object as an array of
		*  integers, for writing to disk.  Size of returned array must contain
		*  number of integers that corresponds to number of args to one of the
		*  constructors.
		*/
		vector<int> serialize()const override;
		/** @return the table associated with this PageId */
		size_t getTableId()const override;
		/**
		* @return the page number in the table getTableId() associated with
		*   this PageId
		*/
		size_t getPageNumber()const override;
		/**
		* @return a hash code for this page, represented by a combination of
		*   the table number and the page number (needed if a PageId is used as a
		*   key in a hash table in the BufferPool, for example.)
		* @see BufferPool
		*/
		size_t hashCode()const override;
		/**
		* Compares one PageId to another.
		*
		* @param o The object to compare against (must be a PageId)
		* @return true if the objects are equal (e.g., page numbers and table
		*   ids are the same)
		*/
		bool equals(const PageId& pId)const override;
	};
}
