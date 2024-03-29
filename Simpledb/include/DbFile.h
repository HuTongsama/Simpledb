#pragma once
#include"Page.h"
#include"TransactionId.h"
#include"Tuple.h"
#include"DbFileIterator.h"
#include"File.h"
#include<vector>
#include<mutex>
#include<stdio.h>
using namespace std;
namespace Simpledb {	
	/**
	* The interface for database files on disk. Each table is represented by a
	* single DbFile. DbFiles can fetch pages and iterate through tuples. Each
	* file has a unique id used to store metadata about the table in the Catalog.
	* DbFiles are generally accessed through the buffer pool, rather than directly
	* by operators.
	*/
	class DbFile {
	public:
		DbFile() {}
		virtual ~DbFile() {}
		/**
		* Read the specified page from disk.
		*
		* @returns null if the page does not exist in this file.
		*/
		virtual shared_ptr<Page> readPage(shared_ptr<PageId> id) = 0;
		/**
		* Push the specified page to disk.
		*
		* @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
		* @throws exception if the write fails
		*
		*/
		virtual void writePage(shared_ptr<Page> p) = 0;
		/**
		* Inserts the specified tuple to the file on behalf of transaction.
		* This method will acquire a lock on the affected pages of the file, and
		* may block until the lock can be acquired.
		*
		* @param tid The transaction performing the update
		* @param t The tuple to add.  This tuple should be updated to reflect that
		*          it is now stored in this file.
		* @return a vector contain the pages that were modified,
		*		  empty vector if the tuple cannot be added,
		*         empty if the needed file can't be read/written
		*/
		virtual vector<shared_ptr<Page>> insertTuple(shared_ptr<TransactionId> tid,shared_ptr<Tuple> t) = 0;
		/**
		* Removes the specified tuple from the file on behalf of the specified
		* transaction.
		* This method will acquire a lock on the affected pages of the file, and
		* may block until the lock can be acquired.
		*
		* @param tid The transaction performing the update
		* @param t The tuple to delete.  This tuple should be updated to reflect that
		*          it is no longer stored on any page.
		* @return a vector contain the pages that were modified,
		*		  empty vector if the tuple cannot be deleted or is not a member
		*		  of the file
		*/
		virtual vector<shared_ptr<Page>> deleteTuple(shared_ptr<TransactionId> tid, Tuple& t) = 0;
		/**
		* Returns an iterator over all the tuples stored in this DbFile. The
		* iterator must use {@link BufferPool#getPage}, rather than
		* {@link #readPage} to iterate through the pages.
		*
		* @return an iterator over all the tuples stored in this DbFile.
		*/
		virtual shared_ptr<DbFileIterator> iterator(shared_ptr<TransactionId> tid) = 0;
		/**
		* Returns a unique ID used to identify this DbFile in the Catalog. This id 
		* can be used to look up the table via {@link Catalog#getDatabaseFile} and
		* {@link Catalog#getTupleDesc}.
		* <p>
		* Implementation note:  you will need to generate this tableid somewhere,
		* ensure that each HeapFile has a "unique id," and that you always
		* return the same value for a particular HeapFile. A simple implementation
		* is to use the hash code of the absolute path of the file underlying
		* the HeapFile, i.e. <code>f.getAbsoluteFile().hashCode()</code>.
		*
		* @return an ID uniquely identifying this HeapFile.
		*/
		virtual size_t getId() = 0;
		/**
		* Returns the TupleDesc of the table stored in this DbFile.
		* @return TupleDesc of this DbFile.
		*/
		virtual shared_ptr<TupleDesc> getTupleDesc() = 0;
		/**
		* Returns the number of pages.
		*/
		virtual size_t numPages() = 0;
	protected:
		mutex _dbfileMutex;
	};
}