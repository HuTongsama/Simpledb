#pragma once
#include"Page.h"
#include"Permissions.h"
#include"Tuple.h"
#include<mutex>
#include<map>
using namespace std;
namespace Simpledb {
	/**
	* BufferPool manages the reading and writing of pages into memory from
	* disk. Access methods call into it to retrieve pages, and it fetches
	* pages from the appropriate location.
	* <p>
	* The BufferPool is also responsible for locking;  when a transaction fetches
	* a page, BufferPool checks that the transaction has the appropriate
	* locks to read/write the page.
	*
	* @Threadsafe, all fields are final
	*/
	class BufferPool {
	public:
		/** Bytes per page, including header. */
		static int DEFAULT_PAGE_SIZE;
		/** Default number of pages passed to the constructor. This is used by
		other classes. BufferPool should use the numPages argument to the
		constructor instead. */
		static int DEFAULT_PAGES;
		/**
		* Creates a BufferPool that caches up to numPages pages.
		*
		* @param numPages maximum number of pages in this buffer pool.
		*/
		BufferPool(int numPages);
		~BufferPool();
		static int getPageSize() {
			return _pageSize;
		}
		// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
		void setPageSize(int pageSize) {
			_pageSize = pageSize;
		}

		// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
		void resetPageSize() {
			_pageSize = DEFAULT_PAGE_SIZE;
		}
		void lockBufferPool() {}
		void unlockBufferPool() {}
		/**
		* Retrieve the specified page with the associated permissions.
		* Will acquire a lock and may block if that lock is held by another 
		* transaction.
		* <p>
		* The retrieved page should be looked up in the buffer pool.  If it 
		* is present, it should be returned.  If it is not present, it should 
		* be added to the buffer pool and returned.  If there is insufficient 
		* space in the buffer pool, a page should be evicted and the new page 
		* should be added in its place.
		*
		* @param tid the ID of the transaction requesting the page
		* @param pid the ID of the requested page
		* @param perm the requested permissions on the page
		*/
		shared_ptr<Page> getPage(shared_ptr<TransactionId> tid,shared_ptr<PageId> pid, Permissions perm);
		/**
		* Releases the lock on a page.
		* Calling this is very risky, and may result in wrong behavior. Think hard
		* about who needs to call this and why, and why they can run the risk of
		* calling it.
		*
		* @param tid the ID of the transaction requesting the unlock
		* @param pid the ID of the page to unlock
		*/
		void unsafeReleasePage(const TransactionId& tid,const PageId& pid);// not necessary for lab1|lab2
		/**
		* Release all locks associated with a given transaction.
		*
		* @param tid the ID of the transaction requesting the unlock
		*/
		void transactionComplete(shared_ptr<TransactionId> tid);// not necessary for lab1|lab2
		/** Return true if the specified transaction has a lock on the specified page */
		bool holdsLock(const TransactionId& tid, const PageId& p);// not necessary for lab1|lab2
		/**
		* Commit or abort a given transaction; release all locks associated to
		* the transaction.
		*
		* @param tid the ID of the transaction requesting the unlock
		* @param commit a flag indicating whether we should commit or abort
		*/
		void transactionComplete(const TransactionId& tid, bool commit);// not necessary for lab1|lab2
		/**
		* Add a tuple to the specified table on behalf of transaction tid.  Will
		* acquire a write lock on the page the tuple is added to and any other
		* pages that are updated (Lock acquisition is not needed for lab2).
		* May block if the lock(s) cannot be acquired.
		*
		* Marks any pages that were dirtied by the operation as dirty by calling
		* their markDirty bit, and adds versions of any pages that have
		* been dirtied to the cache (replacing any existing versions of those pages) so
		* that future requests see up-to-date pages.
		*
		* @param tid the transaction adding the tuple
		* @param tableId the table to add the tuple to
		* @param t the tuple to add
		*/
		void insertTuple(shared_ptr<TransactionId> tid, size_t tableId,shared_ptr<Tuple> t);
		/**
		* Remove the specified tuple from the buffer pool.
		* Will acquire a write lock on the page the tuple is removed from and any
		* other pages that are updated. May block if the lock(s) cannot be acquired.
		*
		* Marks any pages that were dirtied by the operation as dirty by calling
		* their markDirty bit, and adds versions of any pages that have
		* been dirtied to the cache (replacing any existing versions of those pages) so
		* that future requests see up-to-date pages.
		*
		* @param tid the transaction deleting the tuple.
		* @param t the tuple to delete
		*/
		void deleteTuple(shared_ptr<TransactionId> tid, Tuple& t);
		/**
		* Flush all dirty pages to disk.
		* NB: Be careful using this routine -- it writes dirty data to disk so will
		*     break simpledb if running in NO STEAL mode.
		*/
		void flushAllPages();//synchronized , not necessary for lab1
		/** Remove the specific page id from the buffer pool.
		Needed by the recovery manager to ensure that the
		buffer pool doesn't keep a rolled back page in its
		cache.

		Also used by B+ tree files to ensure that deleted pages
		are removed from the cache so they can be reused safely
		*/
		void discardPage(const PageId& pid);//synchronized , not necessary for lab1
		/**
		* Flushes a certain page to disk
		* @param pid an ID indicating the page to flush
		*/
		void flushPage(const PageId& pid);//synchronized , not necessary for lab1
		/** Write all pages of the specified transaction to disk.*/
		void flushPages(const TransactionId& tid);//synchronized , not necessary for lab1 | lab2
		/**
		* Discards a page from the buffer pool.
		* Flushes the page to disk to ensure dirty pages are updated on disk.
		*/
		void evictPage();//synchronized , not necessary for lab1

	private:
		static int _pageSize;
		size_t _numPages;
		size_t _curPages;
		map<size_t, shared_ptr<Page>> _idToPage;
		mutex _mutex;
	};
}