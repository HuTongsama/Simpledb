#pragma once
#include"Page.h"
#include"BTreePageId.h"
#include<mutex>
namespace Simpledb {

	/**
	 * Each instance of BTreeHeaderPage stores data for one page of a BTreeFile and
	 * implements the Page interface that is used by BufferPool.
	 *
	 * @see BTreeFile
	 * @see BufferPool
	 *
	 */
	class BTreeHeaderPage : public Page {
	public:
		static const int INDEX_SIZE;
		/**
		 * Create a BTreeHeaderPage from a set of bytes of data read from disk.
		 * The format of a BTreeHeaderPage is two pointers to the next and previous
		 * header pages, followed by a set of bytes indicating which pages in the file
		 * are used or available
		 * @see BufferPool#getPageSize()
		 *
		 */
		BTreeHeaderPage(shared_ptr<BTreePageId> id, const vector<unsigned char>& data);
		/**
		 * Initially mark all slots in the header used.
		 */
		void init();
		/**
		 * Computes the number of slots in the header
		 */
		static int getNumSlots();
		/** Return a view of this page before it was modified
			-- used by recovery */
		shared_ptr<Page> getBeforeImage()override;
		void setBeforeImage()override;
		/**
		 * @return the PageId associated with this page.
		 */
		shared_ptr<PageId> getId()const override;
		vector<unsigned char> getPageData()override;
		/**
		 * Static method to generate a byte array corresponding to an empty
		 * BTreeHeaderPage.
		 * Used to add new, empty pages to the file. Passing the results of
		 * this method to the BTreeHeaderPage constructor will create a BTreeHeaderPage with
		 * no valid data in it.
		 *
		 * @return The returned ByteArray.
		 */
		static vector<unsigned char> createEmptyPageData();
		/**
		 * Get the page id of the previous header page
		 * @return the page id of the previous header page
		 */
		shared_ptr<BTreePageId> getPrevPageId();
		/**
		 * Get the page id of the next header page
		 * @return the page id of the next header page
		 */
		shared_ptr<BTreePageId> getNextPageId();
		/**
		 * Set the page id of the previous header page
		 * @param id - the page id of the previous header page
		 * @throws runtime error
		 */
		void setPrevPageId(shared_ptr<BTreePageId> id);
		/**
		 * Set the page id of the next header page
		 * @param id - the page id of the next header page
		 * @throws runtime error
		 */
		void setNextPageId(shared_ptr<BTreePageId> id);
		/**
		 * Marks this page as dirty/not dirty and record that transaction
		 * that did the dirtying
		 */
		void markDirty(bool dirty, shared_ptr<TransactionId> tid)override;
		/**
		 * Returns the tid of the transaction that last dirtied this page, 
		 * or null if the page is not dirty
		 */
		shared_ptr<TransactionId> isDirty()const override;
		/**
		 * Returns true if the page of the BTreeFile associated with slot i is used
		 */
		bool isSlotUsed(int i);
		/**
		 * Abstraction to mark a page of the BTreeFile used or unused
		 */
		void markSlotUsed(int i, bool value);
		/**
		 * get the index of the first empty slot
		 * @return the index of the first empty slot or -1 if none exists
		 */
		int getEmptySlot();
	private:
		/**
		 * Computes the number of bytes in the header while saving room for pointers
		 */
		static int getHeaderSize();



		bool _dirty = false;
		shared_ptr<TransactionId> _dirtier = nullptr;
		shared_ptr<BTreePageId> _pid;
		vector<unsigned char> _header;
		int _numSlots;

		int _nextPage; // next header page or 0
		int _prevPage; // previous header page or 0

		vector<unsigned char> _oldData;
		mutex _oldDataLock;
	};
}