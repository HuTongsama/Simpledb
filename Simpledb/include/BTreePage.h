#pragma once
#include"Page.h"
#include"BTreePageId.h"
#include"TupleDesc.h"
#include<mutex>
namespace Simpledb {
	/**
	 * Each instance of BTreeInternalPage stores data for one page of a BTreeFile and
	 * implements the Page interface that is used by BufferPool.
	 *
	 * @see BTreeFile
	 * @see BufferPool
	 *
	 */
	class BTreePage : public Page {
	public:
		static const int INDEX_SIZE;
		/**
		 * Create a BTreeInternalPage from a set of bytes of data read from disk.
		 * The format of a BTreeInternalPage is a set of header bytes indicating
		 * the slots of the page that are in use, some number of entry slots, and extra
		 * bytes for the parent pointer, one extra child pointer (a node with m entries
		 * has m+1 pointers to children), and the category of all child pages (either
		 * leaf or internal).
		 *  Specifically, the number of entries is equal to: <p>
		 *          floor((BufferPool.getPageSize()*8 - extra bytes*8) / (entry size * 8 + 1))
		 * <p> where entry size is the size of entries in this index node
		 * (key + child pointer), which can be determined via the key field and
		 * {@link Catalog#getTupleDesc}.
		 * The number of 8-bit header words is equal to:
		 * <p>
		 *      ceiling((no. entry slots + 1) / 8)
		 * <p>
		 * @see Database#getCatalog
		 * @see Catalog#getTupleDesc
		 * @see BufferPool#getPageSize()
		 *
		 * @param id - the id of this page
		 * @param key - the field which the index is keyed on
		 */
		BTreePage(shared_ptr<BTreePageId> id, int key);
		/**
		 * @return the PageId associated with this page.
		 */
		shared_ptr<PageId> getId()const override;
		/**
		 * Static method to generate a byte array corresponding to an empty
		 * BTreePage.
		 * Used to add new, empty pages to the file. Passing the results of
		 * this method to the BTreeInternalPage or BTreeLeafPage constructor will create a BTreePage with
		 * no valid entries in it.
		 *
		 * @return The returned ByteArray.
		 */
		static vector<unsigned char> createEmptyPageData();
		/**
		 * Get the parent id of this page
		 * @return the parent id
		 */
		shared_ptr<BTreePageId> getParentId();
		/**
		 * Set the parent id
		 * @param id - the id of the parent of this page
		 * @throws DbException if the id is not valid
		 */
		void setParentId(shared_ptr<BTreePageId> id);
		/**
		 * Marks this page as dirty/not dirty and record that transaction
		 * that did the dirtying
		 */
		void markDirty(bool dirty, shared_ptr<TransactionId> tid)override;
		/**
		 * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
		 */
		shared_ptr<TransactionId> isDirty()const override;
		/**
		 * Returns the number of empty slots on this page.
		 */
		virtual size_t getNumEmptySlots() = 0;
		/**
		 * Returns true if associated slot on this page is filled.
		 */
		virtual bool isSlotUsed(size_t i) = 0;
	protected:		
		bool _dirty = false;
		shared_ptr<TransactionId> _dirtier = nullptr;
		shared_ptr<BTreePageId> _pid;
		shared_ptr<TupleDesc> _td;
		int _keyField;
		int _parent; // parent is always internal node or 0 for root node
		vector<unsigned char> _oldData;
		mutex _oldDataLock;
	};
}