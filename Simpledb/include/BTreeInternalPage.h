#pragma once
#include"BTreePage.h"
#include"DataStream.h"
#include"BTreeEntry.h"
#include"Iterator.h"
namespace Simpledb {
	/**
	 * Each instance of BTreeInternalPage stores data for one page of a BTreeFile and
	 * implements the Page interface that is used by BufferPool.
	 *
	 * @see BTreeFile
	 * @see BufferPool
	 *
	 */
	class BTreeInternalPage : public BTreePage
	{
	public:
		void checkRep(shared_ptr<Field> lowerBound, shared_ptr<Field> upperBound,
			bool checkOccupancy, int depth);
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
		 * @param data - the raw data of this page
		 * @param key - the field which the index is keyed on
		 */
		BTreeInternalPage(shared_ptr<BTreePageId> id, const vector<unsigned char>& data, int key);
		/**
		 * Retrieve the maximum number of entries this page can hold. (The number of keys)
		 */
		int getMaxEntries();
		/** Return a view of this page before it was modified
		-- used by recovery */
		shared_ptr<Page> getBeforeImage()override;
		void setBeforeImage()override;
		/**
		 * Generates a byte array representing the contents of this page.
		 * Used to serialize this page to disk.
		 * <p>
		 * The invariant here is that it should be possible to pass the byte
		 * array generated by getPageData to the BTreeInternalPage constructor and
		 * have it produce an identical BTreeInternalPage object.
		 *
		 * @see #BTreeInternalPage
		 * @return A byte array correspond to the bytes of this page.
		 */
		vector<unsigned char> getPageData()override;
		/**
		 * Delete the specified entry (key + right child pointer) from the page. The recordId
		 * is used to find the specified entry, so it must not be null. After deletion, the
		 * entry's recordId should be set to null to reflect that it is no longer stored on
		 * any page.
		 * @throws DbException if this entry is not on this page, or entry slot is
		 *         already empty.
		 * @param e The entry to delete
		 */
		void deleteKeyAndRightChild(BTreeEntry* e);
		/**
		 * Delete the specified entry (key + left child pointer) from the page. The recordId
		 * is used to find the specified entry, so it must not be null. After deletion, the
		 * entry's recordId should be set to null to reflect that it is no longer stored on
		 * any page.
		 * @throws DbException if this entry is not on this page, or entry slot is
		 *         already empty.
		 * @param e The entry to delete
		 */
		void deleteKeyAndLeftChild(BTreeEntry* e);
		/**
		 * Update the key and/or child pointers of an entry at the location specified by its
		 * record id.
		 * @param e - the entry with updated key and/or child pointers
		 * @throws DbException if this entry is not on this page, entry slot is
		 *         already empty, or updating this key would put the entry out of
		 *         order on the page
		 */
		void updateEntry(BTreeEntry* e);
		/**
		 * Adds the specified entry to the page; the entry's recordId should be updated to
		 * reflect that it is now stored on this page.
		 * @throws DbException if the page is full (no empty slots) or key field type,
		 *         table id, or child page category is a mismatch, or the entry is invalid
		 * @param e The entry to add.
		 */
		void insertEntry(BTreeEntry* e);
		/**
		 * Returns the number of entries (keys) currently stored on this page
		 */
		int getNumEntries();
		/**
		 * Returns the number of empty slots on this page.
		 */
		size_t getNumEmptySlots()override;
		/**
		 * Returns true if associated slot on this page is filled.
		 */
		bool isSlotUsed(size_t i)override;
		/**
		 * @return an iterator over all entries on this page (calling remove on this iterator throws an UnsupportedOperationException)
		 * (note that this iterator shouldn't return entries in empty slots!)
		 */
		shared_ptr<Iterator<BTreeEntry>> iterator();
		shared_ptr<Iterator<BTreeEntry>> reverseIterator();
		/**
		 * protected method used by the iterator to get the ith key out of this page
		 * @param i - the index of the key
		 * @return the ith key
		 * @throws runtime error
		 */
		shared_ptr<Field> getKey(int i);
		/**
		 * protected method used by the iterator to get the ith child page id out of this page
		 * @param i - the index of the child page id
		 * @return the ith child page id
		 * @throws runtime error
		 */
		shared_ptr<BTreePageId> getChildId(int i);
	private:
		/**
		 * Computes the number of bytes in the header of a B+ internal page with each entry occupying entrySize bytes
		 * @return the number of bytes in the header
		 */
		int getHeaderSize();
		/**
		 * Read keys from the source file.
		 */
		shared_ptr<Field> readNextKey(DataStream& dis, int slotId);
		/**
		 * Read child pointers from the source file.
		 */
		int readNextChild(DataStream& dis, int slotId);
		/**
		 * Delete the specified entry (key + 1 child pointer) from the page. The recordId
		 * is used to find the specified entry, so it must not be null. After deletion, the
		 * entry's recordId should be set to null to reflect that it is no longer stored on
		 * any page.
		 * @throws DbException if this entry is not on this page, or entry slot is
		 *         already empty.
		 * @param e The entry to delete
		 * @param deleteRightChild - if true, delete the right child. Otherwise
		 *        delete the left child
		 */
		void deleteEntry(BTreeEntry* e, bool deleteRightChild);
		/**
		 * Move an entry from one slot to another slot, and update the corresponding
		 * headers
		 */
		void moveEntry(int from, int to);
		/**
		 * Abstraction to fill or clear a slot on this page.
		 */
		void markSlotUsed(int i, bool value);

		vector<unsigned char> _header;
		vector<shared_ptr<Field>> _keys;
		vector<int> _children;
		int _numSlots;
		int _childCategory; // either leaf or internal
	};

	class BTreeInternalPageIterator : public Iterator<BTreeEntry> {	
	public:
		BTreeInternalPageIterator(BTreeInternalPage* p);
		bool hasNext()override;
		BTreeEntry* next()override;
	private:
		int _curEntry = 1;
		shared_ptr<BTreePageId> _prevChildId = nullptr;
		shared_ptr<BTreeEntry> _nextToReturn = nullptr;
		BTreeInternalPage* _p;
	};

	class BTreeInternalPageReverseIterator : public Iterator<BTreeEntry> {
	public:
		BTreeInternalPageReverseIterator(BTreeInternalPage* p);
		bool hasNext()override;
		BTreeEntry* next()override;
	private:
		int _curEntry = 1;
		shared_ptr<BTreePageId> _nextChildId = nullptr;
		shared_ptr<BTreeEntry> _nextToReturn = nullptr;
		BTreeInternalPage* _p;
	};
}