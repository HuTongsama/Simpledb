#pragma once
#include"Page.h"
#include"HeapPageId.h"
#include"Tuple.h"
#include"Common.h"
#include<mutex>
namespace Simpledb {
	class HeapPage : public Page {
	public:
        class HeapPageIter : public Iterator<Tuple> {
        public:
            HeapPageIter(HeapPage* page) :_page(page) {}
            bool hasNext()override { return false; }
            Tuple& next() {
                static Tuple t(make_shared<TupleDesc>());
                return t;
            }
            void remove()override{}
        private:
            HeapPage* _page;
        };
        /**
         * Create a HeapPage from a set of bytes of data read from disk.
         * The format of a HeapPage is a set of header bytes indicating
         * the slots of the page that are in use, some number of tuple slots.
         *  Specifically, the number of tuples is equal to: <p>
         *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
         * <p> where tuple size is the size of tuples in this
         * database table, which can be determined via {@link Catalog#getTupleDesc}.
         * The number of 8-bit header words is equal to:
         * <p>
         *      ceiling(no. tuple slots / 8)
         * <p>
         * @see Database#getCatalog
         * @see Catalog#getTupleDesc
         * @see BufferPool#getPageSize()
         */
        HeapPage(shared_ptr<HeapPageId> id, const vector<unsigned char>& data);
        /**
         * @return the PageId associated with this page.
         */
        shared_ptr<PageId> getId()const override;
        /**
        * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
        */
        shared_ptr<TransactionId> isDirty()const override;
        /**
        * Marks this page as dirty/not dirty and record that transaction
        * that did the dirtying
        */
        void markDirty(bool dirty, const TransactionId& tid)override;
        /**
         * Generates a byte array representing the contents of this page.
         * Used to serialize this page to disk.
         * <p>
         * The invariant here is that it should be possible to pass the byte
         * array generated by getPageData to the HeapPage constructor and
         * have it produce an identical HeapPage object.
         *
         * @see #HeapPage
         * @return A byte array correspond to the bytes of this page.
         */
        vector<unsigned char> getPageData()const override;
        /** Return a view of this page before it was modified
            -- used by recovery */
        shared_ptr<Page> getBeforeImage()const override;
        void setBeforeImage()override;
        /**
         * Static method to generate a byte array corresponding to an empty
         * HeapPage.
         * Used to add new, empty pages to the file. Passing the results of
         * this method to the HeapPage constructor will create a HeapPage with
         * no valid tuples in it.
         *
         * @return The returned ByteArray.
         */
        static vector<unsigned char> createEmptyPageData();
        /**
         * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
         *   that it is no longer stored on any page.
         * @throws DbException if this tuple is not on this page, or tuple slot is
         *         already empty.
         * @param t The tuple to delete
         */
        void deleteTuple(const Tuple& t);
        /**
         * Adds the specified tuple to the page;  the tuple should be updated to reflect
         *  that it is now stored on this page.
         * @throws DbException if the page is full (no empty slots) or tupledesc
         *         is mismatch.
         * @param t The tuple to add.
         */
        void insertTuple(const Tuple& t);
        /**
         * Returns the number of empty slots on this page.
         */
        int getNumEmptySlots();
        /**
         * Returns true if associated slot on this page is filled.
         */
        bool isSlotUsed(int i);
        /**
         * @return an iterator over all tuples on this page (calling remove on this iterator is invalid)
         * (note that this iterator shouldn't return tuples in empty slots!)
         */
        HeapPageIter& iterator();
    private:
            /** Retrieve the number of tuples on this page.
                @return the number of tuples on this page
            */
        int getNumTuples();
            /**
             * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
             * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
             */
        int getHeaderSize();
            /**
            * Suck up tuples from the source file.
            */
        unique_ptr<Tuple> readNextTuple(const Unique_File& inFile, int slotId);          
            /**
            * Abstraction to fill or clear a slot on this page.
            */
        void markSlotUsed(int i, bool value);

        HeapPageIter _iter;
        shared_ptr<HeapPageId> _pid;
        TupleDesc _td;
        unique_ptr<unsigned char[]> _header;
        vector<unique_ptr<Tuple>> _tuples;
        int _numSlots;
        unique_ptr<unsigned char[]> _oldData;
        mutex _oldDataLock;
	};
}