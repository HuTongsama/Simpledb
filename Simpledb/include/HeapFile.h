#pragma once
#include"DbFile.h"
#include"HeapPage.h"
#include"AbstractDbFileIterator.h"
namespace Simpledb {
	/**
	* HeapFile is an implementation of a DbFile that stores a collection of tuples
	* in no particular order. Tuples are stored on pages, each of which is a fixed
	* size, and the file is simply a collection of those pages. HeapFile works
	* closely with HeapPage. The format of HeapPages is described in the HeapPage
	* constructor.
	*/
	class HeapFile : public DbFile {
	public:
		class HeapFileIterator : public AbstractDbFileIterator {
		public:
			HeapFileIterator(size_t tableId,size_t pageCount, shared_ptr<TransactionId> tid)
				: _open(false), _tableId(tableId), _tid(tid),
				_pageNo(0),_pageCount(pageCount), _iter(nullptr) {
			}
			void open()override;
			void rewind()override;
			void close()override;
		private:
			Tuple* readNext()override;
			void readNextPage();

			bool _open;
			size_t _tableId;
			shared_ptr<TransactionId> _tid;
			size_t _pageNo;
			size_t _pageCount;
			shared_ptr<HeapPage::HeapPageIter> _iter;
		};
		/**
		* Constructs a heap file backed by the specified file.
		*
		* @param f the file that stores the on-disk backing store for this heap file.
		*/
		HeapFile(shared_ptr<File> f, shared_ptr<TupleDesc> td);
		/**
		* Returns the File backing this HeapFile on disk.
		*
		* @return the File backing this HeapFile on disk.
		*/
		shared_ptr<File> getFile();
		/**
		* Returns an ID uniquely identifying this HeapFile. Implementation note:
		* you will need to generate this tableid somewhere to ensure that each
		* HeapFile has a "unique id," and that you always return the same value for 
		* a particular HeapFile. We suggest hashing the absolute file name of the
		* file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
		*
		* @return an ID uniquely identifying this HeapFile.
		*/
		size_t getId()override;
		/**
		* Returns the TupleDesc of the table stored in this DbFile.
		*
		* @return TupleDesc of this DbFile.
		*/
		shared_ptr<TupleDesc> getTupleDesc()override;
		shared_ptr<Page> readPage(shared_ptr<PageId> pid)override;
		void writePage(shared_ptr<Page> page)override;
		/**
		* Returns the number of pages in this HeapFile.
		*/
		size_t numPages();
		list<shared_ptr<Page>> insertTuple(const TransactionId& tid, const Tuple& t)override;
		list<shared_ptr<Page>> deleteTuple(const TransactionId& tid, const Tuple& t)override;
		shared_ptr<DbFileIterator> iterator(shared_ptr<TransactionId> tid)override;
	private:
		size_t getPageOffset(size_t pageNo);

		shared_ptr<File> _file;
		shared_ptr<TupleDesc> _td;
	};
}