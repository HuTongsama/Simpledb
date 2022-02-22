#pragma once
#include"DbFile.h"
namespace Simpledb {
	class HeapFile : public DbFile {
	public:
		class HeapFileIterator : public DbFileIterator {
		public:
			bool hasNext()override { return false; }
			Tuple& next()override;
			void remove()override {}
			bool open()override { return false; }
			bool rewind()override { return false; }
			void close()override {  }
		};
		HeapFile(File& f, shared_ptr<TupleDesc> td);
		File& getFile();
		size_t getId()override;
		shared_ptr<TupleDesc> getTupleDesc()override;
		shared_ptr<Page> readPage(const PageId& pid)override;
		void writePage(const Page& page)override;
		int numPages();
		list<shared_ptr<Page>> insertTuple(const TransactionId& tid, const Tuple& t)override;
		list<shared_ptr<Page>> deleteTuple(const TransactionId& tid, const Tuple& t)override;
		DbFileIterator& iterator(const TransactionId& tid)override { return _hfIter; }
	private:
		HeapFileIterator _hfIter;
	};
}