#pragma once
#include"DbFile.h"

namespace Simpledb {
	class HeapFile : public DbFile {
	public:
		HeapFile(File& f, shared_ptr<TupleDesc> td);
		File& getFile();
		size_t getId()override;
		shared_ptr<TupleDesc> getTupleDesc()override;
		shared_ptr<Page> readPage(const PageId& pid)override;
		bool writePage(const Page& page)override;
		int numPages();
		list<shared_ptr<Page>> insertTuple(const TransactionId& tid, const Tuple& t)override;
		list<shared_ptr<Page>> deleteTuple(const TransactionId& tid, const Tuple& t)override;
		DbFileIterator& iterator(const TransactionId& tid)override {};
	private:

	};
}