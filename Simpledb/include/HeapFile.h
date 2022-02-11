#pragma once
#include"DbFile.h"

namespace Simpledb {
	class HeapFile : public DbFile {
	public:
		HeapFile(File& f, shared_ptr<TupleDesc> td);
		File& getFile();
		int getId()override;
		const TupleDesc* getTupleDesc()override;
		const Page* readPage(const PageId& pid)override;
		bool writePage(const Page& page)override;
		int numPages();
		list<const Page*> insertTuple(const TransactionId& tid, const Tuple& t)override;
		list<const Page*> deleteTuple(const TransactionId& tid, const Tuple& t)override;
		DbFileIterator* iterator(const TransactionId& tid)override;
	private:

	};
}