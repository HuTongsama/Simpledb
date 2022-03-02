#include"HeapFile.h"
#pragma warning(disable:4996)
namespace Simpledb {
	HeapFile::HeapFile(shared_ptr<File> f, shared_ptr<TupleDesc> td)
	{
	}
	shared_ptr<File> HeapFile::getFile()
	{
		return nullptr;
	}
	size_t HeapFile::getId()
	{
		return 0;
	}
	shared_ptr<TupleDesc> HeapFile::getTupleDesc()
	{
		return nullptr;
	}
	shared_ptr<Page> HeapFile::readPage(const PageId& pid)
	{
		return nullptr;
	}
	void HeapFile::writePage(shared_ptr<Page> page)
	{
		return;
	}
	int HeapFile::numPages()
	{
		return 0;
	}
	list<shared_ptr<Page>> HeapFile::insertTuple(const TransactionId& tid, const Tuple& t)
	{
		return list<shared_ptr<Page>>();
	}
	list<shared_ptr<Page>> HeapFile::deleteTuple(const TransactionId& tid, const Tuple& t)
	{
		return list<shared_ptr<Page>>();
	}
	Tuple& HeapFile::HeapFileIterator::next()
	{
		shared_ptr<TupleDesc> td;
		static Tuple t(td);
		return t;
	}
	/*DbFileIterator& HeapFile::iterator(const TransactionId& tid)
	{
		
	}*/
}