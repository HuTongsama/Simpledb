#include"HeapFile.h"
#pragma warning(disable:4996)
namespace Simpledb {
	HeapFile::HeapFile(File& f, shared_ptr<TupleDesc> td)
	{
	}
	File& HeapFile::getFile()
	{
		static File f(tmpfile());
		return f;
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
	bool HeapFile::writePage(const Page& page)
	{
		return false;
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
	/*DbFileIterator& HeapFile::iterator(const TransactionId& tid)
	{
		
	}*/
}