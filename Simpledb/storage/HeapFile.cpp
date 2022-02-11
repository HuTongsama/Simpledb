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
	int HeapFile::getId()
	{
		return 0;
	}
	const TupleDesc* HeapFile::getTupleDesc()
	{
		return nullptr;
	}
	const Page* HeapFile::readPage(const PageId& pid)
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
	list<const Page*> HeapFile::insertTuple(const TransactionId& tid, const Tuple& t)
	{
		return list<const Page*>();
	}
	list<const Page*> HeapFile::deleteTuple(const TransactionId& tid, const Tuple& t)
	{
		return list<const Page*>();
	}
	DbFileIterator* HeapFile::iterator(const TransactionId& tid)
	{
		return nullptr;
	}
}