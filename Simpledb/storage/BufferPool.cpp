#include"BufferPool.h"
namespace Simpledb 
{
	int BufferPool::DEFAULT_PAGE_SIZE = 4096;
	int BufferPool::DEFAULT_PAGES = 50;
	int BufferPool::_pageSize = BufferPool::DEFAULT_PAGE_SIZE;
	BufferPool::BufferPool(int numPages)
	{
	}
	const Page* BufferPool::getPage(const TransactionId& tid, const PageId& pid, Permissions perm)
	{
		return nullptr;
	}
	void BufferPool::unsafeReleasePage(const TransactionId& tid, const PageId& pid)
	{
	}
	void BufferPool::transactionComplete(const TransactionId& tid)
	{
	}
	bool BufferPool::holdsLock(const TransactionId& tid, const PageId& p)
	{
		return false;
	}
	void BufferPool::transactionComplete(const TransactionId& tid, bool commit)
	{
	}
	void BufferPool::insertTuple(const TransactionId& tid, int tableId, const Tuple& t)
	{
	}
	void BufferPool::deleteTuple(const TransactionId& tid, const Tuple& t)
	{
	}
	void BufferPool::flushAllPages()
	{
	}
	void BufferPool::discardPage(const PageId& pid)
	{
	}
	void BufferPool::flushPage(const PageId& pid)
	{
	}
	void BufferPool::flushPages(const TransactionId& tid)
	{
	}
	void BufferPool::evictPage()
	{
	}
}