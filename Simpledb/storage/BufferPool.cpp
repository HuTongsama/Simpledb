#include"BufferPool.h"
#include"Database.h"
namespace Simpledb 
{
	int BufferPool::DEFAULT_PAGE_SIZE = 4096;
	int BufferPool::DEFAULT_PAGES = 50;
	int BufferPool::_pageSize = BufferPool::DEFAULT_PAGE_SIZE;
	BufferPool::BufferPool(int numPages)
	{
		_numPages = numPages;
		_curPages = 0;
	}
	BufferPool::~BufferPool()
	{
	}
	shared_ptr<Page> BufferPool::getPage(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid, Permissions perm)
	{
		lock_guard<mutex> lock(_mutex);
		if (tid == nullptr || pid == nullptr)
			return nullptr;
		size_t pidHashCode = pid->hashCode();
		if (_idToPage.find(pidHashCode) == _idToPage.end()) {
			if (_curPages == _numPages) {
				throw runtime_error("too many pages in bufferPool");
			}
			size_t tableId = pid->getTableId();
			shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableId);
			_idToPage[pidHashCode] = dbFile->readPage(pid);
		}
		return _idToPage[pidHashCode];
	}
	void BufferPool::unsafeReleasePage(const TransactionId& tid, const PageId& pid)
	{
	}
	void BufferPool::transactionComplete(shared_ptr<TransactionId> tid)
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