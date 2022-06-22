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
		if (_idToPageInfo.find(pidHashCode) == _idToPageInfo.end()) {
			if (_idToPageInfo.size() == _numPages) {
				evictPageInner();
			}
			size_t tableId = pid->getTableId();
			shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableId);
			_idToPageInfo[pidHashCode]._page = dbFile->readPage(pid);
		}
		_idToPageInfo[pidHashCode]._lastGetTime = clock();
		return _idToPageInfo[pidHashCode]._page;
	}
	void BufferPool::unsafeReleasePage(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		_lockManager.unlockPage(tid, pid);
	}
	void BufferPool::transactionComplete(shared_ptr<TransactionId> tid)
	{
	}
	bool BufferPool::holdsLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		return _lockManager.holdsLock(tid, pid);
	}
	void BufferPool::transactionComplete(shared_ptr<TransactionId> tid, bool commit)
	{
	}
	void BufferPool::insertTuple(shared_ptr<TransactionId> tid, size_t tableId, shared_ptr<Tuple> t)
	{
		shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableId);
		vector<shared_ptr<Page>> pages = dbFile->insertTuple(tid, t);
		for (auto& page : pages) {
			dbFile->writePage(page);
		}
	}
	void BufferPool::deleteTuple(shared_ptr<TransactionId> tid, Tuple& t)
	{
		auto idIter = Database::getCatalog()->tableIdIterator();
		while (idIter->hasNext())
		{
			size_t tableId = idIter->next();
			shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableId);
			vector<shared_ptr<Page>> pages = dbFile->deleteTuple(tid, t);
			if (!pages.empty()) {
				for (auto& page : pages) {
					dbFile->writePage(page);
				}
				break;
			}
		}
	}
	void BufferPool::flushAllPages()
	{
		lock_guard<mutex> lock(_mutex);
		for (auto& iter : _idToPageInfo) {
			shared_ptr<Page> p = iter.second._page;
			if (p->isDirty()) {
				flushPageInner(p);
				p->markDirty(false, nullptr);
			}
		}
	}
	void BufferPool::discardPage(const PageId& pid)
	{
		lock_guard<mutex> lock(_mutex);
		for (auto& iter : _idToPageInfo) {
			if (iter.second._page->getId()->equals(pid)) {
				_idToPageInfo.erase(iter.first);
				break;
			}
		}
	}
	void BufferPool::flushPage(const PageId& pid)
	{
		lock_guard<mutex> lock(_mutex);
		flushPageInner(pid);
	}
	void BufferPool::flushPages(const TransactionId& tid)
	{
	}
	void BufferPool::evictPage()
	{
		lock_guard<mutex> lock(_mutex);
		evictPageInner();
	}
	void BufferPool::flushPageInner(const PageId& pid)
	{
		
		shared_ptr<Page> p = nullptr;
		for (auto& iter : _idToPageInfo) {
			if (iter.second._page->getId()->equals(pid))
			{
				p = iter.second._page;
				break;
			}
		}
		flushPageInner(p);
	}
	void BufferPool::flushPageInner(shared_ptr<Page> p)
	{
		if (nullptr == p)
			return;
		size_t tableId = p->getId()->getTableId();
		shared_ptr<DbFile> dbfile = Database::getCatalog()->getDatabaseFile(tableId);
		dbfile->writePage(p);
	}
	void BufferPool::evictPageInner()
	{
		clock_t t = -1;
		size_t id = 0;
		for (auto& iter : _idToPageInfo) {
			if (-1 == t) {
				id = iter.first;
				t = iter.second._lastGetTime;
			}
			else {
				if (t > iter.second._lastGetTime)
				{
					t = iter.second._lastGetTime;
					id = iter.first;
				}
			}
		}
		if (t != -1) {
			PageInfo& info = _idToPageInfo[id];
			if (info._page->isDirty())
				flushPageInner(info._page);
			_idToPageInfo.erase(id);
		}
	}
}