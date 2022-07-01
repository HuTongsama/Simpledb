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
		_lockManager.accessPermission(perm, tid, pid);
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
		transactionComplete(tid, true);
	}

	void BufferPool::transactionComplete(shared_ptr<TransactionId> tid, bool commit)
	{
		auto pids = _lockManager.getRelatedPageIds(tid);
		if (commit) {
			flushPages(tid);
		}
		else {
			lock_guard<mutex> guard(_mutex);
			for (auto id : pids) {
				if (_idToPageInfo.find(id) != _idToPageInfo.end()) {
					shared_ptr<Page> page = _idToPageInfo[id]._page;
					if (page->isDirty()) {
						shared_ptr<PageId> pid = page->getId();
						size_t tableId = pid->getTableId();
						shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableId);
						_idToPageInfo[id]._page = dbFile->readPage(pid);
					}
				}
				else {
					throw runtime_error("error page id");
				}
			}
		}
		_lockManager.transactionComplete(tid);
	}

	bool BufferPool::holdsLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		return _lockManager.holdsLock(tid, pid);
	}
	
	void BufferPool::insertTuple(shared_ptr<TransactionId> tid, size_t tableId, shared_ptr<Tuple> t)
	{
		shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableId);
		vector<shared_ptr<Page>> pages = dbFile->insertTuple(tid, t);
		for (auto& page : pages) {
			page->markDirty(true, tid);
		}
		for (auto& page : pages) {
			_lockManager.unlockPage(tid, page->getId());
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
					page->markDirty(true, tid);
				}
				for (auto& page : pages) {
					_lockManager.unlockPage(tid, page->getId());
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
			}
		}
	}
	void BufferPool::discardPage(shared_ptr<PageId> pid)
	{
		lock_guard<mutex> lock(_mutex);
		for (auto& iter : _idToPageInfo) {
			if (iter.second._page->getId()->equals(*pid)) {
				_idToPageInfo.erase(iter.first);
				break;
			}
		}
	}
	void BufferPool::flushPage(shared_ptr<PageId> pid)
	{
		lock_guard<mutex> lock(_mutex);
		flushPageInner(pid);
	}
	void BufferPool::flushPages(shared_ptr<TransactionId> tid)
	{
		lock_guard<mutex> lock(_mutex);
		auto pids = _lockManager.getRelatedPageIds(tid);
		for (auto id : pids) {
			if (_idToPageInfo.find(id) != _idToPageInfo.end()) {
				shared_ptr<Page> page = _idToPageInfo[id]._page;
				if (page->isDirty()) {
					flushPageInner(page);
				}
			}
			else {
				throw runtime_error("flush error page");
			}
		}
	}
	void BufferPool::evictPage()
	{
		lock_guard<mutex> lock(_mutex);
		evictPageInner();
	}
	void BufferPool::flushPageInner(shared_ptr<PageId> pid)
	{
		
		shared_ptr<Page> p = nullptr;
		for (auto& iter : _idToPageInfo) {
			if (iter.second._page->getId()->equals(*pid))
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
		p->markDirty(false, nullptr);
	}
	void BufferPool::evictPageInner()
	{
		clock_t t = -1;
		size_t id = 0;
		for (auto& iter : _idToPageInfo) {
			if (_idToPageInfo[iter.first]._page->isDirty())
				continue;
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
			_lockManager.deletePageLock(info._page->getId());
			_idToPageInfo.erase(id);
		}
		else {
			throw runtime_error("evict error");
		}
	}
}