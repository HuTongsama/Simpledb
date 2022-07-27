#include "LockManager.h"

namespace Simpledb {
	bool LockManager::holdsLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{		
		auto tCode = tid->getId();
		lock_guard<mutex> guard(_managerLock);
		auto pInfo = _tidToInfo[tCode];
		if (pInfo == nullptr)
			return false;
		auto pCode = pid->hashCode();
		return pInfo->isLocked(pCode, Permissions::READ_ONLY)
			|| pInfo->isLocked(pCode, Permissions::READ_WRITE);
	}
	void LockManager::unlockPage(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		auto tCode = tid->getId();
		lock_guard<mutex> guard(_managerLock);
		auto pInfo = _tidToInfo[tCode];
		if (pInfo == nullptr)
			return;
		auto pCode = pid->hashCode();
		pInfo->unlock(pCode, Permissions::READ_ONLY);
		pInfo->unlock(pCode, Permissions::READ_WRITE);
	}
	void LockManager::accessPermission(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		updateWaitforGraph(p, tid, pid);
		lock_guard<mutex> guard(_managerLock);
		auto tCode = tid->getId();
		auto pCode = pid->hashCode();

		auto pInfo = _tidToInfo[tCode];
		if (pInfo == nullptr) {
			pInfo = make_shared<TransactionLockInfo>();
			_tidToInfo[tCode] = pInfo;
		}
		if (!pInfo->holdsLock(pCode)) {
			auto pMutex = _pidToMutex[pCode];
			if (pMutex == nullptr) {
				pMutex = make_shared<shared_mutex>();
				_pidToMutex[pCode] = pMutex;
			}
			pInfo->addLock(pCode, pMutex, p);
		}
		else {
			if (p == Permissions::READ_WRITE) {
				if (pInfo->isLocked(pCode, Permissions::READ_ONLY)) {
					bool findFlag = false;
					for (auto iter : _tidToInfo) {
						if (iter.first != tCode) {
							auto tmpInfo = iter.second;
							if (tmpInfo->holdsLock(pCode) &&
								tmpInfo->isLocked(pCode, Permissions::READ_ONLY)) {
								findFlag = true;
								break;
							}
						}
					}
					if (!findFlag) {
						pInfo->unlock(pCode, Permissions::READ_ONLY);
					}
				}
			}
			else if (p == Permissions::READ_ONLY) {
				if (pInfo->isLocked(pCode, Permissions::READ_WRITE))
					return;
			}
			if (pInfo->isLocked(pCode, p)) {
				return;
			}
			else {
				pInfo->lock(pCode, p);
			}
		}
		

	}
	void LockManager::transactionComplete(shared_ptr<TransactionId> tid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto pInfo = _tidToInfo[tid->getId()];
		if (!pInfo)
			return;
		_tidToInfo.erase(tid->getId());
		_waitforGraph.deleteVertex(tid->getId());
		
	}
	vector<size_t> LockManager::getRelatedPageIds(shared_ptr<TransactionId> tid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto pInfo = _tidToInfo[tid->getId()];
		if (!pInfo)
			return {};
		return pInfo->getAllPageIds();
	}
	void LockManager::deletePageLock(shared_ptr<PageId> pid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto pCode = pid->hashCode();
		for (auto iter : _tidToInfo) {
			shared_ptr<TransactionLockInfo> pInfo = iter.second;
			pInfo->deleteLock(pCode);
		}
		auto iter = _pidToMutex.find(pCode);
		if (iter != _pidToMutex.end()) {
			_pidToMutex.erase(iter);
		}
	}

	void LockManager::updateWaitforGraph(Permissions p, shared_ptr<TransactionId>& tid, shared_ptr<PageId> pid)
	{
		auto t1 = tid->getId();
		auto p1 = pid->hashCode();
		_waitforGraph.addVertex(t1);
		for (auto iter : _tidToInfo) {
			auto t2 = iter.first;
			if (t2 == t1)
				continue;
			auto& info = iter.second;
			auto pids = info->getAllPageIds();
			auto findIter = find(pids.begin(), pids.end(), p1);
			if (findIter == pids.end()) {
				continue;
			}

			if (info->isLocked(p1, Permissions::READ_WRITE)) {
				_waitforGraph.addEdge(t2, t1);
			}
			else if (info->isLocked(p1, Permissions::READ_ONLY)
				&& p == Permissions::READ_WRITE) {
				_waitforGraph.addEdge(t1, t2);
			}
		}
		bool log = false;
		if (!_waitforGraph.isAcyclic(log)) {
			throw runtime_error("wait for graph");
		}
	}
	
	void TransactionLockInfo::addLock(size_t pid, shared_ptr<shared_mutex> pageMutex, Permissions perm)
	{
		auto iter = find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
		if (iter == _locks.end()) {
			shared_ptr<PageLock> plock = make_shared<PageLock>(pid, pageMutex);
			plock->lock(perm);
			_locks.push_back(plock);
		}
		else {
			(*iter)->lock(perm);
		}
	}

	void TransactionLockInfo::lock(size_t pid, Permissions perm)
	{
		auto iter = find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
		if (iter != _locks.end()) {
			(*iter)->lock(perm);
		}
		else {
			throw runtime_error("transaction does not hold the lock of page " + to_string(pid));
		}
	}

	void TransactionLockInfo::unlock(size_t pid, Permissions perm)
	{
		auto iter = find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
		if (iter != _locks.end()) {
			(*iter)->unlock(perm);
		}
		else {
			throw runtime_error("transaction does not hold the lock of page " + to_string(pid));
		}
	}

	bool TransactionLockInfo::isLocked(size_t pid, Permissions perm)
	{
		auto iter = find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
		if (iter != _locks.end()) {
			return (*iter)->isLocked(perm);
		}
		else {
			throw runtime_error("transaction does not hold the lock of page " + to_string(pid));
		}
		return false;
	}

	bool TransactionLockInfo::holdsLock(size_t pid)
	{
		auto iter = find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
		if (iter == _locks.end())
			return false;
		return true;
	}

	void TransactionLockInfo::deleteLock(size_t pid)
	{
		auto iter = find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
		if (iter == _locks.end()) {
			return;
		}
		else {
			if ((*iter)->isLocked(Permissions::READ_WRITE)) {
				throw runtime_error("delete a lock that is still exclusive locked");
			}
			_locks.erase(iter);
			
		}
	}

	vector<size_t> TransactionLockInfo::getAllPageIds()
	{
		vector<size_t> result;
		for (auto& lock : _locks) {
			result.push_back(lock->getPageId());
		}
		return result;
	}


	void PageLock::lock(Permissions perm)
	{
		bool repeatLock = false;
		switch (perm)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (_rlock.owns_lock()) {
				repeatLock = true;
			}
			else {
				_rlock.lock();
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (_wlock.owns_lock()) {
				repeatLock = true;
			}
			else {
				_wlock.lock();
			}
			break;
		default:
			throw runtime_error("error permission type");
			break;
		}
		if (repeatLock) {
			throw runtime_error("repeat lock");
		}
	}

	void PageLock::unlock(Permissions perm)
	{
		switch (perm)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (_rlock.owns_lock()) {
				_rlock.unlock();
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (_wlock.owns_lock()) {
				_wlock.unlock();
			}
			break;
		default:
			throw runtime_error("error permission type");
			break;
		}
	}

	bool PageLock::isLocked(Permissions perm)
	{
		switch (perm)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (_rlock.owns_lock()) {
				return true;
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (_wlock.owns_lock()) {
				return true;
			}
			break;
		default:
			throw runtime_error("error permission type");
			break;
		}
		return false;
	}

}
