#include "LockManager.h"
#include<iostream>
namespace Simpledb {
	bool LockManager::holdsLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{		
		auto tCode = tid->getId();
		lock_guard<mutex> guard(_managerLock);
		auto pInfo = getInfo(tid->getId());
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
		auto pInfo = getInfo(tid->getId());
		if (pInfo == nullptr)
			return;
		auto pCode = pid->hashCode();
		pInfo->unlock(pCode, Permissions::READ_ONLY);
		pInfo->unlock(pCode, Permissions::READ_WRITE);
		deleteTid(tCode);
	}
	
	void LockManager::accessPermission(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		updateWaitforGraph(p, tid, pid);
		
		auto tCode = tid->getId();
		auto pCode = pid->hashCode();

		auto pInfo = getInfo(tid->getId());
		{
			lock_guard<mutex> guard(_managerLock);
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
				addTid(pCode, tCode);
			}
			
		}
		
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
				else {
					pInfo->setUpgradeLock(pCode, true);
				}
			}
		}
		else if (p == Permissions::READ_ONLY) {
			if (pInfo->isLocked(pCode, Permissions::READ_WRITE))
				return;
		}
		if (!pInfo->isLocked(pCode, p)) {
			pInfo->lock(pCode, p);
		}
		
		
	}
	
	void LockManager::transactionComplete(shared_ptr<TransactionId> tid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto pInfo = getInfo(tid->getId());
		if (!pInfo)
			return;
		auto tCode = tid->getId();
		_tidToInfo.erase(tCode);
		_waitforGraph.deleteVertex(tid->getId());
		deleteTid(tCode);

		for (auto iter : _tidToInfo) {
			auto tid = iter.first;
			auto info = iter.second;
			auto pids = info->getAllPageIds();

			for (auto pid : pids) {
				auto& tidVec = _pidToTids[pid];
				if (tidVec.size() == 1 && tidVec[0] == tid) {
					info->upgradeLock(pid);
				}
			}
		}
		
	}
	
	vector<size_t> LockManager::getRelatedPageIds(shared_ptr<TransactionId> tid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto pInfo = getInfo(tid->getId());
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
		lock_guard<mutex> guard(_managerLock);
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
		if (!_waitforGraph.isAcyclic()) {
			throw runtime_error("waitforGraph exist cycle");
		}
	}

	shared_ptr<TransactionLockInfo> LockManager::getInfo(int64_t tid)
	{
		if (_tidToInfo.find(tid) != _tidToInfo.end()) {
			return _tidToInfo[tid];
		}
		return nullptr;
	}

	void LockManager::addTid(size_t pid, size_t tid)
	{
		auto& vec = _pidToTids[pid];
		if (find(vec.begin(), vec.end(), tid) == vec.end()) {
			vec.push_back(tid);
		}
	}

	void LockManager::deleteTid(size_t tid)
	{
		for (auto& iter : _pidToTids) {
			auto& vec = iter.second;
			auto tIter = find(vec.begin(), vec.end(), tid);
			if (tIter != vec.end()) {
				vec.erase(tIter);
			}
		}
	}
	
	void TransactionLockInfo::addLock(size_t pid, shared_ptr<shared_mutex> pageMutex, Permissions perm)
	{
		auto iter = findLock(pid);
		if (iter == _locks.end()) {
			shared_ptr<PageLock> plock = make_shared<PageLock>(pid, pageMutex);
			//plock->lock(perm);
			_locks.push_back(plock);
		}
		else {
			(*iter)->lock(perm);
		}
	}

	void TransactionLockInfo::lock(size_t pid, Permissions perm)
	{
		auto iter = findLock(pid);
		if (iter != _locks.end()) {
			(*iter)->lock(perm);
		}
		else {
			throw runtime_error("transaction does not hold the lock of page " + to_string(pid));
		}
	}

	void TransactionLockInfo::unlock(size_t pid, Permissions perm)
	{
		auto iter = findLock(pid);
		if (iter != _locks.end()) {
			(*iter)->unlock(perm);
		}
		else {
			throw runtime_error("transaction does not hold the lock of page " + to_string(pid));
		}
	}

	bool TransactionLockInfo::isLocked(size_t pid, Permissions perm)
	{
		auto iter = findLock(pid);
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
		auto iter = findLock(pid);
		if (iter == _locks.end())
			return false;
		return true;
	}

	void TransactionLockInfo::deleteLock(size_t pid)
	{
		auto iter = findLock(pid);
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

	void TransactionLockInfo::setUpgradeLock(size_t pid, bool flag)
	{
		auto iter = findLock(pid);
		if (iter == _locks.end()) {
			return;
		}

		(*iter)->setTryUpgradeLock(flag);
	}

	void TransactionLockInfo::upgradeLock(size_t pid)
	{
		auto iter = findLock(pid);
		if (iter == _locks.end()) {
			return;
		}
		if ((*iter)->getTryUpgradeLock()
			&& (*iter)->isLocked(Permissions::READ_ONLY)) {
			(*iter)->unlock(Permissions::READ_ONLY);
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

	vector<shared_ptr<PageLock>>::iterator TransactionLockInfo::findLock(size_t pid)
	{
		return find_if(_locks.begin(), _locks.end(),
			[pid](shared_ptr<PageLock> plock) {
				return plock->getPageId() == pid;
			});
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
