#include "LockManager.h"

namespace Simpledb {
	void LockManager::lock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		shared_ptr<PageLock> plock = getLock(tid, pid);	
		lock_guard<mutex> lock(_managerLock);
		bool repeatLock = false;
		switch (p)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (!plock->_rlock.owns_lock()) {
				plock->_rlock.lock();
			}
			else {
				repeatLock = true;
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (!plock->_wlock.owns_lock()) {
				plock->_wlock.lock();
			}
			else {
				repeatLock = true;
			}
			break;
		default:
			throw runtime_error("error permission type");
		}
		if (repeatLock) {
			throw runtime_error("repeatLock");
		}
	}
	void LockManager::unlock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		shared_ptr<PageLock> plock = getLock(tid, pid);
		lock_guard<mutex> lock(_managerLock);
		switch (p)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (plock->_rlock.owns_lock()) {
				plock->_rlock.unlock();
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (plock->_wlock.owns_lock()) {
				plock->_wlock.unlock();
			}
			break;
		default:
			throw runtime_error("error permission type");
		}

	}
	
	bool LockManager::isLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		shared_ptr<PageLock> plock = getLock(tid, pid);
		lock_guard<mutex> lock(_managerLock);
		switch (p)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (plock->_rlock.owns_lock()) {
				return true;
			}
			else{
				return false;
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (plock->_wlock.owns_lock()) {
				return true;
			}
			else {
				return false;
			}
			break;
		default:
			throw runtime_error("error permission type");
		}
	}

	void LockManager::accessPage(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		auto pageId = pid->hashCode();
		auto t = tid->getId();
		lock_guard<mutex> lock(_managerLock);
		shared_ptr<TransactionLockInfo> pInfo = _tidToInfo[t];
		if (pInfo == nullptr || pInfo->getLock(pageId) != nullptr)
			return;
		shared_ptr<PageMutex> pMutex = _pidToMutex[pageId];
		if (pMutex == nullptr)
			return;
		if (p == Permissions::READ_ONLY) {
			shared_lock<shared_mutex> tmp(pMutex->_r);
			return;
		}
		else if (p == Permissions::READ_WRITE) {
			unique_lock<shared_mutex> tmp(pMutex->_w);
			return;
		}
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

	shared_ptr<PageLock> LockManager::getLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto t = tid->getId();
		shared_ptr<TransactionLockInfo> pInfo = _tidToInfo[t];
		if (pInfo == nullptr) {
			pInfo = make_shared<TransactionLockInfo>();
			_tidToInfo[t] = pInfo;
		}
		auto p = pid->hashCode();
		auto plock = pInfo->getLock(p);
		if (plock == nullptr) {
			shared_ptr<PageMutex> pMutex = _pidToMutex[p];
			if (pMutex == nullptr) {
				pMutex = make_shared<PageMutex>();
				_pidToMutex[p] = pMutex;
			}
			pInfo->addLock(p, pMutex);
			plock = pInfo->getLock(p);		
		}
		if (plock == nullptr)
			throw runtime_error("getLock failed");
			
		return plock;
	}
	
	void TransactionLockInfo::addLock(size_t pid, shared_ptr<PageMutex> pageMutex)
	{
		if (find(_pids.begin(), _pids.end(), pid) != _pids.end()) {
			throw runtime_error("lock " + to_string(pid) + " exist");
		}
		else {
			_pids.push_back(pid);
			_locks.push_back(make_shared<PageLock>(pageMutex->_r, pageMutex->_w));
		}
	}

	void TransactionLockInfo::deleteLock(size_t pid)
	{
		auto sz = _pids.size();
		bool flag = false;
		for (int i = 0; i < sz; i++) {
			if (_pids[i] == pid) {
				shared_ptr<PageLock> lock = _locks[i];
				if (lock->_rlock.owns_lock()
					|| lock->_wlock.owns_lock()) {
					throw runtime_error("delete failed transaction still lock page " + to_string(pid));
				}
			}
			else {
				_pids.erase(_pids.begin() + i);
				_locks.erase(_locks.begin() + i);
				flag = true;
				break;
			}
		}
		if (!flag) {
			throw runtime_error("delte failed");
		}
	}

	shared_ptr<PageLock> TransactionLockInfo::getLock(size_t pid)
	{
		auto sz = _pids.size();
		for (int i = 0; i < sz; i++) {
			if (_pids[i] == pid) {
				return _locks[i];
			}
		}
		return nullptr;
	}

}
