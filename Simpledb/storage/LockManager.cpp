#include "LockManager.h"

namespace Simpledb {
	void LockManager::lock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		shared_ptr<PageLock> pLock = getLock(tid, pid);
		lock_guard<mutex> lock(_managerLock);
		auto pCode = pid->hashCode();
		auto tCode = tid->getId();
		shared_ptr<PageMutex> pageMutex = _pidToMutex[pCode];
		if (pLock == nullptr) {
			vector<size_t>& pids = _tidToPids[tCode];
			vector<shared_ptr<PageLock>>& pageLocks = _tidToLocks[tCode];
			shared_ptr<PageLock> plock = make_shared<PageLock>(pageMutex->_r, pageMutex->_w);
			pageLocks.push_back(plock);
			pids.push_back()

		}
		
		size_t t = tid->getId();
		size_t pCode = pid->hashCode();
		vector<size_t>& pids = _tidToPids[t];

		bool repeatLock = false;
		PageLock& plock = _pidToLock[pCode];
		switch (p)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (plock._rLock.owns_lock()) {
				repeatLock = true;
			}
			else {
				plock._rLock.lock();
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (plock._wLock.owns_lock()) {
				repeatLock = true;
			}
			else {
				plock._wLock.lock();
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
		lock_guard<mutex> lock(_managerLock);
		size_t t = tid->getId();
		size_t pCode = pid->hashCode();
		vector<size_t>& pids = _tidToPids[t];

		bool repeatLock = false;
		PageLock& plock = _pidToLock[pCode];
		switch (p)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (plock._rLock.owns_lock()) {
				plock._rLock.unlock();
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (plock._wLock.owns_lock()) {
				plock._wLock.unlock();
			}
			break;
		default:
			throw runtime_error("error permission type");
		}

	}
	bool LockManager::isLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		lock_guard<mutex> lock(_managerLock);
		size_t t = tid->getId();
		size_t pCode = pid->hashCode();
		vector<size_t>& pids = _tidToPids[t];

		bool repeatLock = false;
		PageLock& plock = _pidToLock[pCode];
		switch (p)
		{
		case Simpledb::Permissions::READ_ONLY:
			if (plock._rLock.owns_lock()) {
				return true;
			}
			else{
				return false;
			}
			break;
		case Simpledb::Permissions::READ_WRITE:
			if (plock._wLock.owns_lock()) {
				plock._wLock.unlock();
			}
			else {
				return false;
			}
			break;
		default:
			throw runtime_error("error permission type");
		}
	}

	void LockManager::deletePageLock(shared_ptr<PageId> pid)
	{
		lock_guard<mutex> lock(_managerLock);
		auto pCode = pid->hashCode();
		for (auto& p : _tidToPids) {
			vector<size_t>& pids = p.second;
			auto sz = pids.size();
			for (int i = 0; i < sz; ++i) {
				if (pCode == pids[i]) {
					vector<PageLock>& pageLocks = _tidToLocks[p.first];
					PageLock& l = pageLocks[i];
					if (l._rlock.owns_lock()
						|| l._wlock.owns_lock()) {
						throw runtime_error("delete failed transaction " +
							to_string(p.first) + " still lock page " + to_string(pCode));
					}
					else {
						pageLocks.erase(pageLocks.begin() + i);
						pids.erase(pids.begin() + i);
						break;
					}
				}
			}
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
		auto p = pid->hashCode();

		auto& pids = _tidToPids[t];
		auto sz = pids.size();
		for (int i = 0; i < sz; ++i) {
			if (pids[i] == p) {
				return _tidToLocks[t][i];
			}
		}
		return nullptr;
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
				
			}
		}
	}

}
