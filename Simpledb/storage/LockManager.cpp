#include "LockManager.h"
#include<iostream>
static mutex printMutex;
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
	}
	
	void LockManager::accessPermission(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		unique_lock<mutex> guard(_managerLock);
			
		auto tCode = tid->getId();
		auto pCode = pid->hashCode();
		
		auto pInfo = getInfo(tid->getId());
		if (pInfo == nullptr) {
			pInfo = make_shared<TransactionLockInfo>();
			_tidToInfo[tCode] = pInfo;
		}
		if (isLocked(pCode, tCode, p)
			|| (p == Permissions::READ_ONLY && isLocked(pCode, tCode, Permissions::READ_WRITE)))
			return;
		
		updateWaitforGraph(p, tid, pid);
		_waitCond.wait(guard, [this, tCode, pCode, p]() {
			int writeLockCount = countLock(pCode, Permissions::READ_WRITE);
			int readLockCount = countLock(pCode, Permissions::READ_ONLY);
			if (p == Permissions::READ_ONLY) {
				if (writeLockCount > 0) {
					return false;
				}
			}
			else {//Permissions::READ_WRITE
				if (isLocked(pCode, tCode, Permissions::READ_ONLY) &&
					readLockCount == 1) {
					return true;
				}
				else {
					if (readLockCount > 0 || writeLockCount > 0) {
						return false;
					}
				}
			}

			return true;
			});
		if (p == Permissions::READ_WRITE && isLocked(pCode, tCode, Permissions::READ_ONLY)) {
			int readLockCount = countLock(pCode, Permissions::READ_ONLY);
			if (readLockCount == 1)pInfo->unlock(pCode, Permissions::READ_ONLY);
		}
		pInfo->lock(pCode, p);
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
		_waitCond.notify_all();
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
	}

	void LockManager::updateWaitforGraph(Permissions p, shared_ptr<TransactionId>& tid, shared_ptr<PageId> pid)
	{
		//lock_guard<mutex> guard(_managerLock);
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
			bool t2ReadLock = info->isLocked(p1, Permissions::READ_ONLY);
			bool t2WriteLock = info->isLocked(p1, Permissions::READ_WRITE);
			if (findIter == pids.end()
				|| (!t2ReadLock && !t2WriteLock)) {
				continue;
			}
			if (t2WriteLock) {
				_waitforGraph.addEdge(t2, t1);
			}
			else if (t2ReadLock) {
				if (p == Permissions::READ_WRITE) {
					_waitforGraph.addEdge(t2, t1);
				}
			}
		}
		if (!_waitforGraph.isAcyclic()) {
			throw runtime_error("waitforGraph exist cycle");
		}
		else
		{
			//string s("graph: \n");
			//for (auto p : _waitforGraph._indegreeMap) {
			//	s += to_string(p.first) + " : " + to_string(p.second) + "\n";
			//}
			//printf("%s", s.c_str());
		}
	}

	shared_ptr<TransactionLockInfo> LockManager::getInfo(int64_t tid)
	{
		if (_tidToInfo.find(tid) != _tidToInfo.end()) {
			return _tidToInfo[tid];
		}
		return nullptr;
	}

	bool LockManager::isLocked(size_t pid, size_t tid, Permissions perm)
	{
		if (_tidToInfo.find(tid) != _tidToInfo.end()) {
			auto& info = _tidToInfo[tid];
			if (info->isLocked(pid, perm))
				return true;
		}
		return false;
	}

	int LockManager::countLock(size_t pid, Permissions perm)
	{
		int count = 0;
		for (auto& idToInfo : _tidToInfo) {
			auto& info = idToInfo.second;
			if (info->isLocked(pid, perm))
				count++;
		}
		return count;
	}

	void TransactionLockInfo::lock(size_t pid, Permissions perm)
	{
		if (_lockMap.find(pid) == _lockMap.end()) {
			_lockMap[pid] = make_shared<PageLock>(pid);
		}
		_lockMap[pid]->lock(perm);
	}

	void TransactionLockInfo::unlock(size_t pid, Permissions perm)
	{
		if (_lockMap.find(pid) == _lockMap.end()) {
			throw runtime_error("transaction does not hold the lock of page " + to_string(pid));
		}
		_lockMap[pid]->unlock(perm);
	}

	bool TransactionLockInfo::isLocked(size_t pid, Permissions perm)
	{
		if (_lockMap.find(pid) == _lockMap.end()) {
			return false;
		}
		return _lockMap[pid]->isLocked(perm);
	}

	void TransactionLockInfo::deleteLock(size_t pid)
	{
		if (_lockMap.find(pid) != _lockMap.end()) {
			_lockMap.erase(pid);
		}
	}

	vector<size_t> TransactionLockInfo::getAllPageIds()
	{
		vector<size_t> result;
		for (auto& lock : _lockMap) {
			result.push_back(lock.first);
		}
		return result;
	}

	void PageLock::lock(Permissions perm)
	{
		_lockMap[perm] = true;
	}

	void PageLock::unlock(Permissions perm)
	{
		_lockMap[perm] = false;
	}

	bool PageLock::isLocked(Permissions perm)
	{
		if (_lockMap.find(perm) != _lockMap.end())
			return _lockMap[perm];
		return false;
	}

}
