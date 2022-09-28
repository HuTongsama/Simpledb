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
	}
	
	void LockManager::accessPermission(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
	{
		unique_lock<mutex> guard(_managerLock);
			
		auto tCode = tid->getId();
		auto pCode = pid->hashCode();
		
		string s = to_string(tCode) + " " + (p == Permissions::READ_ONLY ? "read" : "write") + "\n";
		//printf("%s", s.c_str());
		auto pInfo = getInfo(tid->getId());
		if (pInfo == nullptr) {
			pInfo = make_shared<TransactionLockInfo>();
			_tidToInfo[tCode] = pInfo;
		}
		bool needWait = false;
		bool upgradeFlag = false;
		if (isLocked(pCode, Permissions::READ_WRITE)) {
			needWait = true;
		}
		else {
			if (p == Permissions::READ_ONLY) {
				if (pInfo->isLocked(pCode, Permissions::READ_WRITE))
					return;
			}
			else if (p == Permissions::READ_WRITE) {
				int readlockCount = countLock(pCode, Permissions::READ_ONLY);
				if (pInfo->isLocked(pCode, Permissions::READ_ONLY)
					&& readlockCount == 1) {
					upgradeFlag = true;
				}
				else if (readlockCount > 0) {
					needWait = true;
				}
			}
		}
		
		if (upgradeFlag) {
			pInfo->unlock(pCode, Permissions::READ_ONLY);
			pInfo->lock(pCode, Permissions::READ_WRITE);
		}
		if (needWait) {
			updateWaitforGraph(p, tid, pid);
			guard.unlock();
			unique_lock<mutex> lock(_waitLock);
			_waitCond.wait(lock, [this, pCode]() {
				//this_thread::sleep_for(chrono::milliseconds(100));
				lock_guard<mutex> lock(_managerLock);
				return (countLock(pCode, Permissions::READ_WRITE) <= 1);
				});
			guard.lock();
			int readlockCount = countLock(pCode, Permissions::READ_ONLY);
			if (readlockCount == 1 &&
				pInfo->isLocked(pCode, Permissions::READ_ONLY)
				&& p == Permissions::READ_WRITE) {
				pInfo->unlock(pCode, Permissions::READ_ONLY);
			}
		}
		pInfo->lock(pCode, p);
		updateWaitforGraph(p, tid, pid);
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
		string s = "graph: \n";
		for (auto p : _waitforGraph._indegreeMap) {
			s += "t :" + to_string(p.first) + " ,indegree: " + to_string(p.second) + "\n";
		}
		//printf("%s", s.c_str());
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

	bool LockManager::isLocked(size_t pid, Permissions perm)
	{
		for (auto& idToInfo : _tidToInfo) {
			auto& info = idToInfo.second;
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
			if (info->isLocked(pid, Permissions::READ_ONLY))
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
