#include "LockManager.h"

namespace Simpledb {
	void LockManager::lock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
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
	
	unique_lock<mutex> LockManager::getLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid)
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
			return unique_lock<mutex>(*plock._rLock.mutex());
		case Simpledb::Permissions::READ_WRITE:
			return unique_lock<mutex>(*plock._wLock.mutex());
			break;
		default:
			throw runtime_error("error permission type");
		}
		return unique_lock<mutex>();
	}
}
