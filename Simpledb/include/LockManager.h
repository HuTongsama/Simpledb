#pragma once
#include"Noncopyable.h"
#include"TransactionId.h"
#include"PageId.h"
#include"Permissions.h"
#include<mutex>
#include<shared_mutex>
#include<map>
#include<vector>
using namespace std;
namespace Simpledb {
	struct PageLock
	{
		PageLock(size_t pid, shared_ptr<shared_mutex> pMutex)
			:_pid(pid), _pMutex(pMutex) {
			_rlock = shared_lock<shared_mutex>(*_pMutex, std::defer_lock);
			_wlock = unique_lock<shared_mutex>(*_pMutex, std::defer_lock);
		}
		void lock(Permissions perm);
		void unlock(Permissions perm);
		bool isLocked(Permissions perm);
		size_t getPageId() { return _pid; }
	private:
		size_t _pid;
		shared_ptr<shared_mutex> _pMutex;
		shared_lock<shared_mutex> _rlock;
		unique_lock<shared_mutex> _wlock;
	};
	struct TransactionLockInfo {
		void addLock(size_t pid, shared_ptr<shared_mutex> pageMutex, Permissions perm);
		void lock(size_t pid, Permissions perm);
		void unlock(size_t pid, Permissions perm);
		bool isLocked(size_t pid, Permissions perm);
		bool holdsLock(size_t pid);
		void deleteLock(size_t pid);
	private:
		vector<shared_ptr<PageLock>> _locks;
	};
	class LockManager :public Noncopyable {
	public:
		bool holdsLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void unlockPage(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void accessPermission(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void transactionComplete(shared_ptr<TransactionId> tid);
		void deletePageLock(shared_ptr<PageId> pid);

	private:
		map<size_t, shared_ptr<TransactionLockInfo>> _tidToInfo;
		map<size_t, shared_ptr<shared_mutex>> _pidToMutex;
		mutex _managerLock;
	};
}