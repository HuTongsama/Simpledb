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
	struct PageMutex
	{
		shared_mutex _r;
		shared_mutex _w;
	};
	struct PageLock
	{
		PageLock(shared_mutex& r, shared_mutex& w)
			:_rlock(r, std::defer_lock),
			_wlock(w, std::defer_lock) {}
		shared_lock<shared_mutex> _rlock;
		unique_lock<shared_mutex> _wlock;
	};
	struct TransactionLockInfo {
		void addLock(size_t pid, shared_ptr<PageMutex> pageMutex);
		void deleteLock(size_t pid);
		shared_ptr<PageLock> getLock(size_t pid);
	private:
		vector<size_t> _pids;
		vector<shared_ptr<PageLock>> _locks;
	};
	class LockManager :public Noncopyable {
	public:
		void lock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void unlock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		bool isLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void accessPage(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void deletePageLock(shared_ptr<PageId> pid);
	private:
		shared_ptr<PageLock> getLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);

		map<size_t, shared_ptr<TransactionLockInfo>> _tidToInfo;
		map<size_t, shared_ptr<PageMutex>> _pidToMutex;
		mutex _managerLock;
	};
}