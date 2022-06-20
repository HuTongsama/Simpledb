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
		mutex _r;
		mutex _w;
	};
	struct PageLock
	{
		PageLock(mutex& r, mutex& w)
			:_rlock(r, std::defer_lock),
			_wlock(w, std::defer_lock) {}
		shared_lock<mutex> _rlock;  
		unique_lock<mutex> _wlock;
	};
	struct TransactionLockInfo {
		void addLock(size_t pid, shared_ptr<PageMutex> pageMutex);
		void deleteLock(size_t pid);
	private:
		vector<size_t> _pids;
		vector<shared_ptr<PageLock>> _locks;
	};
	class LockManager :public Noncopyable {
	public:
		void lock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void unlock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		bool isLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void deletePageLock(shared_ptr<PageId> pid);
	private:
		shared_ptr<PageLock> getLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);


		map<size_t, vector<size_t>> _tidToPids;
		map<size_t, vector<shared_ptr<PageLock>>> _tidToLocks;
		map<size_t, shared_ptr<PageMutex>> _pidToMutex;
		mutex _managerLock;
	};
}