#pragma once
#include"Noncopyable.h"
#include"TransactionId.h"
#include"PageId.h"
#include"Permissions.h"
#include<mutex>
#include<map>
#include<vector>
using namespace std;
namespace Simpledb {
	struct PageLock
	{
		PageLock() {
			_rLock = unique_lock<mutex>(_r, std::defer_lock);
			_wLock = unique_lock<mutex>(_w, std::defer_lock);
		}

		unique_lock<mutex> _rLock;
		unique_lock<mutex> _wLock;

	private:
		mutex _r;
		mutex _w;
	};
	class LockManager :public Noncopyable {
	public:
		void lock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void unlock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		bool isLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		unique_lock<mutex> getLock(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
	private:
		map<size_t, vector<size_t>> _tidToPids;
		map<size_t, PageLock> _pidToLock;
		mutex _managerLock;
	};
}