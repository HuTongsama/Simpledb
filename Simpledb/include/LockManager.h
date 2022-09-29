#pragma once
#include"Noncopyable.h"
#include"TransactionId.h"
#include"PageId.h"
#include"Permissions.h"
#include"DirectedGraph.h"
#include<mutex>
#include<shared_mutex>
#include<map>
#include<vector>
using namespace std;
namespace Simpledb {
	struct PageLock
	{
		PageLock(size_t pid) :_pid(pid) , _isWaitWriting(false) {}
		void lock(Permissions perm);
		void unlock(Permissions perm);
		bool isLocked(Permissions perm);
		size_t getPageId() { return _pid; }
		void setWaitWriting(bool flag) {
			_isWaitWriting = flag;
		}
		bool getWaitWriting() { return _isWaitWriting; }
	private:
		size_t _pid;
		map<Permissions, bool> _lockMap;
		bool _isWaitWriting;
	};
	struct TransactionLockInfo {
		void lock(size_t pid, Permissions perm);
		void unlock(size_t pid, Permissions perm);
		bool isLocked(size_t pid, Permissions perm);
		void deleteLock(size_t pid);
		void setWaitWriting(size_t pid, bool flag);
		bool getWaitWriting(size_t pid);
		vector<size_t> getAllPageIds();
	private:

		map<size_t, shared_ptr<PageLock>> _lockMap;
	};
	class LockManager :public Noncopyable {
	public:
		bool holdsLock(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void unlockPage(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void accessPermission(Permissions p, shared_ptr<TransactionId> tid, shared_ptr<PageId> pid);
		void transactionComplete(shared_ptr<TransactionId> tid);
		vector<size_t> getRelatedPageIds(shared_ptr<TransactionId> tid);
		void deletePageLock(shared_ptr<PageId> pid);

	private:
		void updateWaitforGraph(Permissions p, shared_ptr<TransactionId>& tid, shared_ptr<PageId> pid);
		shared_ptr<TransactionLockInfo> getInfo(int64_t tid);
		bool isLocked(size_t pid, Permissions perm);
		int countLock(size_t pid, Permissions perm);

		map<size_t, shared_ptr<TransactionLockInfo>> _tidToInfo;
		mutex _managerLock;
		mutex _waitLock;
		condition_variable _waitCond;
		DirectedGraph _waitforGraph;
	};
}