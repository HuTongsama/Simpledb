#pragma once
#include"SimpleDbTestBase.h"
#include"BTreeFile.h"
#include"BTreeUtility.h"
#include<random>
using namespace Simpledb;
class BTreeDeadlockTest : public SimpleDbTestBase {
protected:
	static const int POLL_INTERVAL = 100;
	static const int WAIT_INTERVAL = 200;

	// just so we have a pointer shorter than Database.getBufferPool
	shared_ptr<BufferPool> _bp;
	shared_ptr<BTreeFile> _bf;
	int _item1;
	int _item2;
	int _count1;
	int _count2;
	std::random_device _randomDevice;
	std::default_random_engine _randomEngine;

	void SetUp()override {
		SimpleDbTestBase::SetUp();

		// create a packed B+ tree with no empty slots
		vector<vector<int>> tmp;
		_bf = BTreeUtility::createRandomBTreeFile(2, 253008, map<int, int>(), tmp, 0);
		_randomEngine.seed(_randomDevice());
		std::uniform_int_distribution<int> uniform_dist(0, BTreeUtility::MAX_RAND_VALUE);
		_item1 = uniform_dist(_randomEngine);
		_item2 = uniform_dist(_randomEngine);
		_bp = Database::resetBufferPool(BufferPool::DEFAULT_PAGES);

		// first make sure that item1 is not contained in our B+ tree
		shared_ptr<TransactionId> tid = make_shared<TransactionId>();
		auto it = _bf->indexIterator(tid, IndexPredicate(Predicate::Op::EQUALS, make_shared<IntField>(_item1)));
		it->open();
		vector<Tuple*> tuples;
		while (it->hasNext()) {
			tuples.push_back(it->next());
		}
		for (auto t : tuples) {
			_bp->deleteTuple(tid, *t);
		}

		// this is the number of tuples we must insert to replace the deleted tuples 
		// and cause the root node to split
		_count1 = tuples.size() + 1;

		// do the same thing for item 2
		it = _bf->indexIterator(tid, IndexPredicate(Predicate::Op::EQUALS, make_shared<IntField>(_item2)));
		it->open();
		tuples.clear();
		while (it->hasNext()) {
			tuples.push_back(it->next());
		}
		for (auto t : tuples) {
			_bp->deleteTuple(tid, *t);
		}

		// this is the number of tuples we must insert to replace the deleted tuples 
		// and cause the root node to split
		_count2 = tuples.size() + 1;

		// clear all state from the buffer pool, increase the number of pages
		_bp->flushAllPages();
	}

	shared_ptr<BTreeUtility::BTreeWriter> startWriter(
		shared_ptr<TransactionId> tid, int item, int count) {

		shared_ptr<BTreeUtility::BTreeWriter> bw = 
			make_shared<BTreeUtility::BTreeWriter>(tid, _bf, item, count);
		bw->start();
		return bw;
	}
};

/**
 * Not-so-unit test to construct a deadlock situation.
 *
 * This test causes two different transactions to update two (probably) different leaf nodes
 * Each transaction can happily insert tuples until the page fills up, but then
 * it needs to obtain a write lock on the root node in order to split the page. This will cause
 * a deadlock situation.
 */
TEST_F(BTreeDeadlockTest, TestReadWriteDeadlock) {
	cout << "testReadWriteDeadlock constructing deadlock:" << endl;

	shared_ptr<TransactionId> tid1 = make_shared<TransactionId>();
	shared_ptr<TransactionId> tid2 = make_shared<TransactionId>();

	Database::getBufferPool()->getPage(tid1, BTreeRootPtrPage::getId(_bf->getId()), Permissions::READ_ONLY);
	Database::getBufferPool()->getPage(tid2, BTreeRootPtrPage::getId(_bf->getId()), Permissions::READ_ONLY);
	
	// allow read locks to acquire
	this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));

	auto writer1 = startWriter(tid1, _item1, _count1);
	auto writer2 = startWriter(tid2, _item2, _count2);

	while (true) {
		this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));

		if (writer1->succeeded() || writer2->succeeded()) break;
		std::uniform_int_distribution<int> uniform_dist(0, WAIT_INTERVAL);
		if (writer1->getError() != "") {
			writer1 = nullptr;
			_bp->transactionComplete(tid1);
			
			this_thread::sleep_for(chrono::milliseconds(uniform_dist(_randomEngine)));

			tid1 = make_shared<TransactionId>();
			writer1 = startWriter(tid1, _item1, _count1);
		}

		if (writer2->getError() != "") {
			writer2 = nullptr;
			_bp->transactionComplete(tid2);
			this_thread::sleep_for(chrono::milliseconds(uniform_dist(_randomEngine)));

			tid2 = make_shared<TransactionId>();
			writer2 = startWriter(tid2, _item2, _count2);
		}

	}

	cout << "testReadWriteDeadlock resolved deadlock" << endl;
}