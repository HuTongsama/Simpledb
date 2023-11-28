#pragma once
#include"SimpleDbTestBase.h"
#include"SystemTestUtil.h"
#include"BTreeUtility.h"
#include<random>
using namespace Simpledb;
using BTreeInserter = BTreeUtility::BTreeInserter;
using BTreeDeleter = BTreeUtility::BTreeDeleter;
class SysBTreeTest :public SimpleDbTestBase {
protected:
	std::random_device _randomDevice;
	std::default_random_engine _randomEngine;

	static const int POLL_INTERVAL = 100;
	/**
	 * Helper method to clean up the syntax of starting a BTreeInserter thread.
	 * The parameters pass through to the BTreeInserter constructor.
	 */
	shared_ptr<BTreeInserter> startInserter(shared_ptr<BTreeFile> bf, 
		const vector<int>& tupdata, shared_ptr<BlockingQueue<vector<int>>> insertedTuples) {

		shared_ptr<BTreeInserter> bi = make_shared<BTreeInserter>(bf, tupdata, insertedTuples);
		bi->start();
		return bi;
	}

	/**
	 * Helper method to clean up the syntax of starting a BTreeDeleter thread.
	 * The parameters pass through to the BTreeDeleter constructor.
	 */
	shared_ptr<BTreeDeleter> startDeleter(shared_ptr<BTreeFile> bf,
		shared_ptr<BlockingQueue<vector<int>>> insertedTuples) {
		shared_ptr<BTreeDeleter> bd = make_shared<BTreeDeleter>(bf, insertedTuples);
		bd->start();
		return bd;
	}
	
	template<typename T>
	void waitForThreads(vector<shared_ptr<T>>& insertThreads) {
		this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));
		for (auto& thread : insertThreads) {
			while (!thread->succeeded() && thread->getError() == "") {
				this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));
			}
		}
	}

	vector<int> getRandomTupleData() {
		_randomEngine.seed(_randomDevice());
		std::uniform_int_distribution<int> uniform_dist(0, BTreeUtility::MAX_RAND_VALUE);
		int item1 = uniform_dist(_randomEngine);
		int item2 = uniform_dist(_randomEngine);
		return { item1,item2 };
	}

	void TearDown()override {
		// set the page size back to the default
		BufferPool::resetPageSize();
		Database::reset();
	}
};

/** Test that doing lots of inserts and deletes in multiple threads works */
TEST_F(SysBTreeTest, TestBigFile) {
	// For this test we will decrease the size of the Buffer Pool pages
	BufferPool::setPageSize(1024);

	// This should create a B+ tree with a packed second tier of internal pages
	// and packed third tier of leaf pages
	printf("Creating large random B+ tree...\n");
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> bf = BTreeUtility::createRandomBTreeFile(2, 31000,
		map<int, int>(), tuples, 0);

	// we will need more room in the buffer pool for this test
	Database::resetBufferPool(500);

	shared_ptr<BlockingQueue<vector<int>>> insertedTuples = make_shared<BlockingQueue<vector<int>>>(100000);
	for (auto& tuple : tuples) {
		insertedTuples->push(tuple);
	}
	size_t size = insertedTuples->size();
	EXPECT_EQ(31000, size);
	
	// now insert some random tuples
	printf("Inserting tuples...\n");
	vector<shared_ptr<BTreeInserter>> insertThreads;
	_randomEngine.seed(_randomDevice());
	std::uniform_int_distribution<int> uniform_dist(0, POLL_INTERVAL);
	for (int i = 0; i < 200; i++) {
		shared_ptr<BTreeInserter> bi = startInserter(bf, getRandomTupleData(), insertedTuples);
		insertThreads.push_back(bi);
		// The first few inserts will cause pages to split so give them a little
		// more time to avoid too many deadlock situations
		this_thread::sleep_for(chrono::milliseconds(uniform_dist(_randomEngine)));
	}

	for (int i = 0; i < 800; i++) {
		shared_ptr<BTreeInserter> bi = startInserter(bf, getRandomTupleData(), insertedTuples);
		insertThreads.push_back(bi);
	}

	// wait for all threads to finish
	waitForThreads(insertThreads);
	EXPECT_TRUE(insertedTuples->size() > size);

	// now insert and delete tuples at the same time
	printf("Inserting and deleting tuples...\n");
	vector<shared_ptr<BTreeDeleter>> deleteThreads;
	for (auto& thread : insertThreads) {
		thread->rerun(bf, getRandomTupleData(), insertedTuples);
		shared_ptr<BTreeDeleter> bd = startDeleter(bf, insertedTuples);
		deleteThreads.push_back(bd);
	}

	// wait for all threads to finish
	waitForThreads(insertThreads);
	waitForThreads(deleteThreads);
	size_t numPages = bf->numPages();
	size = insertedTuples->size();

	// now delete a bunch of tuples
	printf("Deleting tuples...\n");
	for (int i = 0; i < 10; i++) {
		for (auto& thread : deleteThreads) {
			thread->rerun(bf, insertedTuples);
		}

		// wait for all threads to finish
		waitForThreads(deleteThreads);
	}
	EXPECT_TRUE(insertedTuples->size() < size);
	size = insertedTuples->size();

	// now insert a bunch of random tuples again
	printf("Inserting tuples...\n");
	for (int i = 0; i < 10; i++) {
		for (auto& thread : insertThreads) {
			thread->rerun(bf, getRandomTupleData(), insertedTuples);
		}

		// wait for all threads to finish
		waitForThreads(insertThreads);
	}
	EXPECT_TRUE(insertedTuples->size() > size);
	size = insertedTuples->size();
	// we should be reusing the deleted pages
	EXPECT_TRUE(bf->numPages() < numPages + 20);

	// kill all the threads
	insertThreads.clear();
	deleteThreads.clear();

	vector<vector<int>> tuplesList;
	size_t sz = insertedTuples->size();
	while (!insertedTuples->isEmpty()) {
		tuplesList.push_back(insertedTuples->pop());
	}
	shared_ptr<TransactionId> tid = make_shared<TransactionId>();

	// First look for random tuples and make sure we can find them
	std::uniform_int_distribution<int> uniform_dist1(0, sz);
	printf("Searching for tuples...\n");
	for (int i = 0; i < 10000; i++) {
		int rand = uniform_dist1(_randomEngine);
		vector<int>& tuple = tuplesList[rand];
		shared_ptr<IntField> randKey = make_shared<IntField>(tuple[bf->keyField()]);
		IndexPredicate ipred(Predicate::Op::EQUALS, randKey);
		shared_ptr<DbFileIterator> it = bf->indexIterator(tid, ipred);
		it->open();
		bool found = false;
		size_t tSz = tuple.size();
		while (it->hasNext()) {
			Tuple* t = it->next();
			auto tmp = SystemTestUtil::tupleToVector(*t);
			size_t tmpSz = tmp.size();
			if (tSz != tmpSz)continue;
			bool allSame = true;
			for (size_t n = 0; n < tSz; ++n) {
				if (tuple[n] == tmp[n])continue;
				allSame = false;
				break;
			}
			if (allSame) {
				found = true;
				break;
			}
		}
		EXPECT_TRUE(found);
		it->close();
	}

	// now make sure all the tuples are in order and we have the right number
	printf("Performing sanity checks...\n");
	shared_ptr<DbFileIterator> it = bf->iterator(tid);
	shared_ptr<Field> prev;
	it->open();
	int count = 0;
	while (it->hasNext()) {
		Tuple* t = it->next();
		if (prev != nullptr) {
			EXPECT_TRUE(t->getField(bf->keyField())->compare(Predicate::Op::GREATER_THAN_OR_EQ, *prev));
		}
		prev = t->getField(bf->keyField());
		count++;
	}
	it->close();
	EXPECT_EQ(count, tuplesList.size());
	Database::getBufferPool()->transactionComplete(tid);

	// set the page size back
	BufferPool::resetPageSize();
}