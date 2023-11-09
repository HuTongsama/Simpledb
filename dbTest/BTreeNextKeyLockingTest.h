#pragma once
#include"SimpleDbTestBase.h"
#include"BTreeUtility.h"


using namespace Simpledb;
class BTreeNextKeyLockingTest : public SimpleDbTestBase {
protected:
	shared_ptr<TransactionId> _tid;
	static const int POLL_INTERVAL = 100;

	void SetUp()override {
		SimpleDbTestBase::SetUp();
		_tid = make_shared<TransactionId>();
	}

	void TearDown()override {
		Database::getBufferPool()->transactionComplete(_tid);
	}
};

TEST_F(BTreeNextKeyLockingTest, NextKeyLockingTestLessThan) {
	// This should create a B+ tree with 100 leaf pages
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> bigFile = BTreeUtility::createRandomBTreeFile(2, 50200,
		map<int,int>(), tuples, 0);
	Database::resetBufferPool(100);
	// get a key from the middle of the root page
	shared_ptr<BTreePageId> rootPtrPid = make_shared<BTreePageId>(bigFile->getId(), 0, BTreePageId::ROOT_PTR);
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
		(Database::getBufferPool()->getPage(_tid, rootPtrPid, Permissions::READ_ONLY));
	shared_ptr<BTreePageId> rootId = rootPtr->getRootId();
	EXPECT_EQ(rootId->pgcateg(), BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>
		(Database::getBufferPool()->getPage(_tid, rootId, Permissions::READ_ONLY));
	int keyIndex = 50; // this should be right in the middle since there are 100 leaf pages
	auto it = root->iterator();
	shared_ptr<Field> key;
	int count = 0;
	while (it->hasNext()) {
		BTreeEntry* e = it->next();
		if (count == keyIndex) {
			key = e->getKey();
			break;
		}
		count++;
	}
	EXPECT_TRUE(key != nullptr);


	// now find all tuples containing that key and delete them, as well as the next key
	IndexPredicate ipred(Predicate::Op::EQUALS, key);
	auto fit = bigFile->indexIterator(_tid, ipred);
	fit->open();
	while (fit->hasNext()) {
		Database::getBufferPool()->deleteTuple(_tid, *fit->next());
	}
	fit->close();

	count = 0;
	while (count == 0) {
		key = make_shared<IntField>((dynamic_pointer_cast<IntField>(key))->getValue() + 1);
		ipred = IndexPredicate(Predicate::Op::EQUALS, key);
		fit = bigFile->indexIterator(_tid, ipred);
		fit->open();
		while (fit->hasNext()) {
			Database::getBufferPool()->deleteTuple(_tid, *fit->next());
			count++;
		}
		fit->close();
	}

	Database::getBufferPool()->transactionComplete(_tid);
	_tid = make_shared<TransactionId>();

	// search for tuples less than or equal to the key
	ipred = IndexPredicate(Predicate::Op::LESS_THAN_OR_EQ, key);	
	fit = bigFile->indexIterator(_tid, ipred);;
	fit->open();
	int keyCountBefore = 0;
	while (fit->hasNext()) {
		fit->next();
		keyCountBefore++;
	}
	fit->close();

	// In a different thread, try to insert tuples containing the key
	shared_ptr<TransactionId> tid1 = make_shared<TransactionId>();
	BTreeUtility::BTreeWriter bw1(tid1, bigFile, dynamic_pointer_cast<IntField>(key)->getValue(), 1);
	bw1.start();

	// allow thread to start
	this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));

	// search for tuples less than or equal to the key
	ipred = IndexPredicate(Predicate::Op::LESS_THAN_OR_EQ, key);
	fit = bigFile->indexIterator(_tid, ipred);
	fit->open();
	int keyCountAfter = 0;
	while (fit->hasNext()) {
		fit->next();
		keyCountAfter++;
	}
	fit->close();

	// make sure our indexIterator() is working
	EXPECT_TRUE(keyCountBefore > 0);

	// check that we don't have any phantoms
	EXPECT_EQ(keyCountBefore, keyCountAfter);
	EXPECT_FALSE(bw1.succeeded());

	// now let the inserts happen
	Database::getBufferPool()->transactionComplete(_tid);

	while (!bw1.succeeded() && bw1.getError() == "") {
		this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));
		if (bw1.succeeded()) {
			Database::getBufferPool()->transactionComplete(tid1);
		}
	}
}

TEST_F(BTreeNextKeyLockingTest, NextKeyLockingTestGreaterThan) {
	// This should create a B+ tree with 100 leaf pages	
	vector<vector<int>> tuples;
	shared_ptr<BTreeFile> bigFile = BTreeUtility::createRandomBTreeFile(2, 50200,
		map<int, int>(), tuples, 0);
	Database::resetBufferPool(100);
	// get a key from the middle of the root page
	shared_ptr<BTreePageId> rootPtrPid = make_shared<BTreePageId>(bigFile->getId(), 0, BTreePageId::ROOT_PTR);
	shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
		(Database::getBufferPool()->getPage(_tid, rootPtrPid, Permissions::READ_ONLY));
	shared_ptr<BTreePageId> rootId = rootPtr->getRootId();
	EXPECT_EQ(rootId->pgcateg(), BTreePageId::INTERNAL);
	shared_ptr<BTreeInternalPage> root = dynamic_pointer_cast<BTreeInternalPage>
		(Database::getBufferPool()->getPage(_tid, rootId, Permissions::READ_ONLY));
	int keyIndex = 50; // this should be right in the middle since there are 100 leaf pages
	auto it = root->iterator();
	shared_ptr<Field> key;
	int count = 0;
	while (it->hasNext()) {
		BTreeEntry* e = it->next();
		if (count == keyIndex) {
			key = e->getKey();
			break;
		}
		count++;
	}
	EXPECT_TRUE(key != nullptr);

	// now find all tuples containing that key and delete them, as well as the next key
	IndexPredicate ipred(Predicate::Op::EQUALS, key);
	auto fit = bigFile->indexIterator(_tid, ipred);
	fit->open();
	while (fit->hasNext()) {
		Database::getBufferPool()->deleteTuple(_tid, *fit->next());
	}
	fit->close();

	count = 0;
	while (count == 0) {
		key = make_shared<IntField>((dynamic_pointer_cast<IntField>(key))->getValue() - 1);
		ipred = IndexPredicate(Predicate::Op::EQUALS, key);
		fit = bigFile->indexIterator(_tid, ipred);
		fit->open();
		while (fit->hasNext()) {
			Database::getBufferPool()->deleteTuple(_tid, *fit->next());
			count++;
		}
		fit->close();
	}

	Database::getBufferPool()->transactionComplete(_tid);
	_tid = make_shared<TransactionId>();

	// search for tuples greater than or equal to the key
	ipred = IndexPredicate(Predicate::Op::GREATER_THAN_OR_EQ, key);
	fit = bigFile->indexIterator(_tid, ipred);
	fit->open();
	int keyCountBefore = 0;
	while (fit->hasNext()) {
		fit->next();
		keyCountBefore++;
	}
	fit->close();

	// In a different thread, try to insert tuples containing the key
	shared_ptr<TransactionId> tid1 = make_shared<TransactionId>();
	BTreeUtility::BTreeWriter bw1(tid1, bigFile, dynamic_pointer_cast<IntField>(key)->getValue(), 1);
	bw1.start();

	// allow thread to start
	this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));

	// search for tuples greater than or equal to the key
	ipred = IndexPredicate(Predicate::Op::GREATER_THAN_OR_EQ, key);
	fit = bigFile->indexIterator(_tid, ipred);
	fit->open();
	int keyCountAfter = 0;
	while (fit->hasNext()) {
		fit->next();
		keyCountAfter++;
	}
	fit->close();

	// make sure our indexIterator() is working
	EXPECT_TRUE(keyCountBefore > 0);

	// check that we don't have any phantoms
	EXPECT_EQ(keyCountBefore, keyCountAfter);
	EXPECT_FALSE(bw1.succeeded());

	// now let the inserts happen
	Database::getBufferPool()->transactionComplete(_tid);

	while (!bw1.succeeded() && bw1.getError() == "") {
		this_thread::sleep_for(chrono::milliseconds(POLL_INTERVAL));
		if (bw1.succeeded()) {
			Database::getBufferPool()->transactionComplete(tid1);
		}
	}
}