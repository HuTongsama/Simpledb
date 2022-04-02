#pragma once
#include"HeapPageTestBase.h"

class HeapPageWriteTest : public HeapPageTestBase {};

/**
 * Unit test for HeapPage.isDirty()
 */
TEST_F(HeapPageWriteTest, TestDirty) {
    shared_ptr<TransactionId> tid = make_shared<TransactionId>();
    shared_ptr<HeapPage> page = make_shared<HeapPage>(_pid, EXAMPLE_DATA);
    page->markDirty(true, *tid);
    shared_ptr<TransactionId> dirtier = page->isDirty();
    EXPECT_TRUE(dirtier != nullptr);
    EXPECT_TRUE(dirtier->equals(*tid));

    page->markDirty(false, *tid);
    dirtier = page->isDirty();
    EXPECT_FALSE(dirtier != nullptr);
}


/**
 * Unit test for HeapPage.addTuple()
 */
TEST_F(HeapPageWriteTest, AddTuple) {
    shared_ptr<HeapPage> page = make_shared<HeapPage>(_pid, EXAMPLE_DATA);
    int free = page->getNumEmptySlots();

    for (int i = 0; i < free; ++i) {
        shared_ptr<Tuple> addition = Utility::getHeapTuple(i, 2);
        page->insertTuple(addition);
        EXPECT_EQ(free - i - 1, page->getNumEmptySlots());

        // loop through the iterator to ensure that the tuple actually exists
        // on the page
        shared_ptr<TupleIterator> it = page->iterator();
        bool found = false;
        while (it->hasNext()) {
            Tuple& tup = it->next();
            if (TestUtil::compareTuples(*addition, tup)) {
                found = true;
                // verify that the RecordId is sane
                EXPECT_TRUE(page->getId()->equals(*tup.getRecordId()->getPageId()));
                break;
            }
        }
        EXPECT_TRUE(found);
    }
    // now, the page should be full.
    EXPECT_THROW(page->insertTuple(Utility::getHeapTuple(0, 2)), runtime_error);   
}

/**
 * Unit test for HeapPage.deleteTuple() with false tuples
 */
TEST_F(HeapPageWriteTest, DeleteNonexistentTuple) {
    shared_ptr<HeapPage> page = make_shared<HeapPage>(_pid, EXAMPLE_DATA);
    EXPECT_THROW(page->deleteTuple(Utility::getHeapTuple(2, 2)),runtime_error);
}

/**
 * Unit test for HeapPage.deleteTuple()
 */
TEST_F(HeapPageWriteTest, DeleteTuple) {
    shared_ptr<HeapPage> page = make_shared<HeapPage>(_pid, EXAMPLE_DATA);
    int free = page->getNumEmptySlots();

    //// first, build a list of the tuples on the page.
    //shared_ptr<TupleIterator> it = page->iterator();
    //LinkedList<Tuple> tuples = new LinkedList<>();
    //while (it.hasNext())
    //    tuples.add(it.next());
    //Tuple first = tuples.getFirst();

    //// now, delete them one-by-one from both the front and the end.
    //int deleted = 0;
    //while (tuples.size() > 0) {
    //    page.deleteTuple(tuples.removeFirst());
    //    page.deleteTuple(tuples.removeLast());
    //    deleted += 2;
    //    assertEquals(free + deleted, page.getNumEmptySlots());
    //}

    //// now, the page should be empty.
    //try {
    //    page.deleteTuple(first);
    //    throw new Exception("page should be empty; expected DbException");
    //}
    //catch (DbException e) {
    //    // explicitly ignored
    //}
}