#pragma once
#include"FilterBase.h"
#include"Filter.h"
class SysFilterTest :public FilterBase {
protected:
	int applyPredicate(shared_ptr<HeapFile> table,
		shared_ptr<TransactionId> tid,
		shared_ptr<Predicate> predicate)override {
        shared_ptr<SeqScan> ss = make_shared<SeqScan>(tid, table->getId(), "");
        shared_ptr<Filter> filter = make_shared<Filter>(predicate, ss);
        filter->open();

        int resultCount = 0;
        while (filter->hasNext()) {
            EXPECT_NO_THROW(filter->next());
            resultCount += 1;
        }

        filter->close();
        return resultCount;
	}
};