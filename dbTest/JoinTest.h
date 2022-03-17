#pragma once
#include"SimpleDbTestBase.h"
#include"Join.h"
using namespace Simpledb;
class JoinTest :public SimpleDbTestBase {
protected:
	void SetUp()override {
        this.scan1 = TestUtil.createTupleList(width1,
            new int[] { 1, 2,
            3, 4,
            5, 6,
            7, 8 });
        this.scan2 = TestUtil.createTupleList(width2,
            new int[] { 1, 2, 3,
            2, 3, 4,
            3, 4, 5,
            4, 5, 6,
            5, 6, 7 });
        this.eqJoin = TestUtil.createTupleList(width1 + width2,
            new int[] { 1, 2, 1, 2, 3,
            3, 4, 3, 4, 5,
            5, 6, 5, 6, 7 });
        this.gtJoin = TestUtil.createTupleList(width1 + width2,
            new int[] {
                3, 4, 1, 2, 3, // 1, 2 < 3
                    3, 4, 2, 3, 4,
                    5, 6, 1, 2, 3, // 1, 2, 3, 4 < 5
                    5, 6, 2, 3, 4,
                    5, 6, 3, 4, 5,
                    5, 6, 4, 5, 6,
                    7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
                    7, 8, 2, 3, 4,
                    7, 8, 3, 4, 5,
                    7, 8, 4, 5, 6,
                    7, 8, 5, 6, 7 });
    }
	}


	int _width1 = 2;
	int _width2 = 3;
	shared_ptr<OpIterator> _scan1;
	shared_ptr<OpIterator> _scan2;
	shared_ptr<OpIterator> _eqJoin;
	shared_ptr<OpIterator> _gtJoin;
};