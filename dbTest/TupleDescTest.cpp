#include "pch.h"
#include "TupleDescTest.h"

bool TupleDescTest::combinedStringArrays(shared_ptr<TupleDesc> td1, shared_ptr<TupleDesc> td2, shared_ptr<TupleDesc> combined)
{
    for (int i = 0; i < td1->numFields(); i++) {
        if (!(td1->getFieldName(i) == combined->getFieldName(i))) {
            return false;
        }
    }

    for (int i = td1->numFields(); i < td1->numFields() + td2->numFields(); i++) {
        if (!(td2->getFieldName(i - td1->numFields()) == combined->getFieldName(i))) {
            return false;
        }
    }
    return true;
}
