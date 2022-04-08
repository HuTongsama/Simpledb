#include"pch.h"
#include"SystemTestUtil.h"
int SystemTestUtil::MAX_RAND_VALUE = 1 << 16;
shared_ptr<TupleDesc> SystemTestUtil::SINGLE_INT_DESCRIPTOR = make_shared<TupleDesc>(vector<shared_ptr<Type>>{Int_Type::INT_TYPE() });