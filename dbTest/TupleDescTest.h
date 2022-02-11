#pragma once
#include"TupleDesc.h"
#include"Utility.h"
#include"SimpleDbTestBase.h"
using namespace Simpledb;
class TupleDescTest :public::SimpleDbTestBase {
	
protected:
	bool combinedStringArrays(shared_ptr<TupleDesc> td1, shared_ptr<TupleDesc> td2, shared_ptr<TupleDesc> combined);
};
TEST_F(TupleDescTest, Combine) {
    shared_ptr<TupleDesc> td1, td2, td3;

    td1 = Utility::getTupleDesc(1, "td1");
    td2 = Utility::getTupleDesc(2, "td2");

    // test td1.combine(td2)
    td3 = TupleDesc::merge(*td1, *td2);
    EXPECT_EQ(3, td3->numFields());
    EXPECT_EQ(3 * Int_Type::INT_TYPE->getLen(), td3->getSize());
    
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(Int_Type::INT_TYPE->toString(), td3->getFieldType(i)->toString());
    }
    EXPECT_TRUE(combinedStringArrays(td1, td2, td3));

    // test td2.combine(td1)
    td3 = TupleDesc::merge(*td2, *td1);
    EXPECT_EQ(3, td3->numFields());
    EXPECT_EQ(3 * Int_Type::INT_TYPE->getLen(), td3->getSize());
    
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(Int_Type::INT_TYPE->toString(), td3->getFieldType(i)->toString());
    }
    EXPECT_TRUE(combinedStringArrays(td2, td1, td3));

    // test td2.combine(td2)
    td3 = TupleDesc::merge(*td2, *td2);
    EXPECT_EQ(4, td3->numFields());
    EXPECT_EQ(4 * Int_Type::INT_TYPE->getLen(), td3->getSize());
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(Int_Type::INT_TYPE->toString(), td3->getFieldType(i)->toString());
    }
    EXPECT_TRUE(combinedStringArrays(td2, td2, td3));
}
TEST_F(TupleDescTest, GetType) {
    vector<int> lengths{ 1,2,1000 };
    for (auto len : lengths) {
        shared_ptr<TupleDesc> td = Utility::getTupleDesc(len);
        for (int i = 0; i < len; ++i) {
            EXPECT_EQ(Int_Type::INT_TYPE->toString(), td->getFieldType(i)->toString());
        }
    }
}
TEST_F(TupleDescTest, NameToId) {
    vector<int> lengths{ 1,2,2000 };
    string prefix = "test";

    for (int len : lengths) {
        // Make sure you retrieve well-named fields
        shared_ptr<TupleDesc> td = Utility::getTupleDesc(len, prefix);
        for (int i = 0; i < len; ++i) {
            EXPECT_EQ(i, td->fieldNameToIndex(prefix + to_string(i)));
        }

        EXPECT_EQ(-1, td->fieldNameToIndex("foo"));

        td = Utility::getTupleDesc(len);
        EXPECT_EQ(-1, td->fieldNameToIndex(prefix));
    }
}
TEST_F(TupleDescTest, GetSize) {

    vector<int> lengths{ 1,2,1000 };
    for (int len : lengths) {
        shared_ptr<TupleDesc> td = Utility::getTupleDesc(len);
        EXPECT_EQ(len * Int_Type::INT_TYPE->getLen(), td->getSize());
    }
}
TEST_F(TupleDescTest, NumFields) {
    vector<int> lengths{ 1,2,1000 };
    for (int len : lengths) {
        shared_ptr<TupleDesc> td = Utility::getTupleDesc(len);
        EXPECT_EQ(len, td->numFields());
    }
}
TEST_F(TupleDescTest, TestEquals) {
    shared_ptr<TupleDesc> singleInt = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ Int_Type::INT_TYPE });
    shared_ptr<TupleDesc> singleInt2 = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ Int_Type::INT_TYPE });
    shared_ptr<TupleDesc> intString = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ Int_Type::INT_TYPE, String_Type::STRING_TYPE });
    shared_ptr<TupleDesc> intString2 = make_shared<TupleDesc>(vector<shared_ptr<Type>>{ Int_Type::INT_TYPE, String_Type::STRING_TYPE });
  
    EXPECT_TRUE(singleInt->equals(*singleInt));
    EXPECT_TRUE(singleInt->equals(*singleInt2));
    EXPECT_TRUE(singleInt2->equals(*singleInt));
    EXPECT_TRUE(intString->equals(*intString));
    
    EXPECT_FALSE(singleInt->equals(*intString));
    EXPECT_FALSE(singleInt2->equals(*intString));
    EXPECT_FALSE(intString->equals(*singleInt));
    EXPECT_FALSE(intString->equals(*singleInt2));
    
    EXPECT_TRUE(intString->equals(*intString2));
    EXPECT_TRUE(intString2->equals(*intString));
}