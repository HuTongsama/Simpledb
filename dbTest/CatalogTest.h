#pragma once
#include"SimpleDbTestBase.h"
#include"TestUtil.h"
#include"SystemTestUtil.h"
#include"Database.h"
#include"Utility.h"
#include<ctime>
#include<random>
#include<string>
using namespace std;
using namespace Simpledb;
class CatalogTest :public::SimpleDbTestBase {
protected:
	void SetUp()override;

	static default_random_engine _r;
	static string _name;
	static int _id1;
	static int _id2;
	string _nameThisTestRun;
};

TEST_F(CatalogTest, GetTupleDesc) {
	shared_ptr<TupleDesc> expected = Utility::getTupleDesc(2);
	shared_ptr<TupleDesc> actual = Database::getCatalog()->getTupleDesc(_id1);
	EXPECT_TRUE(expected->equals(*actual));
}
TEST_F(CatalogTest, GetTableId) {

    EXPECT_EQ(_id2, Database::getCatalog()->getTableId(_name));
    EXPECT_EQ(_id1, Database::getCatalog()->getTableId(_nameThisTestRun));
    EXPECT_EQ(-1, Database::getCatalog()->getTableId(""));
    EXPECT_EQ(-1, Database::getCatalog()->getTableId("foo"));
}
TEST_F(CatalogTest, GetDatabaseFile) {
	shared_ptr<DbFile> f = Database::getCatalog()->getDatabaseFile(_id1);
	// not to dig too deeply into the DbFile API here; we
	// rely on HeapFileTest for that. perform some basic checks.
	EXPECT_EQ(_id1, f->getId());
}
TEST_F(CatalogTest, HandleDuplicateNames) {
	int id3 = _r();
	Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(id3, Utility::getTupleDesc(2)), _name);
	EXPECT_EQ(id3, Database::getCatalog()->getTableId(_name));
}
TEST_F(CatalogTest, HandleDuplicateIds) {
	string newName = SystemTestUtil::getUUID();
	shared_ptr<DbFile> f = make_shared<TestUtil::SkeletonFile>(_id2, Utility::getTupleDesc(2));
	Database::getCatalog()->addTable(f, newName);
	EXPECT_EQ(newName, Database::getCatalog()->getTableName(_id2));
	EXPECT_EQ(f->getId(), Database::getCatalog()->getDatabaseFile(_id2)->getId());
}