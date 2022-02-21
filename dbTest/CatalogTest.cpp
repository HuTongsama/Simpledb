#include "pch.h"
#include"CatalogTest.h"

default_random_engine CatalogTest::_r(time(0));
string CatalogTest::_name = SystemTestUtil::getUUID();
size_t CatalogTest::_id1 = CatalogTest::_r();
size_t CatalogTest::_id2 = CatalogTest::_r();

void CatalogTest::SetUp()
{
	SimpleDbTestBase::SetUp();
	Database::getCatalog()->clear();
	_nameThisTestRun = SystemTestUtil::getUUID();
	Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(_id1, Utility::getTupleDesc(2)), _nameThisTestRun);
	Database::getCatalog()->addTable(make_shared<TestUtil::SkeletonFile>(_id2, Utility::getTupleDesc(2)), _name);
}
