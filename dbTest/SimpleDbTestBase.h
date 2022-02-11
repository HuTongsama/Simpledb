#pragma once
#include "pch.h"
#include"Database.h"
class SimpleDbTestBase :public ::testing::Test {
protected:
	void SetUp()override {
		Simpledb::Database::reset();
	}
};