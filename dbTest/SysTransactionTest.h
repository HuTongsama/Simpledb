#pragma once
#include"SimpleDbTestBase.h"

class SysTransactionTest : public SimpleDbTestBase {
protected:
	static const int TIMEOUT_MILLIS = 10 * 60 * 1000;
};