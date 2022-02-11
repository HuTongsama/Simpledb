#include"TransactionId.h"
namespace Simpledb {
	atomic_int64_t TransactionId::_counter = 0;
}