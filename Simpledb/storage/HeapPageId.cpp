#include"HeapPageId.h"
namespace Simpledb {
	HeapPageId::HeapPageId(int tableId, int pgNo)
	{
	}
	vector<int> HeapPageId::serialize()const
	{
		vector<int> result(2, 0);
		result[0] = getTableId();
		result[1] = getPageNumber();
		return result;
	}
	int HeapPageId::getTableId()const
	{
		return 0;
	}
	int HeapPageId::getPageNumber()const
	{
		return 0;
	}
	int64_t HeapPageId::hashCode()const
	{
		return int64_t();
	}
	bool HeapPageId::equals(const PageId& pId)const
	{
		return false;
	}
}