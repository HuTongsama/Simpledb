#include"HeapPageId.h"
namespace Simpledb {
	HeapPageId::HeapPageId(size_t tableId, size_t pgNo)
	{
	}
	vector<int> HeapPageId::serialize()const
	{
		vector<int> result(2, 0);
		result[0] = getTableId();
		result[1] = getPageNumber();
		return result;
	}
	size_t HeapPageId::getTableId()const
	{
		return 0;
	}
	size_t HeapPageId::getPageNumber()const
	{
		return 0;
	}
	size_t HeapPageId::hashCode()const
	{
		return int64_t();
	}
	bool HeapPageId::equals(const PageId& pId)const
	{
		return false;
	}
}