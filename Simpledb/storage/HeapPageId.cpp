#include"HeapPageId.h"
#include"Common.h"
#include<functional>
#include<sstream>
namespace Simpledb {
	HeapPageId::HeapPageId(size_t tableId, size_t pgNo)
		:_tableId(tableId), _pgNo(pgNo), _hashcode(0)
	{
	}
	vector<size_t> HeapPageId::serialize()const
	{
		vector<size_t> result(2, 0);
		result[0] = getTableId();
		result[1] = getPageNumber();
		return result;
	}
	size_t HeapPageId::getTableId()const
	{
		return _tableId;
	}
	size_t HeapPageId::getPageNumber()const
	{
		return _pgNo;
	}
	size_t HeapPageId::hashCode()const
	{
		if (_combineStr == "") {
			stringstream ss;
			_combineStr = combineArgs(ss, "_", _tableId, _pgNo);
			std::hash<string> hashFunc;
			_hashcode = hashFunc(_combineStr);
		}
		return _hashcode;
	}
	bool HeapPageId::equals(const PageId& pId)const
	{
		try
		{
			const HeapPageId& hid = dynamic_cast<const HeapPageId&>(pId);
			return hashCode() == hid.hashCode();
		}
		catch (const std::exception&)
		{
			return false;
		}
	}

}