#include"RecordId.h"
#include"Common.h"
#include<sstream>
namespace Simpledb {
	long RecordId::_serialVersionUID = 1l;
	RecordId::RecordId(shared_ptr<PageId> pid, int tupleNo)
		:_pid(pid), _tupleNo(tupleNo), _hashcode(0)
	{
	}
	int RecordId::getTupleNumber()
	{
		return _tupleNo;
	}
	shared_ptr<PageId> RecordId::getPageId()
	{
		return _pid;
	}
	bool RecordId::equals(RecordId& rid)
	{
		return hashCode() == rid.hashCode();
	}
	size_t RecordId::hashCode()
	{
		if (_combineStr == "") {
			stringstream ss;
			_combineStr = combineArgs(ss, "_", _pid->hashCode(), _tupleNo);
			hash<string> hashFunc;
			_hashcode = hashFunc(_combineStr);
		}
		return _hashcode;
	}
}