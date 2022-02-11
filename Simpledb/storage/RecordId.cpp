#include"RecordId.h"
namespace Simpledb {
	long RecordId::_serialVersionUID = 1l;
	RecordId::RecordId(shared_ptr<PageId> pid, int tupleNo)
	{
	}
	int RecordId::getTupleNumber()
	{
		return 0;
	}
	shared_ptr<PageId> RecordId::getPageId()
	{
		return shared_ptr<PageId>();
	}
	bool RecordId::equals(const RecordId& rid)
	{
		return false;
	}
	size_t RecordId::hashCode()
	{
		return size_t();
	}
}