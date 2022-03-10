#include"Operator.h"
namespace Simpledb {
	long Operator::_serialVersionUID = 1l;
	bool Operator::hasNext()
	{
		if (!_open)
			throw runtime_error("Operator not yet open");

		if (_next == nullptr)
			_next = fetchNext();
		return _next != nullptr;
	}
	Tuple& Operator::next()
	{
		if (!_open)
			throw runtime_error("Operator not yet open");
		if (_next == nullptr) {
			_next = fetchNext();
			if (_next == nullptr)
				throw runtime_error("no such element");
		}

		Tuple& result = *_next;
		_next = nullptr;
		return result;
	}
}