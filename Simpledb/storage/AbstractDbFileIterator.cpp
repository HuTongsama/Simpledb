#include"AbstractDbFileIterator.h"
namespace Simpledb {
	bool AbstractDbFileIterator::hasNext()
	{
		if (_next == nullptr) {
			_next = readNext();
		} 
		return _next != nullptr;
	}
	Tuple& AbstractDbFileIterator::next()
	{
		if (_next == nullptr) {
			_next = readNext();
			if (_next == nullptr)
				throw runtime_error("no such element");
		}
		Tuple* result = _next;
		_next = nullptr;
		return *result;
	}
	void AbstractDbFileIterator::close()
	{
		// Ensures that a future call to next() will fail
		_next = nullptr;
	}
}