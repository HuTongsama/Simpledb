#include"TupleIterator.h"
namespace Simpledb {
    long TupleIterator::_serialVersionUID = 1l;
	TupleIterator::TupleIterator(shared_ptr<TupleDesc> td, vector<shared_ptr<Tuple>>& tuples)
        : _tuples(tuples)
	{
        _td = td;
        _open = false;
        // check that all tuples are the right TupleDesc
        for (auto t : _tuples) {
            if (!t->getTupleDesc()->equals(*td))
                throw runtime_error("incompatible tuple in tuple set");
        }
	}
}