#include"TupleIterator.h"
namespace Simpledb {
    long TupleIterator::_serialVersionUID = 1l;
    TupleIterator::TupleIterator(shared_ptr<TupleDesc> td, const vector<shared_ptr<Tuple>>& tuples)
        : _td(td), _tuples(tuples), _open(false)
	{
        // check that all tuples are the right TupleDesc
        for (auto t : _tuples) {
            if (t != nullptr && !t->getTupleDesc()->equals(*td))
                throw runtime_error("incompatible tuple in tuple set");
        }
	}
}