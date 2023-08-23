#include"TupleIterator.h"
namespace Simpledb {
    long TupleIterator::_serialVersionUID = 1l;
    TupleIterator::TupleIterator(shared_ptr<TupleDesc> td, const vector<shared_ptr<Tuple>>& tuples)
        :_td(td), _open(false)
	{
        _items = tuples;
        // check that all tuples are the right TupleDesc
        for (auto t : _items) {
            if (t != nullptr && !t->getTupleDesc()->equals(*td))
                throw runtime_error("incompatible tuple in tuple set");
        }
	}
}