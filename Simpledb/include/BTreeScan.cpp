#include"BTreeScan.h"
#include"BTreeFile.h"
namespace Simpledb {
    BTreeScan::BTreeScan(shared_ptr<TransactionId> tid, size_t tableid, const string& tableAlias, shared_ptr<IndexPredicate> ipred)
        :_tid(tid),_ipred(ipred)
    {
        reset(tableid, tableAlias);
    }
    BTreeScan::BTreeScan(shared_ptr<TransactionId> tid, size_t tableid, shared_ptr<IndexPredicate> ipred)
        :BTreeScan(tid, tableid, Database::getCatalog()->getTableName(tableid), ipred)
    {
    }
    void BTreeScan::reset(size_t tableid, const string& tableAlias)
    {
		_isOpen = false;
		_alias = tableAlias;
		_tablename = Database::getCatalog()->getTableName(tableid);
		if (_ipred == nullptr) {
			_it = Database::getCatalog()->getDatabaseFile(tableid)->iterator(_tid);
		}
		else {
			_it = dynamic_pointer_cast<BTreeFile>
				(Database::getCatalog()->getDatabaseFile(tableid))->indexIterator(_tid, *_ipred);
		}
		_myTd = Database::getCatalog()->getTupleDesc(tableid);
		size_t filedSz = _myTd->numFields();
		vector<string> newNames(filedSz);
		vector<shared_ptr<Type>> newTypes(filedSz);
		for (int i = 0; i < filedSz; i++) {
			const string& name = _myTd->getFieldName(i);
			shared_ptr<Type> t = _myTd->getFieldType(i);

			newNames[i] = tableAlias + "." + name;
			newTypes[i] = t;
		}
		_myTd = make_shared<TupleDesc>(newTypes, newNames);
    }
	void BTreeScan::open()
	{
		if (_isOpen)
			throw runtime_error("BTreeScan::hasNext::double open on one OpIterator.");

		_it->open();
		_isOpen = true;
	}
	bool BTreeScan::hasNext()
	{
		if (!_isOpen)
			throw new runtime_error("BTreeScan::hasNext::iterator is closed");
		return _it->hasNext();
	}
	Tuple* BTreeScan::next()
	{
		if (!_isOpen)
			throw runtime_error("BTreeScan::next::iterator is closed");

		return _it->next();
	}
	void BTreeScan::close()
	{
		_it->close();
		_isOpen = false;
	}
	void BTreeScan::rewind()
	{
		close();
		open();
	}
}