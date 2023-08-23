#include"SeqScan.h"
#include"Database.h"
namespace Simpledb {
	SeqScan::SeqScan(shared_ptr<TransactionId> tid,
		size_t tableid, const string& tableAlias)
		:_tid(tid),_tableid(tableid),_tableAlias(tableAlias)
	{
		_tableName = Database::getCatalog()->getTableName(_tableid);
		if (_tableAlias != "") {
			shared_ptr<TupleDesc::TupleDescIter> iter =
				Database::getCatalog()->getTupleDesc(_tableid)->iterator();
			vector<shared_ptr<Type>> typeVec;
			vector<string> nameVec;
			while (iter->hasNext()) {
				auto next = iter->next();
				typeVec.push_back(next->_fieldType);
				nameVec.push_back(_tableAlias + "." + next->_fieldName);
			}
			_td = make_shared<TupleDesc>(typeVec, nameVec);
		}
		else {
			_td = Database::getCatalog()->getTupleDesc(_tableid);
		}
		
		_iter = Database::getCatalog()->getDatabaseFile(_tableid)->iterator(_tid);
	}
	SeqScan::SeqScan(shared_ptr<TransactionId> tid, size_t tableId)
		: SeqScan(tid, tableId, "")
	{
	}
	string SeqScan::getTableName()
	{
		return _tableName;
	}
	string SeqScan::getAlias()
	{
		return _tableAlias;
	}
	void SeqScan::reset(int tableid, const string& tableAlias)
	{
		_tableid = tableid;
		_tableAlias = tableAlias;
		_tableName = Database::getCatalog()->getTableName(_tableid);
		_td = Database::getCatalog()->getTupleDesc(_tableid);
		_iter = Database::getCatalog()->getDatabaseFile(_tableid)->iterator(_tid);
	}
	void SeqScan::open()
	{
		_iter->open();
	}
	shared_ptr<TupleDesc> SeqScan::getTupleDesc()
	{
		return _td;
	}
	bool SeqScan::hasNext()
	{
		return _iter->hasNext();
	}
	Tuple* SeqScan::next()
	{
		return _iter->next();
	}
	void SeqScan::close()
	{
		_iter->close();
	}
	void SeqScan::rewind()
	{
		_iter->rewind();
	}
}