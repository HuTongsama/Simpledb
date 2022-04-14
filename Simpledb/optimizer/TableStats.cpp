#include"TableStats.h"
#include"Database.h"
#include"IntField.h"
#include"StringField.h"
namespace Simpledb {
	ConcurrentMap<string, shared_ptr<TableStats>> TableStats::_map;
	shared_ptr<TableStats> TableStats::getTableStats(const string& tablename)
	{
		return *(_map.getValue(tablename));
	}
	void TableStats::setTableStats(const string& tablename, shared_ptr<TableStats> stats)
	{
		_map.setValue(tablename, stats);
	}
	void TableStats::setStatsMap(ConcurrentMap<string, shared_ptr<TableStats>>& s)
	{
		_map.reset(s);
	}
	const ConcurrentMap<string, shared_ptr<TableStats>>& TableStats::getStatsMap()
	{
		return _map;
	}
	void TableStats::computeStatistics()
	{
		shared_ptr<Iterator<size_t>> tableIt = Database::getCatalog()->tableIdIterator();
		cout << "Computing table stats." << endl;
		while (tableIt->hasNext()) {
			size_t tableid = tableIt->next();
			shared_ptr<TableStats> s = make_shared<TableStats>(tableid, IOCOSTPERPAGE);
			setTableStats(Database::getCatalog()->getTableName(tableid), s);
		}
		cout << "Done." << endl;
	}
	TableStats::TableStats(size_t tableid, int ioCostPerPage)
	{
		// For this function, you'll have to get the
		// DbFile for the table in question,
		// then scan through its tuples and calculate
		// the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything
		// in a single scan of the table.
		// some code goes here
		shared_ptr<DbFile> dbFile = Database::getCatalog()->getDatabaseFile(tableid);
		_numPages = dbFile->numPages();
		_numTuples = 0;
		_ioCostPerPage = ioCostPerPage;
		shared_ptr<TupleDesc> td = dbFile->getTupleDesc();
		size_t numFields = td->numFields();
		map<int, pair<int, int>> fieldToMinMax;
		map<int, vector<int>> fieldToIntVal;
		for (size_t i = 0; i < numFields; ++i) {
			Type::TYPE t = td->getFieldType(i)->type();
			switch (t)
			{
			case Simpledb::Type::TYPE::INT_TYPE:
				fieldToMinMax[i].first = INT_MAX;
				fieldToMinMax[i].second = INT_MIN;
				break;
			case Simpledb::Type::TYPE::STRING_TYPE:
				_fieldToStringHist[i] = StringHistogram(NUM_HIST_BINS);
				break;
			default:
				break;
			}
			
		}
		shared_ptr<TransactionId> tid = make_shared<TransactionId>();
		shared_ptr<DbFileIterator> iter = dbFile->iterator(tid);
		iter->open();
		while (iter->hasNext()) {
			Tuple& tuple = iter->next();
			_numTuples++;
			for (size_t i = 0; i < numFields; ++i) {
				shared_ptr<Field> f = tuple.getField(i);
				Type::TYPE type = f->getType();
				switch (type)
				{
				case Simpledb::Type::TYPE::INT_TYPE:
				{					
					shared_ptr<IntField> iField = dynamic_pointer_cast<IntField>(f);
					int val = iField->getValue();
					fieldToMinMax[i].first = min(fieldToMinMax[i].first, val);
					fieldToMinMax[i].second = max(fieldToMinMax[i].second, val);
					fieldToIntVal[i].push_back(val);
				}
					break;
				case Simpledb::Type::TYPE::STRING_TYPE:
				{
					shared_ptr<StringField> sField = dynamic_pointer_cast<StringField>(f);
					_fieldToStringHist[i].addValue(sField->getValue());
				}
					
					break;
				default:
					break;
				}
			}
		}
		iter->close();
		for (auto& iter : fieldToIntVal) {
			int minVal = fieldToMinMax[iter.first].first;
			int maxVal = fieldToMinMax[iter.first].second;
			_fieldToIntHist[iter.first] = IntHistogram(NUM_HIST_BINS, minVal, maxVal);
			IntHistogram& hist = _fieldToIntHist[iter.first];
			for (auto val : iter.second) {
				hist.addValue(val);
			}
		}
		Database::getBufferPool()->transactionComplete(tid);
	}
	double TableStats::estimateScanCost()
	{
		return _numPages * _ioCostPerPage;
	}
	int TableStats::estimateTableCardinality(double selectivityFactor)
	{
		return selectivityFactor * _numTuples;
	}
	double TableStats::avgSelectivity(int field, Predicate::Op op)
	{
		if (_fieldToIntHist.find(field) != _fieldToIntHist.end()) {
			return _fieldToIntHist[field].avgSelectivity();
		}
		if (_fieldToStringHist.find(field) != _fieldToStringHist.end()) {
			return _fieldToStringHist[field].avgSelectivity();
		}
		return 0.0;
	}
	double TableStats::estimateSelectivity(int field, Predicate::Op op, shared_ptr<Field> constant)
	{
		if (_fieldToIntHist.find(field) != _fieldToIntHist.end()) {
			int val = dynamic_pointer_cast<IntField>(constant)->getValue();
			return _fieldToIntHist[field].estimateSelectivity(op, val);
		}
		if (_fieldToStringHist.find(field) != _fieldToStringHist.end()) {
			string val = dynamic_pointer_cast<StringField>(constant)->getValue();
			return _fieldToStringHist[field].estimateSelectivity(op, val);
		}
		return 0.0;
	}
	int TableStats::totalTuples()
	{
		return _numTuples;
	}
}