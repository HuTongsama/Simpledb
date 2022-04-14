#include"TableStats.h"
#include"Database.h"
namespace Simpledb {
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
	TableStats::TableStats(int tableid, int ioCostPerPage)
	{
		// For this function, you'll have to get the
		// DbFile for the table in question,
		// then scan through its tuples and calculate
		// the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything
		// in a single scan of the table.
		// some code goes here
	}
	double TableStats::estimateScanCost()
	{
		return 0.0;
	}
	int TableStats::estimateTableCardinality(double selectivityFactor)
	{
		return 0;
	}
	double TableStats::avgSelectivity(int field, Predicate::Op op)
	{
		return 0.0;
	}
	double TableStats::estimateSelectivity(int field, Predicate::Op op, shared_ptr<Field> constant)
	{
		return 0.0;
	}
	int TableStats::totalTuples()
	{
		return 0;
	}
}