#pragma once
#include<string>
#include<memory>
#include<map>
#include"Predicate.h"
#include"Field.h"
#include"ConcurrentMap.h"
using namespace std;
namespace Simpledb {
	class TableStats {
	public:
		static shared_ptr<TableStats> getTableStats(const string& tablename);
		static void setTableStats(const string& tablename, shared_ptr<TableStats> stats);
		static void setStatsMap(ConcurrentMap<string, shared_ptr<TableStats>>& s);
		static const ConcurrentMap<string,shared_ptr<TableStats>>& getStatsMap();
		static void computeStatistics();

		/**
		* Create a new TableStats object, that keeps track of statistics on each
		* column of a table
		*
		* @param tableid
		*            The table over which to compute statistics
		* @param ioCostPerPage
		*            The cost per page of IO. This doesn't differentiate between
		*            sequential-scan IO and disk seeks.
		*/
		TableStats(int tableid, int ioCostPerPage);
		/**
		* Estimates the cost of sequentially scanning the file, given that the cost
		* to read a page is costPerPageIO. You can assume that there are no seeks
		* and that no pages are in the buffer pool.
		*
		* Also, assume that your hard drive can only read entire pages at once, so
		* if the last page of the table only has one tuple on it, it's just as
		* expensive to read as a full page. (Most real hard drives can't
		* efficiently address regions smaller than a page at a time.)
		*
		* @return The estimated cost of scanning the table.
		*/
		double estimateScanCost();
		/**
		* This method returns the number of tuples in the relation, given that a
		* predicate with selectivity selectivityFactor is applied.
		*
		* @param selectivityFactor
		*            The selectivity of any predicates over the table
		* @return The estimated cardinality of the scan with the specified
		*         selectivityFactor
		*/
		int estimateTableCardinality(double selectivityFactor);
		/**
		* The average selectivity of the field under op.
		* @param field
		*        the index of the field
		* @param op
		*        the operator in the predicate
		* The semantic of the method is that, given the table, and then given a
		* tuple, of which we do not know the value of the field, return the
		* expected selectivity. You may estimate this value from the histograms.
		* */
		double avgSelectivity(int field, Predicate::Op op);
		/**
		* Estimate the selectivity of predicate <tt>field op constant</tt> on the
		* table.
		*
		* @param field
		*            The field over which the predicate ranges
		* @param op
		*            The logical operation in the predicate
		* @param constant
		*            The value against which the field is compared
		* @return The estimated selectivity (fraction of tuples that satisfy) the
		*         predicate
		*/
		double estimateSelectivity(int field, Predicate::Op op, shared_ptr<Field> constant);
		/**
		* return the total number of tuples in this table
		* */
		int totalTuples();

	private:
		static const int IOCOSTPERPAGE = 1000;
		/**
		* Number of bins for the histogram. Feel free to increase this value over
		* 100, though our tests assume that you have at least 100 bins in your
		* histograms.
		*/
		static const int NUM_HIST_BINS = 100;
		static ConcurrentMap<string, shared_ptr<TableStats>> _map;
		
	};
}