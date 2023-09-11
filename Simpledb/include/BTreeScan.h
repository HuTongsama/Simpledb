#pragma once
#include"OpIterator.h"
#include"Transaction.h"
#include"IndexPredicate.h"
namespace Simpledb {
	class BTreeScan : public OpIterator {
	private:

		static long _serialVersionUID;
		bool _isOpen = false;
		shared_ptr<TransactionId> _tid;
		shared_ptr<TupleDesc> _myTd;
		shared_ptr<IndexPredicate> _ipred;
		shared_ptr<DbFileIterator> _it;
		string _tablename;
		string _alias;

	public:
		/**
		 * Creates a B+ tree scan over the specified table as a part of the
		 * specified transaction.
		 *
		 * @param tid
		 *            The transaction this scan is running as a part of.
		 * @param tableid
		 *            the table to scan.
		 * @param tableAlias
		 *            the alias of this table (needed by the parser); the returned
		 *            tupleDesc should have fields with name tableAlias.fieldName
		 *            (note: this class is not responsible for handling a case where
		 *            tableAlias or fieldName are null. It shouldn't crash if they
		 *            are, but the resulting name can be null.fieldName,
		 *            tableAlias.null, or null.null).
		 * @param ipred
		 * 			  The index predicate to match. If null, the scan will return all tuples
		 *            in sorted order
		 */
		BTreeScan(shared_ptr<TransactionId> tid, size_t tableid, const string& tableAlias, shared_ptr<IndexPredicate> ipred);
		BTreeScan(shared_ptr<TransactionId> tid, size_t tableid, shared_ptr<IndexPredicate> ipred);
		/**
		 * @return
		 *       return the table name of the table the operator scans. This should
		 *       be the actual name of the table in the catalog of the database
		 * */
		const string& getTableName() {
			return _tablename;
		}
		/**
		 * @return Return the alias of the table this operator scans.
		 * */
		const string& getAlias(){
			return _alias;
		}
		/**
		 * Returns the TupleDesc with field names from the underlying BTreeFile,
		 * prefixed with the tableAlias string from the constructor. This prefix
		 * becomes useful when joining tables containing a field(s) with the same
		 * name.
		 *
		 * @return the TupleDesc with field names from the underlying BTreeFile,
		 *         prefixed with the tableAlias string from the constructor.
		 */
		shared_ptr<TupleDesc> getTupleDesc()override {
			return _myTd;
		}
		/**
		 * Reset the tableid, and tableAlias of this operator.
		 * @param tableid
		 *            the table to scan.
		 * @param tableAlias
		 *            the alias of this table (needed by the parser); the returned
		 *            tupleDesc should have fields with name tableAlias.fieldName
		 *            (note: this class is not responsible for handling a case where
		 *            tableAlias or fieldName are null. It shouldn't crash if they
		 *            are, but the resulting name can be null.fieldName,
		 *            tableAlias.null, or null.null).
		 */
		void reset(size_t tableid, const string& tableAlias);
		void open()override;
		bool hasNext()override;
		Tuple* next()override;
		void close()override;
		void rewind()override;
	};
}