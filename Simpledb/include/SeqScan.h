#pragma once
#include"OpIterator.h"
#include"TransactionId.h"
#include"DbFile.h"
#include<string>
namespace Simpledb {
	/**
	* SeqScan is an implementation of a sequential scan access method that reads
	* each tuple of a table in no particular order (e.g., as they are laid out on
	* disk).
	*/
	class SeqScan :public OpIterator {
	public:
		/**
		* Creates a sequential scan over the specified table as a part of the
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
		*/
		SeqScan(shared_ptr<TransactionId> tid, size_t tableid, const string& tableAlias);
		SeqScan(shared_ptr<TransactionId> tid, size_t tableId);
		/**
		* @return
		*       return the table name of the table the operator scans. This should
		*       be the actual name of the table in the catalog of the database
		* */
		string getTableName();
		/**
		* @return Return the alias of the table this operator scans.
		* */
		string getAlias();
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
		void reset(int tableid, const string& tableAlias);
		void open()override;
		/**
		* Returns the TupleDesc with field names from the underlying HeapFile,
		* prefixed with the tableAlias string from the constructor. This prefix
		* becomes useful when joining tables containing a field(s) with the same
		* name.  The alias and name should be separated with a "." character
		* (e.g., "alias.fieldName").
		*
		* @return the TupleDesc with field names from the underlying HeapFile,
		*         prefixed with the tableAlias string from the constructor.
		*/
		shared_ptr<TupleDesc> getTupleDesc()override;
		bool hasNext()override;
		Tuple& next()override;
		void close()override;
		void rewind()override;

	private:
		shared_ptr<TransactionId> _tid;
		size_t _tableid;
		string _tableAlias;
		string _tableName;
		shared_ptr<TupleDesc> _td;
		shared_ptr<DbFileIterator> _iter;
	};
}