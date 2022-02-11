#pragma once
#include"TupleDesc.h"
#include"RecordId.h"
#include"Field.h"
namespace Simpledb {
	
	class Tuple {
	public:
		/**
		* Create a new tuple with the specified schema (type).
		*
		* @param td
		*          the schema of this tuple. It must be a valid TupleDesc
		*          instance with at least one field.
		*/
		Tuple(shared_ptr<TupleDesc> td);
		/**
		* @return The TupleDesc representing the schema of this tuple.
		*/
		const TupleDesc* getTupleDesc();
		/**
		* @return The RecordId representing the location of this tuple on disk. May
		*         be null.
		*/
		const RecordId* getRecordId();
		/**
		* Set the RecordId information for this tuple.
		*
		* @param rid
		*            the new RecordId for this tuple.
		*/
		void setRecordId(shared_ptr<RecordId> rid);
		/**
		* Change the value of the ith field of this tuple.
		*
		* @param i
		*         index of the field to change. It must be a valid index.
		* @param field
		*         new value for the field.
		*/
		void setField(int i, shared_ptr<Field> field);
		/**
		* @return the value of the ith field, or null if it has not been set.
		*
		* @param i
		*         field index to return. Must be a valid index.
		*/
		const Field* getField(int i);
		/**
		* Returns the contents of this Tuple as a string. Note that to pass the
		* system tests, the format needs to be as follows:
		*
		* column1\tcolumn2\tcolumn3\t...\tcolumnN
		*
		* where \t is any whitespace (except a newline)
		*/
		string toString();
		/**
		* @return
		*        An iterator which iterates over all the fields of this tuple
		* */
		Iterator<shared_ptr<Field>> fields();
		/**
		* reset the TupleDesc of this tuple (only affecting the TupleDesc)
		* */
		void resetTupleDesc(shared_ptr<TupleDesc> td);

	private:
		static long _serialVersionUID;
	};
}