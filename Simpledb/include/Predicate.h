#pragma once
#include<string>
#include<memory>
using namespace std;
namespace Simpledb {
	/** Predicate compares tuples to a specified Field value.
	*/
	class Field;
	class Tuple;
	class Predicate {
	public:
		/** Constants used for return codes in Field.compare */
		enum Op {
			EQUALS = 0, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS
		};
		/**
		 * Interface to access operations by a string containing an integer
		 * index for command-line convenience.
		 *
		 * @param s a string containing a valid integer Op index
		 */
		static Op getOp(string s) {
			int i = stoi(s);
			return getOp(i);
		}
		/**
		 * Interface to access operations by integer value for command-line
		 * convenience.
		 *
		 * @param i a valid integer Op index
		 */
		static Op getOp(int i) {
			return (Op)i;
			
		}
		/**
		* Constructor.
		*
		* @param field field number of passed in tuples to compare against.
		* @param op operation to use for comparison
		* @param operand field value to compare passed in tuples to
		*/
		Predicate(int field, Op op, shared_ptr<Field> operand);
		/**
		* Compares the field number of t specified in the constructor to the
		* operand field specified in the constructor using the operator specific
		* in the constructor.  The comparison can be made through Field's
		* compare method.
		*
		* @param t The tuple to compare against
		* @return true if the comparison is true, false otherwise.
		*/
		bool filter(shared_ptr<Tuple> t);
		/**
		* Returns something useful, like
		* "f = field_id op = op_string operand = operand_string
		*/
		string toString();
	};
}