#pragma once
#include<string>
#include<memory>
#include<stdexcept>
using namespace std;
namespace Simpledb {
	/** Predicate compares tuples to a specified Field value.
	*/
	class Field;
	class Tuple;
	class Predicate {
	public:
		/** Constants used for return codes in Field.compare */
		enum class Op{
			EQUALS = 0, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS
		};
		/**
		 * Interface to access operations by integer value for command-line
		 * convenience.
		 *
		 * @param i a valid integer Op index
		 */
		static Op getOp(int i) {
			return (Op)i;
			
		}
		static string opToString(Op op) {
			switch (op)
			{
			case Simpledb::Predicate::Op::EQUALS:
				return "=";
			case Simpledb::Predicate::Op::GREATER_THAN:
				return ">";
			case Simpledb::Predicate::Op::LESS_THAN:
				return "<";
			case Simpledb::Predicate::Op::LESS_THAN_OR_EQ:
				return "<=";
			case Simpledb::Predicate::Op::GREATER_THAN_OR_EQ:
				return ">=";
			case Simpledb::Predicate::Op::LIKE:
				return "LIKE";
			case Simpledb::Predicate::Op::NOT_EQUALS:
				return "<>";
			default:
				throw runtime_error("no such op");
			}
		}
		/**
		* Constructor.
		*
		* @param field 
		*				field number of passed in tuples to compare against.
		* @param op 
		*				operation to use for comparison
		* @param operand 
		*				field value to compare passed in tuples to
		*/
		Predicate(int field, Op op, shared_ptr<Field> operand);
		/**
		* @return the field number
		*/
		int getField();
		/**
		* @return the operator
		*/
		Op getOp();
		/**
		* @return the operand
		*/
		shared_ptr<Field> getOperand();
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
		bool filter(Tuple& t);
		/**
		* Returns something useful, like
		* "f = field_id op = op_string operand = operand_string
		*/
		string toString();
	private:
		int _field;
		Op _op;
		shared_ptr<Field> _operand;
		string _str;
	};
}