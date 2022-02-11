#pragma once
#include"Field.h"
#include"Predicate.h"
namespace Simpledb
{
	/**
	* Instance of Field that stores a single integer.
	*/
	class IntField :public Field 
	{
	public:
		/**
		* Constructor.
		*
		* @param i The value of this field.
		*/
		IntField(int i) :_value(i) {}
		void serialize(ostream& outStream)const override;
		/**
		* Compare the specified field to the value of this Field.
		* Return semantics are as specified by Field.compare
		*/
		bool compare(Predicate::Op op, const Field& value)const override;
		/**
		* Return the Type of this field.
		* @return Int_Type::INT_TYPE
		*/
		shared_ptr<Type> getType()const override;
		int64_t hashCode()const override;
		bool equals(const Field& feild)const override;
		string toString()const override;
		int getValue() {
			return _value;
		}
	private:
		static long _serialVersionUID;
		int _value;		
	};
}