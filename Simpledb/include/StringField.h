#pragma once
#include"Field.h"
using namespace std;
namespace Simpledb {

	class StringField : public Field {
	public:
		/**
		* Constructor.
		*
		* @param s
		*            The value of this field.
		* @param maxSize
		*            The maximum size of this string
		*/
		StringField(const string& s, int maxSize);
		/**
		* Write this string to vector. Always writes maxSize + 4 bytes to the vector
		* First four bytes are string length, next bytes are string, with
		* remainder padded with 0 to maxSize.
		* @return the vector Where the string is written
		*/
		vector<unsigned char> serialize()const override;
		/**
		* Compare the specified field to the value of this Field. Return semantics
		* are as specified by Field.compare
		*
		* @throws IllegalCastException
		*             if val is not a StringField
		* @see Field#compare
		*/
		bool compare(Predicate::Op op, const Field& value)const override;
		/**
		* @return the Type for this Field
		*/
		shared_ptr<Type> getType()const override;
		int64_t hashCode()const override;
		bool equals(const Field& feild)const override;
		string toString()const override;
		string getValue() {
			return _value;
		}
	private:
		static long _serialVersionUID;
		string _value;
		int _maxSize;
	};

}