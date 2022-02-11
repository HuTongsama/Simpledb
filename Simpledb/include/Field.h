#pragma once
#include<iostream>
#include<string>
#include<functional>
#include"Predicate.h"
#include"Type.h"
using namespace std;
namespace Simpledb
{
	
   /**
	* Interface for values of fields in tuples in SimpleDB.
	*/
	class Field 
	{
	public:
		virtual ~Field() {}
		/**
		* Write the bytes representing this field to the specified		
		* @param outStream The ostream to write to.
		*/
		virtual void serialize(ostream& outStream)const = 0;
		/**
		* Compare the value of this field object to the passed in value.
		* @param op The operator
		* @param value The value to compare this Field to
		* @return Whether or not the comparison yields true.
		*/
		virtual bool compare(Predicate::Op op, const Field& value)const = 0;
		/**
		* Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
		* @return type of this field
		*/
		virtual shared_ptr<Type> getType()const = 0;
		/**
		* Hash code.
		* Different Field objects representing the same value should probably
		* return the same hashCode.
		*/
		virtual int64_t hashCode()const = 0;
		virtual bool equals(const Field& feild)const = 0;
		virtual string toString()const = 0;
	};
}