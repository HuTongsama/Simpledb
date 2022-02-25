#pragma once
#include<memory>
#include<iostream>
#include"Noncopyable.h"
#include"DataStream.h"
using namespace std;
namespace Simpledb
{
	class Field;
	/**
	* Class representing a type in SimpleDB.
	*/
	class Type : public Noncopyable
	{
	public:
		virtual ~Type() {}
		/**
		* @return the number of bytes required to store a field of this type.
		*/
		virtual int getLen() = 0;
		/**
		* @return a Field object of the same type as this object that has contents
		* read from the specified DataInputStream.
		* @param instream The input stream to read from
		*/
		virtual shared_ptr<Field> parse(DataStream& ds) = 0;
		virtual string toString() = 0;
		static int STRING_LEN;
	};

	class Int_Type : public Type 
	{
	public:
		int getLen()override { return 4; }
		shared_ptr<Field> parse(DataStream& ds)override;
		string toString()override {
			return "INT_TYPE";
		}
		static shared_ptr<Int_Type> INT_TYPE;
	private:
		Int_Type() = default;
	};

	class String_Type : public Type
	{
	public:
		int getLen()override { return STRING_LEN + 4; }
		shared_ptr<Field> parse(DataStream& ds)override;
		string toString()override {
			return "STRING_TYPE";
		}
		static shared_ptr<String_Type> STRING_TYPE;
	private:
		String_Type() = default;
	};
};