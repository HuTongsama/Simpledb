#pragma once
#include"Type.h"
#include"Collection.h"
#include<vector>
namespace Simpledb {
	
	class TupleDesc : public Noncopyable {
	public:
		/*
		* A help class to facilitate organizing the information of each field
		*/
		class TDItem {
		public:	
			TDItem(shared_ptr<Type> type, const string& name) :
				_fieldType(type), _fieldName(name) {}
			string toString()const {
				return _fieldName + "(" + _fieldType->toString() + ")";
			}			
			shared_ptr<Type> _fieldType;
			string _fieldName;
		private:
			static long _serialVersionUID;
		};
		class TupleDescIter :public Iterator<TDItem> {
		public:
			TupleDescIter(TupleDesc* p);
			bool hasNext()override;
			TDItem* next()override;
			void remove()override;
		private:
			TupleDesc* _pTd;
			size_t _position;
		};
		/*
		* @return
		*        An iterator which iterates over all the field TDItems
		*        that are included in this TupleDesc
		*/
		const TupleDescIter& iterator() { return _tdIter; }
		TupleDesc():_tdIter(this) {}
		/**
		* Create a new TupleDesc with typeAr.length fields with fields of the
		* specified types, with associated named fields.
		*
		* @param typeVec
		*            vector specifying the number of and types of fields in this
		*            TupleDesc. It must contain at least one entry.
		* @param fieldVec
		*            vector specifying the names of the fields. Note that names may
		*            be null.
		*/
		TupleDesc(const vector<shared_ptr<Type>>& typeVec, const vector<string>& fieldVec);
		/**
		* Constructor. Create a new tuple desc with typeAr.length fields with
		* fields of the specified types, with anonymous (unnamed) fields.
		*
		* @param typeVec
		*            vector specifying the number of and types of fields in this
		*            TupleDesc. It must contain at least one entry.
		*/
		TupleDesc(const vector<shared_ptr<Type>>& typeVec);
		/**
		* @return the number of fields in this TupleDesc
		*/
		int numFields()const;
		/**
		* Gets the (possibly null) field name of the ith field of this TupleDesc.
		*
		* @param i index of the field name to return. It must be a valid index.
		* @return the name of the ith field
		*		  empty string if i is invalid
		*/
		string getFieldName(int i)const;
		/**
		* Gets the type of the ith field of this TupleDesc.
		*
		* @param i The index of the field to get the type of. It must be a valid
		*         index.
		* @return the type of the ith field
		*		  nullptr if i is invalid
		*/
		shared_ptr<Type> getFieldType(int i)const;
		/**
		* Find the index of the field with a given name.
		*
		* @param name
		*            name of the field.
		* @return the index of the field that is first to have the given name.
				  -1 if no field with a matching name is found.
		*/
		int fieldNameToIndex(const string& name)const;
		/**
		* @return The size (in bytes) of tuples corresponding to this TupleDesc.
		*         Note that tuples from a given TupleDesc are of a fixed size.
		*/
		int getSize()const;
		/**
		* Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
		* with the first td1.numFields coming from td1 and the remaining from td2.
		*
		* @param td1
		*            The TupleDesc with the first fields of the new TupleDesc
		* @param td2
		*            The TupleDesc with the last fields of the TupleDesc
		* @return the new TupleDesc
		*/
		static shared_ptr<TupleDesc> merge(const TupleDesc& td1, const TupleDesc& td2);
		/**
		* Compares the specified object with this TupleDesc for equality. Two 
		* TupleDescs are considered equal if they have the same number of items 
		* and if the i-th type in this TupleDesc is equal to the i-th type in td
		* for every i.
		*
		* @param o
		*         the Object to be compared for equality with this TupleDesc.
		* @return true if the object is equal to this TupleDesc.
		*/
		bool equals(const TupleDesc& td)const;
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		size_t hashCode()const;
		/**
		* Returns a String describing this descriptor. It should be of the form 
		* "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although 
		* the exact format does not matter.
		*
		* @return String describing this descriptor.
		*/
		string toString()const;
	private:
		static long _serialVersionUID;
		vector<TDItem> _tdItems;
		TupleDescIter _tdIter;
	};
}