#pragma once
#include"PageId.h"
#include<memory>
#include<string>
using namespace std;
namespace Simpledb 
{
	/**
	* A RecordId is a reference to a specific tuple on a specific page of a
	* specific table.
	*/
	class RecordId {
	public:
		/**
		* Creates a new RecordId referring to the specified PageId and tuple
		* number.
		*
		* @param pid
		*            the pageid of the page on which the tuple resides
		* @param tupleno
		*            the tuple number within the page.
		*/
		RecordId(shared_ptr<PageId> pid, int tupleNo);
		/**
		* @return the tuple number this RecordId references.
		*/
		int getTupleNumber();
		/**
		* @return the page id this RecordId references.
		*/
		shared_ptr<PageId> getPageId();
		/**
		* Two RecordId objects are considered equal if they represent the same
		* tuple.
		*
		* @return True if this and o represent the same tuple
		*/
		bool equals(RecordId& rid);
		/**
		* You should implement the hashCode() so that two equal RecordId instances
		* (with respect to equals()) have the same hashCode().
		*
		* @return An int that is the same for equal RecordId objects.
		*/
		size_t hashCode();
	private:
		static long _serialVersionUID;
		shared_ptr<PageId> _pid;
		int _tupleNo;
		string _combineStr;
		size_t _hashcode;
	};
}