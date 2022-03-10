#pragma once
#include"Iterator.h"
#include"Tuple.h"
namespace Simpledb {
	/**
	* DbFileIterator is the iterator interface that all SimpleDB Dbfile should
	* implement.
	*/
	class DbFileIterator : public Iterator<Tuple> {
	public:
		/**
		* Opens the iterator
		*/
		virtual void open() = 0;
		/**
		* Resets the iterator to the start.
		*/
		virtual void rewind() = 0;
		/**
		* Closes the iterator.
		*/
		virtual void close() = 0;
	};
}