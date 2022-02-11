#pragma once
namespace Simpledb {
	template<typename T>
	class Iterator {
	public:
		Iterator() {}
		virtual ~Iterator() {}
		/*
		* return true if there is next element
		*/
		virtual bool hasNext() = 0;
		/*
		* return the pointer to next element,
		* return null if there is not next element
		*/
		virtual T* next() = 0;
		/*
		* removes from the underlying collection the last element returned by this iterator 
		*/
		virtual void remove() = 0;
	};
}