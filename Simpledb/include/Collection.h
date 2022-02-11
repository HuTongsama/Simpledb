#pragma once
#include"Iterator.h"
namespace Simpledb {

	template<typename T>
	class Collection {
	public:
		Collection() {}
		virtual ~Collection() {}
		/*
		* ensures that this collection contains the specified element
		*/
		virtual bool add(const T& element) = 0;
		/*
		* Removes all of the elements from this collection
		*/
		virtual void clear() = 0;
		/*
		* returns true if this collection contains the specified element
		*/
		virtual bool contains(const T& element) = 0;
		virtual size_t hashCode() = 0;
		virtual bool isEmpty() = 0;
		/*
		* returns an iterator over the elements in this collection
		*/
		virtual const Iterator<T>& iterator() = 0;
		/*
		* removes a single instance of the specified element from this collection, if it is present
		*/
		virtual bool remove(const T& element) = 0;
		/*
		* removes all of the elements of this collection that satisfy the given predicate
		*/
		virtual bool removeIf(bool(*predicate)(const T&)) = 0;
		virtual size_t size() = 0;
	};
}