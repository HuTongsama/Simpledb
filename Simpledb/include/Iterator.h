#pragma once
#include"Noncopyable.h"
#include<vector>
#include<stdexcept>
using namespace std;
namespace Simpledb {
	template<typename T>
	class Iterator : public Noncopyable {
	public:
		Iterator() :_position(0) {}
		Iterator(const vector<T>& items) :_items(items), _position(0) {}
		virtual ~Iterator() {}
		/*
		* return true if there is next element
		*/
		virtual bool hasNext() {
			if (_items.size() > _position)
				return true;
			return false;
		}
		/*
		* return the pointer to next element,
		* throw exception if there is not next element
		*/
		virtual T& next() {
			if (_position >= _items.size())
				throw runtime_error("no such element");
			T& t = _items[_position];
			_position++;
			return t;
		}
	protected:
		size_t _position;
		vector<T> _items;
	};
}