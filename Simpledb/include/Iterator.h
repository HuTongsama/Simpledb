#pragma once
#include"Noncopyable.h"
#include<vector>
#include<memory>
#include<stdexcept>
using namespace std;
namespace Simpledb {
	template<typename T>
	class Iterator : public Noncopyable {
	public:
		Iterator() :_position(0) {}
		Iterator(const vector<shared_ptr<T>>& items) :_items(items), _position(0) {}
		Iterator(const vector<T>& items) :_position(0) {
			for (auto& item : items) {
				_items.push_back(make_shared<T>(item));
			}
		}
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
		virtual T* next() {
			if (_position >= _items.size())
				throw runtime_error("no such element");
			shared_ptr<T>& t = _items[_position];
			_position++;
			return t.get();
		}
	protected:
		size_t _position;
		vector<shared_ptr<T>> _items;
	};
}