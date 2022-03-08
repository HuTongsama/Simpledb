#pragma once
namespace Simpledb {
	template<typename T>
	class Iterator {
	public:
		Iterator() :_position(0) {}
		virtual ~Iterator() {}
		/*
		* return true if there is next element
		*/
		virtual bool hasNext() = 0;
		/*
		* return the pointer to next element,
		* throw exception if there is not next element
		*/
		virtual T& next() = 0;;
	protected:
		size_t _position;
	};
}