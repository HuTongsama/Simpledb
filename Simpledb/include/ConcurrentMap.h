#pragma once
#include"Noncopyable.h"
#include<map>
#include<mutex>
using namespace std;
namespace Simpledb {
	template<typename keyType,typename valueType>
	class ConcurrentMap : public Noncopyable {
	public:
		void setValue(const keyType& key, valueType& val);
		//return nullptr if not find
		valueType* getValue(const keyType& key);
		void reset(ConcurrentMap<keyType, valueType>& m);

	private:
		map<keyType, valueType> _map;
		mutex _mutex;
	};
	template<typename keyType, typename valueType>
	inline void ConcurrentMap<keyType, valueType>::setValue(const keyType& key, valueType& val)
	{
		lock_guard<mutex> guard(_mutex);
		_map[key] = val;
	}
	template<typename keyType, typename valueType>
	inline valueType* ConcurrentMap<keyType, valueType>::getValue(const keyType& key)
	{
		lock_guard<mutex> guard(_mutex);
		if (_map.find(key) != _map.end()) {
			valueType& val = _map[key];
			return &val;
		}
		return nullptr;
	}
	template<typename keyType, typename valueType>
	inline void ConcurrentMap<keyType, valueType>::reset(ConcurrentMap<keyType, valueType>& m)
	{
		lock_guard<mutex> guard(_mutex);
		_map.clear();
		for (auto iter = m._map.begin(); iter != m._map.end(); ++iter) {
			_map[iter->first] = iter->second;
		}
	}
}