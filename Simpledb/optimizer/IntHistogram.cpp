#include"IntHistogram.h"

namespace Simpledb {
	IntHistogram::IntHistogram(int buckets, int min, int max)
		:_min(0), _max(0), _bucketCount(0), _interval(0)
	{
		if (buckets == 0)
			return;
		_buckets.resize(buckets, 0);
		_min = min;
		_max = max;
		_bucketCount = buckets;
		_interval = (max - min) / buckets;
		if (_interval == 0)
			_interval = 1;
	}
	void IntHistogram::addValue(int v)
	{
		if (_interval == 0)
			return;
		int id = getBucketIndex(v);
		_buckets[id] += 1;
	}
	double IntHistogram::estimateSelectivity(Predicate::Op op, int v)
	{
		int tupleCount = getTupleCount();
		int bid = getBucketIndex(v);
		if (bid == -1)
		{
			switch (op)
			{
			case Simpledb::Predicate::Op::LIKE:
			case Simpledb::Predicate::Op::EQUALS:
				return 0;		
			case Simpledb::Predicate::Op::GREATER_THAN:
			case Simpledb::Predicate::Op::GREATER_THAN_OR_EQ:
				if (v > _max)
					return 0;
				else
					return 1;
			case Simpledb::Predicate::Op::LESS_THAN:
			case Simpledb::Predicate::Op::LESS_THAN_OR_EQ:
				if (v > _max)
					return 1;
				else
					return 0;
			case Simpledb::Predicate::Op::NOT_EQUALS:
				return 1;
			default:
				return 0;
			}
		}
		int left = _min + bid * _interval;
		int right = left + _interval - 1;//[left,right]
		if (bid == _bucketCount - 1) {
			right++;
		}
		int distance = right - left + 1;
		if (_interval == 0 || _bucketCount == 0 
			|| tupleCount == 0 || distance == 0)
			return 0.0;

		double count = 0;	
		switch (op)
		{
		case Simpledb::Predicate::Op::LIKE:
		case Simpledb::Predicate::Op::EQUALS:
			count = (double)_buckets[bid] / distance;
			break;
		case Simpledb::Predicate::Op::GREATER_THAN: 
		{
			double ratio = (right - v) / distance;
			count = _buckets[bid] * ratio;
			for (int id = bid + 1; id < _bucketCount; ++id) {
				count += _buckets[id];
			}
		}
			break;
		case Simpledb::Predicate::Op::LESS_THAN:
		{
			double ratio = (v - left) / distance;
			count = _buckets[bid] * ratio;
			for (int id = bid - 1; id >= 0; --id) {
				count += _buckets[id];
			}	
		}
			break;
		case Simpledb::Predicate::Op::LESS_THAN_OR_EQ:
		{
			double ratio = (v - left + 1) / distance;
			count = _buckets[bid] * ratio;
			for (int id = bid - 1; id >= 0; --id) {
				count += _buckets[id];
			}
		}
			break;
		case Simpledb::Predicate::Op::GREATER_THAN_OR_EQ:
		{
			double ratio = (right - v + 1) / distance;
			count = _buckets[bid] * ratio;
			for (int id = bid + 1; id < _bucketCount; ++id) {
				count += _buckets[id];
			}
		}
			break;
		case Simpledb::Predicate::Op::NOT_EQUALS:
			count = (double)_buckets[bid] / distance;
			count = (double)tupleCount - count;
			break;
		default:
			break;
		}
		double result = count / tupleCount;
		return result;
	}
	double IntHistogram::avgSelectivity()
	{
		return 0.0;
	}
	string IntHistogram::toString()
	{
		return string();
	}
	int IntHistogram::getBucketIndex(int v)
	{
		if (_interval == 0 || v < _min || v > _max)
			return -1;
		int id = (v - _min) / _interval;
		if (id == _bucketCount)
			id -= 1;
		return id;
	}
	int IntHistogram::getTupleCount()
	{
		int count = 0;
		for (auto c : _buckets) {
			count += c;
		}
		return count;
	}
}