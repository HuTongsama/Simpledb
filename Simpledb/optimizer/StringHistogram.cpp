#include"StringHistogram.h"
namespace Simpledb {
	StringHistogram::StringHistogram(int buckets)
		:_intHistogram(buckets, minVal(), maxVal())
	{
	}
	void StringHistogram::addValue(const string& s)
	{
		int val = stringToInt(s);
		_intHistogram.addValue(val);
	}
	double StringHistogram::estimateSelectivity(Predicate::Op op, const string& s)
	{
		int val = stringToInt(s);
		return _intHistogram.estimateSelectivity(op, val);
	}
	double StringHistogram::avgSelectivity()
	{
		return _intHistogram.avgSelectivity();
	}
	int StringHistogram::stringToInt(const string& s)
	{
		int i;
		int v = 0;
		for (i = 3; i >= 0; i--) {
			if (s.length() > 3 - i) {
				int ci = s.at(3 - i);
				v += (ci) << (i * 8);
			}
		}

		// XXX: hack to avoid getting wrong results for
		// strings which don't output in the range min to max
		if (!(s == "" || s == "zzzz")) {
			if (v < minVal()) {
				v = minVal();
			}

			if (v > maxVal()) {
				v = maxVal();
			}
		}

		return v;
	}
}