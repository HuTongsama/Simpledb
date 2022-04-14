#pragma once
#include"IntHistogram.h"
namespace Simpledb {
	class StringHistogram {
	public:
		StringHistogram() {}
        /**
		* Create a new StringHistogram with a specified number of buckets.
		* <p>
		* Our implementation is written in terms of an IntHistogram by converting
		* each String to an integer.
		*
		* @param buckets
		*            the number of buckets
		*/
		StringHistogram(int buckets);
		/** Add a new value to thte histogram */
		void addValue(const string& s);
		/**
		* Estimate the selectivity (as a double between 0 and 1) of the specified
		* predicate over the specified string
		*
		* @param op
		*            The operation being applied
		* @param s
		*            The string to apply op to
		*/
		double estimateSelectivity(Predicate::Op op, const string& s);
		/**
		* @return the average selectivity of this histogram.
		*
		*         This is not an indispensable method to implement the basic join
		*         optimization. It may be needed if you want to implement a more
		*         efficient optimization
		* */
		double avgSelectivity();
	private:
		int stringToInt(const string& s);
		/** @return the maximum value indexed by the histogram */
		int maxVal() {
			return stringToInt("zzzz");
		}
		/** @return the minimum value indexed by the histogram */
		int minVal() {
			return stringToInt("");
		}



		IntHistogram _intHistogram;
	};
}