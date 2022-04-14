#pragma once
#include"Predicate.h"
#include<vector>
#include<map>
namespace Simpledb {
	/** A class to represent a fixed-width histogram over a single integer-based field.
	*/
	class IntHistogram {
	public:
		IntHistogram() {}
        /**
		* Create a new IntHistogram.
		*
		* This IntHistogram should maintain a histogram of integer values that it receives.
		* It should split the histogram into "buckets" buckets.
		*
		* The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
		*
		* Your implementation should use space and have execution time that are both
		* constant with respect to the number of values being histogrammed.  For example, you shouldn't
		* simply store every value that you see in a sorted list.
		*
		* @param buckets The number of buckets to split the input value into.
		* @param min The minimum integer value that will ever be passed to this class for histogramming
		* @param max The maximum integer value that will ever be passed to this class for histogramming
		*/
		IntHistogram(int buckets, int min, int max);
		/**
		* Add a value to the set of values that you are keeping a histogram of.
		* @param v Value to add to the histogram
		*/
		void addValue(int v);
		/**
		* Estimate the selectivity of a particular predicate and operand on this table.
		*
		* For example, if "op" is "GREATER_THAN" and "v" is 5,
		* return your estimate of the fraction of elements that are greater than 5.
		*
		* @param op Operator
		* @param v Value
		* @return Predicted selectivity of this particular operator and value
		*/
		double estimateSelectivity(Predicate::Op op, int v);
		/**
		* @return
		*     the average selectivity of this histogram.
		*
		*     This is not an indispensable method to implement the basic
		*     join optimization. It may be needed if you want to
		*     implement a more efficient optimization
		* */
		double avgSelectivity();
		/**
		* @return A string describing this histogram, for debugging purposes
		*/
		string toString();

	private:
		int getBucketIndex(int v);
		int getTupleCount();


		vector<int> _buckets;
		int _interval;
		int _min;
		int _max;
		int _bucketCount;
	};
}