#pragma once
#include"OpIterator.h"
#include"TupleIterator.h"
#include<map>
namespace Simpledb {
	/**
	* The common interface for any class that can compute an aggregate over a
	* list of Tuples.
	*/
	class Aggregator {
	public:
        static const int NO_GROUPING = -1;
        /**
         * SUM_COUNT and SC_AVG will
         * only be used in lab7, you are not required
         * to implement them until then.
         * */
        enum class Op {
            MIN, MAX, SUM, AVG, COUNT,
            /**
             * SUM_COUNT: compute sum and count simultaneously, will be
             * needed to compute distributed avg in lab7.
             * */
             SUM_COUNT,
             /**
              * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
              * will be used to compute distributed avg in lab7.
              * */
              SC_AVG
        };
        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        static Aggregator::Op getOp(const string& s) {
            int i = stoi(s);
            return getOp(i);
        }
        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        static Aggregator::Op getOp(int i) {
            return (Op)i;
        }
        static string opToString(Aggregator::Op op)
        {
            switch (op)
            {
            case Simpledb::Aggregator::Op::MIN:
                return "min";
            case Simpledb::Aggregator::Op::MAX:
                return "max";
            case Simpledb::Aggregator::Op::SUM:
                return "sum";
            case Simpledb::Aggregator::Op::AVG:
                return "avg";
            case Simpledb::Aggregator::Op::COUNT:
                return "count";
            case Simpledb::Aggregator::Op::SUM_COUNT:
                return "sum_count";
            case Simpledb::Aggregator::Op::SC_AVG:
                return "sc_avg";
            default:
                throw runtime_error("impossible to reach here");

            }
        }
        
        Aggregator(int gbfield, shared_ptr<Type> gbfieldtype, int afield, Aggregator::Op what)
            :_gbfield(gbfield), _gbfieldtype(gbfieldtype), _afield(afield), _op(what) {}

        virtual ~Aggregator() {}
        /**
         * Merge a new tuple into the aggregate for a distinct group value;
         * creates a new group aggregate result if the group value has not yet
         * been encountered.
         *
         * @param tup the Tuple containing an aggregate field and a group-by field
         */
        virtual void mergeTupleIntoGroup(Tuple& tup) = 0;
        /**
         * Create a OpIterator over group aggregate results.
         * @see TupleIterator for a possible helper
         */
        virtual shared_ptr<OpIterator> iterator() = 0;
    protected:
        shared_ptr<Tuple> getAimTuple(Tuple& tup);
        int getModifyfield() {
            int field = _gbfield == Aggregator::NO_GROUPING ? 0 : 1;
            return field;
        }



        int _gbfield;
        shared_ptr<Type> _gbfieldtype;
        int _afield;
        Aggregator::Op _op;
        vector<shared_ptr<Tuple>> _tuples;
        map<int64_t, int64_t> _groupToCount;
        shared_ptr<TupleDesc> _td;
	};
}