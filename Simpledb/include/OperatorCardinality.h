#pragma once
#include"Operator.h"
#include"TableStats.h"
#include"Filter.h"
#include"Join.h"
#include"HashEquiJoin.h"
#include"Aggregate.h"
namespace Simpledb {
	/**
	 * A utility class, which computes the estimated cardinalities of an operator
	 * tree.
	 *
	 * All methods have been fully provided. No extra codes are required.
	 */
	class OperatorCardinality {
	public:
		static bool updateOperatorCardinality(shared_ptr<Operator> o,
			map<string, size_t>& tableAliasToId,
			ConcurrentMap<string, shared_ptr<TableStats>>& tableStats);

		static bool updateFilterCardinality(shared_ptr<Filter> f,
			map<string, size_t>& tableAliasToId,
			ConcurrentMap<string, shared_ptr<TableStats>>& tableStats);

		static bool updateJoinCardinality(shared_ptr<Join> j,
			map<string, size_t>& tableAliasToId,
			ConcurrentMap<string, shared_ptr<TableStats>>& tableStats);

		static bool updateHashEquiJoinCardinality(shared_ptr<HashEquiJoin> j,
			map<string, size_t>& tableAliasToId,
			ConcurrentMap<string, shared_ptr<TableStats>>& tableStats);

		static bool updateAggregateCardinality(shared_ptr<Aggregate> a,
			map<string, size_t>& tableAliasToId,
			ConcurrentMap<string, shared_ptr<TableStats>>& tableStats);
	};
}