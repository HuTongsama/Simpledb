#pragma once
#include<vector>
#include"LogicalJoinNode.h"
namespace Simpledb {
	/** Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan} specifying the
	cost and cardinality of the optimal plan represented by plan.
	*/
	class CostCard {
	public:
		/** The cost of the optimal subplan */
		double cost;
		/** The cardinality of the optimal subplan */
		int card;
		/** The optimal subplan */
		vector<shared_ptr<LogicalJoinNode>> plan;
	};
}