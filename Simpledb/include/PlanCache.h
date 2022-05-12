#pragma once
#include<map>
#include<vector>
#include<set>
#include"LogicalJoinNode.h"
namespace Simpledb {
	class PlanCache {
	public:
        /** Add a new cost, cardinality and ordering for a particular join set.  Does not verify that the
        new cost is less than any previously added cost -- simply adds or replaces an existing plan for the
        specified join set
        @param s the set of joins for which a new ordering (plan) is being added
        @param cost the estimated cost of the specified plan
        @param card the estimatied cardinality of the specified plan
        @param order the ordering of the joins in the plan
    */
        void addPlan(const set<shared_ptr<LogicalJoinNode>>& s, double cost, int card, vector<shared_ptr<LogicalJoinNode>>& order) {
            _bestOrders.emplace(s, order);
            _bestCosts.emplace(s, cost);
            _bestCardinalities.emplace(s, card);
        }

        /** Find the best join order in the cache for the specified plan
            @param s the set of joins to look up the best order for
            @return the best order for s in the cache
        */
        vector<shared_ptr<LogicalJoinNode>> getOrder(const set<shared_ptr<LogicalJoinNode>>& s) {
            if (_bestOrders.find(s) != _bestOrders.end())
                return _bestOrders.at(s);
            return vector<shared_ptr<LogicalJoinNode>>();
        }

        /** Find the cost of the best join order in the cache for the specified plan
            @param s the set of joins to look up the best cost for
            @return the cost of the best order for s in the cache
        */
        double getCost(set<shared_ptr<LogicalJoinNode>>& s) {
            return _bestCosts.at(s);
        }

        /** Find the cardinality of the best join order in the cache for the specified plan
            @param s the set of joins to look up the best cardinality for
            @return the cardinality of the best order for s in the cache
        */
        int getCard(set<shared_ptr<LogicalJoinNode>>& s) {
            return _bestCardinalities.at(s);
        }

	private:
		map<set<shared_ptr<LogicalJoinNode>>, vector<shared_ptr<LogicalJoinNode>>> _bestOrders;
		map<set<shared_ptr<LogicalJoinNode>>, double> _bestCosts;
		map<set<shared_ptr<LogicalJoinNode>>, int> _bestCardinalities;
	};
}