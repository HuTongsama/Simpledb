#pragma once
#include"LogicalPlan.h"
#include"CostCard.h"
#include"PlanCache.h"
#include"ConcurrentMap.h"
#include<set>
namespace Simpledb {
	class JoinOptimizer {
	public:
		/**
		 * Constructor
		 *
		 * @param p
		 *            the logical plan being optimized
		 * @param joins
		 *            the vector of joins being performed
		 */
		JoinOptimizer(shared_ptr<LogicalPlan> p, vector<shared_ptr<LogicalJoinNode>>& joins)
			:_p(p), _joins(joins) {}
		/**
		 * Return best iterator for computing a given logical join, given the
		 * specified statistics, and the provided left and right subplans. Note that
		 * there is insufficient information to determine which plan should be the
		 * inner/outer here -- because OpIterator's don't provide any cardinality
		 * estimates, and stats only has information about the base tables. For this
		 * reason, the plan1
		 *
		 * @param lj
		 *            The join being considered
		 * @param plan1
		 *            The left join node's child
		 * @param plan2
		 *            The right join node's child
		 */
		static shared_ptr<OpIterator> instantiateJoin(shared_ptr<LogicalJoinNode> lj,
			shared_ptr<OpIterator> plan1, shared_ptr<OpIterator> plan2);
		/**
		 * Estimate the cost of a join.
		 *
		 * The cost of the join should be calculated based on the join algorithm (or
		 * algorithms) that you implemented for Lab 2. It should be a function of
		 * the amount of data that must be read over the course of the query, as
		 * well as the number of CPU opertions performed by your join. Assume that
		 * the cost of a single predicate application is roughly 1.
		 *
		 *
		 * @param j
		 *            A LogicalJoinNode representing the join operation being
		 *            performed.
		 * @param card1
		 *            Estimated cardinality of the left-hand side of the query
		 * @param card2
		 *            Estimated cardinality of the right-hand side of the query
		 * @param cost1
		 *            Estimated cost of one full scan of the table on the left-hand
		 *            side of the query
		 * @param cost2
		 *            Estimated cost of one full scan of the table on the right-hand
		 *            side of the query
		 * @return An estimate of the cost of this query, in terms of cost1 and
		 *         cost2
		 */
		double estimateJoinCost(shared_ptr<LogicalJoinNode> j, int card1, int card2,
			double cost1, double cost2);
		/**
		 * Estimate the cardinality of a join. The cardinality of a join is the
		 * number of tuples produced by the join.
		 *
		 * @param j
		 *            A LogicalJoinNode representing the join operation being
		 *            performed.
		 * @param card1
		 *            Cardinality of the left-hand table in the join
		 * @param card2
		 *            Cardinality of the right-hand table in the join
		 * @param t1pkey
		 *            Is the left-hand table a primary-key table?
		 * @param t2pkey
		 *            Is the right-hand table a primary-key table?
		 * @param stats
		 *            The table stats, referenced by table names, not alias
		 * @return The cardinality of the join
		 */
		int estimateJoinCardinality(shared_ptr<LogicalJoinNode> j, int card1, int card2,
			bool t1pkey, bool t2pkey, ConcurrentMap<string, shared_ptr<TableStats>>& stats);
		/**
		 * Estimate the join cardinality of two tables.
		 * */
		static int estimateTableJoinCardinality(Predicate::Op joinOp,
			const string& table1Alias, const string& table2Alias, const string& field1PureName,
			const string& field2PureName, int card1, int card2, bool t1pkey,
			bool t2pkey, ConcurrentMap<string, shared_ptr<TableStats>>& stats,
			map<string, size_t>& tableAliasToId);
		/**
		 * Helper method to enumerate all of the subsets of a given size of a
		 * specified vector.
		 *
		 * @param v
		 *            The vector whose subsets are desired
		 * @param size
		 *            The size of the subsets of interest
		 * @return a set of all subsets of the specified size
		 */
		template<typename T>
		set<set<T>> enumerateSubsets(vector<T>& v, int size);
		/**
		 * Compute a logical, reasonably efficient join on the specified tables. See
		 * PS4 for hints on how this should be implemented.
		 *
		 * @param stats
		 *            Statistics for each table involved in the join, referenced by
		 *            base table names, not alias
		 * @param filterSelectivities
		 *            Selectivities of the filter predicates on each table in the
		 *            join, referenced by table alias (if no alias, the base table
		 *            name)
		 * @param explain
		 *            Indicates whether your code should explain its query plan or
		 *            simply execute it
		 * @return A List<LogicalJoinNode> that stores joins in the left-deep
		 *         order in which they should be executed.
		 * @throws ParsingException
		 *             when stats or filter selectivities is missing a table in the
		 *             join, or or when another internal error occurs
		 */
		vector<shared_ptr<LogicalJoinNode>> orderJoins(map<string, shared_ptr<TableStats>>& stats,
			map<string, double>& filterSelectivities, bool explain);
	private:
		/**
		 * This is a helper method that computes the cost and cardinality of joining
		 * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
		 * all of the subsets of size joinSet.size() - 1 have already been computed
		 * and stored in PlanCache pc.
		 *
		 * @param stats
		 *            table stats for all of the tables, referenced by table names
		 *            rather than alias (see {@link #orderJoins})
		 * @param filterSelectivities
		 *            the selectivities of the filters over each of the tables
		 *            (where tables are indentified by their alias or name if no
		 *            alias is given)
		 * @param joinToRemove
		 *            the join to remove from joinSet
		 * @param joinSet
		 *            the set of joins being considered
		 * @param bestCostSoFar
		 *            the best way to join joinSet so far (minimum of previous
		 *            invocations of computeCostAndCardOfSubplan for this joinSet,
		 *            from returned CostCard)
		 * @param pc
		 *            the PlanCache for this join; should have subplans for all
		 *            plans of size joinSet.size()-1
		 * @return A {@link CostCard} objects desribing the cost, cardinality,
		 *         optimal subplan
		 * @throws ParsingException
		 *             when stats, filterSelectivities, or pc object is missing
		 *             tables involved in join
		 */
		shared_ptr<CostCard> computeCostAndCardOfSubplan(
			ConcurrentMap<string, shared_ptr<TableStats>>& stats,
			map<string, double>& filterSelectivities,
			shared_ptr<LogicalJoinNode> joinToRemove, set<shared_ptr<LogicalJoinNode>>& joinSet,
			double bestCostSoFar, shared_ptr<PlanCache> pc);
		/**
		 * Return true if the specified table is in the list of joins, false
		 * otherwise
		 */
		bool doesJoin(vector<shared_ptr<LogicalJoinNode>>& joinlist, const string& table);
		/**
		 * Return true if field is a primary key of the specified table, false
		 * otherwise
		 *
		 * @param tableAlias
		 *            The alias of the table in the query
		 * @param field
		 *            The pure name of the field
		 */
		bool isPkey(const string& tableAlias, const string& field);
		/**
		 * Return true if a primary key field is joined by one of the joins in
		 * joinlist
		 */
		bool hasPkey(vector<shared_ptr<LogicalJoinNode>>& joinlist);
		/**
		 * Helper function to display a Swing window with a tree representation of
		 * the specified list of joins. See {@link #orderJoins}, which may want to
		 * call this when the analyze flag is true.
		 *
		 * @param js
		 *            the join plan to visualize
		 * @param pc
		 *            the PlanCache accumulated whild building the optimal plan
		 * @param stats
		 *            table statistics for base tables
		 * @param selectivities
		 *            the selectivities of the filters over each of the tables
		 *            (where tables are indentified by their alias or name if no
		 *            alias is given)
		 */
		void printJoins(vector<shared_ptr<LogicalJoinNode>>& js, shared_ptr<PlanCache> pc,
			map<string, shared_ptr<TableStats>>& stats,
			map<string, double>& selectivities) {}

		shared_ptr<LogicalPlan> _p;
		vector<shared_ptr<LogicalJoinNode>> _joins;
	};
	template<typename T>
	inline set<set<T>> JoinOptimizer::enumerateSubsets(vector<T>& v, int size)
	{
		set<set<T>> els;
		els.emplace(set<T>());
		// Iterator<Set> it;
		// long start = System.currentTimeMillis();

		for (int i = 0; i < size; i++) {
			set<set<T>> newels;
			for (set<T>& s : els) {
				for (T& t : v) {
					set<T> news;
					if (news.emplace(t).second)
						newels.emplace(news);
				}
			}
			els = newels;
		}
		return els;
		return set<set<T>>();
	}
}