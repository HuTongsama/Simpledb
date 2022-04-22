#pragma once
#include"Predicate.h"
#include"Common.h"
namespace Simpledb {
	/** A LogicalJoinNode represens the state needed of a join of two
	* tables in a LogicalQueryPlan */
	class LogicalJoinNode {
	public:
		/** The first table to join (may be null). It's the alias of the table (if no alias, the true table name) */
		string t1Alias;
		/** The second table to join (may be null).  It's the alias of the table, (if no alias, the true table name).*/
		string t2Alias;
		/** The name of the field in t1 to join with. It's the pure name of a field, rather that alias.field. */
		string f1PureName;
		string f1QuantifiedName;
		/** The name of the field in t2 to join with. It's the pure name of a field.*/
		string f2PureName;
		string f2QuantifiedName;
		/** The join predicate */
		Predicate::Op p;

		LogicalJoinNode() :p(Predicate::Op::NOT_EQUALS) {};
		LogicalJoinNode(
			const string& table1,
			const string& table2,
			const string& joinField1,
			const string& joinField2,
			Predicate::Op pred) {
			t1Alias = table1;
			t2Alias = table2;
			vector<string> tmps = split(joinField1, "[.]");
			if (tmps.size() > 1)
				f1PureName = tmps[tmps.size() - 1];
			else
				f1PureName = joinField1;
			tmps = split(joinField2, "[.]");
			if (tmps.size() > 1)
				f2PureName = tmps[tmps.size() - 1];
			else
				f2PureName = joinField2;
			p = pred;
			f1QuantifiedName = t1Alias + "." + f1PureName;
			f2QuantifiedName = t2Alias + "." + f2PureName;
		}

		shared_ptr<LogicalJoinNode> swapInnerOuter() {
			Predicate::Op newp;
			if (p == Predicate::Op::GREATER_THAN)
				newp = Predicate::Op::LESS_THAN;
			else if (p == Predicate::Op::GREATER_THAN_OR_EQ)
				newp = Predicate::Op::LESS_THAN_OR_EQ;
			else if (p == Predicate::Op::LESS_THAN)
				newp = Predicate::Op::GREATER_THAN;
			else if (p == Predicate::Op::LESS_THAN_OR_EQ)
				newp = Predicate::Op::GREATER_THAN_OR_EQ;
			else
				newp = p;

			return make_shared<LogicalJoinNode>(t2Alias, t1Alias, f2PureName, f1PureName, newp);
		}
		bool equals(const LogicalJoinNode& o) {
			return (o.t1Alias == t1Alias || o.t1Alias == t2Alias) && (o.t2Alias == t1Alias || o.t2Alias == t2Alias);
		}
		string toString() {
			return t1Alias + ":" + t2Alias;//+ ";" + f1 + " " + p + " " + f2;
		}
		virtual size_t hashCode() {
			return hash<string>{}(t1Alias) + hash<string>{}(t2Alias) + hash<string>{}(f1PureName) + hash<string>{}(f2PureName);
		}
	};
}