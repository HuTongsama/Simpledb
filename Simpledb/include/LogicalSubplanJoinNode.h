#pragma once
#include"LogicalJoinNode.h"
#include"OpIterator.h"
namespace Simpledb {
	class LogicalSubplanJoinNode : public LogicalJoinNode {
	public:
        LogicalSubplanJoinNode(const string& table1,const string& joinField1, shared_ptr<OpIterator> sp, Predicate::Op pred) {
            t1Alias = table1;
            vector<string> tmps = split(joinField1, "[.]");
            if (tmps.size() > 1)
                f1PureName = tmps[tmps.size() - 1];
            else
                f1PureName = joinField1;
            f1QuantifiedName = t1Alias + "." + f1PureName;
            _subPlan = sp;
            p = pred;
        }

        size_t hashCode()override {
            return hash<string>{}(t1Alias) + hash<string>{}(f1PureName) + hash<shared_ptr<OpIterator>>{}(_subPlan);
        }
        bool equals(const LogicalSubplanJoinNode& o) {
            return (o.t1Alias == t1Alias && o.f1PureName == f1PureName && (o._subPlan == _subPlan));
        }
	private:
		shared_ptr<OpIterator> _subPlan;
	};
}