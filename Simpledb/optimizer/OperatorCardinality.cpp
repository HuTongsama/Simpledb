#include"OperatorCardinality.h"
#include"SeqScan.h"
#include"Common.h"
#include"Database.h"
#include"JoinOptimizer.h"
namespace Simpledb {
	bool OperatorCardinality::updateOperatorCardinality(shared_ptr<Operator> o,
		map<string, size_t>& tableAliasToId, ConcurrentMap<string, shared_ptr<TableStats>>& tableStats)
	{
        
        if (dynamic_pointer_cast<Filter>(o)) {
            return updateFilterCardinality(dynamic_pointer_cast<Filter>(o), tableAliasToId,
                tableStats);
        }
        else if (dynamic_pointer_cast<Join>(o)) {
            return updateJoinCardinality(dynamic_pointer_cast<Join>(o), tableAliasToId, tableStats);
        }
        else if (dynamic_pointer_cast<HashEquiJoin>(o)) {
            return updateHashEquiJoinCardinality(dynamic_pointer_cast<HashEquiJoin>(o),
                tableAliasToId, tableStats);
        }
        else if (dynamic_pointer_cast<Aggregate>(o)) {
            return updateAggregateCardinality(dynamic_pointer_cast<Aggregate>(o), tableAliasToId,
                tableStats);
        }
        else {
            vector<shared_ptr<OpIterator>> children = o->getChildren();
            int childC = 1;
            bool hasJoinPK = false;
            if (children.size() > 0 && children[0] != nullptr) {
                if (dynamic_pointer_cast<Operator>(children[0])) {
                    shared_ptr<Operator> c = dynamic_pointer_cast<Operator>(children[0]);
                    hasJoinPK = updateOperatorCardinality(
                        c, tableAliasToId, tableStats);
                    childC = c->getEstimatedCardinality();
                }
                else if (dynamic_pointer_cast<SeqScan>(children[0])) {
                    childC = (*tableStats.getValue(
                        (dynamic_pointer_cast<SeqScan>(children[0]))->getTableName()))->estimateTableCardinality(1.0);
                }
            }
            o->setEstimatedCardinality(childC);
            return hasJoinPK;
        }
	}
	
    bool OperatorCardinality::updateFilterCardinality(shared_ptr<Filter> f, 
		map<string, size_t>& tableAliasToId, ConcurrentMap<string, shared_ptr<TableStats>>& tableStats)
	{
        shared_ptr<OpIterator> child = f->getChildren()[0];
        shared_ptr<Predicate> pred = f->getPredicate();
        vector<string> tmp = split(child->getTupleDesc()->getFieldName(pred->getField()), string("[.]"));
        string tableAlias = tmp[0];
        string pureFieldName = tmp[1];
        
        double selectivity = 1.0;
        if (tableAliasToId.find(tableAlias) != tableAliasToId.end()) {
            size_t tableId = tableAliasToId[tableAlias];
            selectivity = (*tableStats.getValue(
                Database::getCatalog()->getTableName(tableId)))
                ->estimateSelectivity(
                    Database::getCatalog()->getTupleDesc(tableId)
                    ->fieldNameToIndex(pureFieldName),
                    pred->getOp(), pred->getOperand());
            if (dynamic_pointer_cast<Operator>(child)) {
                shared_ptr<Operator> oChild = dynamic_pointer_cast<Operator>(child);
                bool hasJoinPK = updateOperatorCardinality(oChild,
                    tableAliasToId, tableStats);
                f->setEstimatedCardinality((int)(oChild
                    ->getEstimatedCardinality() * selectivity) + 1);
                return hasJoinPK;
            }
            else if (dynamic_pointer_cast<SeqScan>(child)) {
                f->setEstimatedCardinality((int)((*tableStats.getValue(
                    (dynamic_pointer_cast<SeqScan>(child))->getTableName()))
                    ->estimateTableCardinality(1.0) * selectivity) + 1);
                return false;
            }
        }
        f->setEstimatedCardinality(1);
        return false;
	}
	
    bool OperatorCardinality::updateJoinCardinality(shared_ptr<Join> j, 
		map<string, size_t>& tableAliasToId, ConcurrentMap<string, shared_ptr<TableStats>>& tableStats)
	{
        vector<shared_ptr<OpIterator>> children = j->getChildren();
        shared_ptr<OpIterator> child1 = children[0];
        shared_ptr<OpIterator> child2 = children[1];
        int child1Card = 1;
        int child2Card = 1;

        vector<string> tmp1 = split(j->getJoinField1Name(), string("[.]"));
        string tableAlias1 = tmp1[0];
        string pureFieldName1 = tmp1[1];

        vector<string> tmp2 = split(j->getJoinField2Name(), string("[.]"));
        string tableAlias2 = tmp2[0];
        string pureFieldName2 = tmp2[1];

        bool child1HasJoinPK = Database::getCatalog()
            ->getPrimaryKey(tableAliasToId[tableAlias1]) == pureFieldName1;
        bool child2HasJoinPK = Database::getCatalog()
            ->getPrimaryKey(tableAliasToId[tableAlias2]) == pureFieldName2;

        if (dynamic_pointer_cast<Operator>(child1)) {
            shared_ptr<Operator> child1O = dynamic_pointer_cast<Operator>(child1);
            bool pk = updateOperatorCardinality(child1O, tableAliasToId,
                tableStats);
            child1HasJoinPK = pk || child1HasJoinPK;
            child1Card = child1O->getEstimatedCardinality();
            child1Card = child1Card > 0 ? child1Card : 1;
        }
        else if (dynamic_pointer_cast<SeqScan>(child1)) {
            child1Card = (*tableStats.getValue((dynamic_pointer_cast<SeqScan>(child1))
                ->getTableName()))->estimateTableCardinality(1.0);
        }

        if (dynamic_pointer_cast<Operator>(child2)) {
            shared_ptr<Operator> child2O = dynamic_pointer_cast<Operator>(child2);
            bool pk = updateOperatorCardinality(child2O, tableAliasToId,
                tableStats);
            child2HasJoinPK = pk || child2HasJoinPK;
            child2Card = child2O->getEstimatedCardinality();
            child2Card = child2Card > 0 ? child2Card : 1;
        }
        else if (dynamic_pointer_cast<SeqScan>(child2)) {
            child2Card = (*tableStats.getValue((dynamic_pointer_cast<SeqScan>(child2))
                ->getTableName()))->estimateTableCardinality(1.0);
        }

        j->setEstimatedCardinality(JoinOptimizer::estimateTableJoinCardinality(j
            ->getJoinPredicate()->getOperator(), tableAlias1, tableAlias2,
            pureFieldName1, pureFieldName2, child1Card, child2Card,
            child1HasJoinPK, child2HasJoinPK, tableStats, tableAliasToId));
        return child1HasJoinPK || child2HasJoinPK;
	}
	
    bool OperatorCardinality::updateHashEquiJoinCardinality(shared_ptr<HashEquiJoin> j,
		map<string, size_t>& tableAliasToId, ConcurrentMap<string, shared_ptr<TableStats>>& tableStats)
	{
        vector<shared_ptr<OpIterator>> children = j->getChildren();
        shared_ptr<OpIterator> child1 = children[0];
        shared_ptr<OpIterator> child2 = children[1];
        int child1Card = 1;
        int child2Card = 1;

        vector<string> tmp1 = split(j->getJoinField1Name(), "[.]");
        string tableAlias1 = tmp1[0];
        string pureFieldName1 = tmp1[1];
        vector<string> tmp2 = split(j->getJoinField2Name(), "[.]");
        string tableAlias2 = tmp2[0];
        string pureFieldName2 = tmp2[1];

        bool child1HasJoinPK = Database::getCatalog()
            ->getPrimaryKey(tableAliasToId[tableAlias1]) == pureFieldName1;
        bool child2HasJoinPK = Database::getCatalog()
            ->getPrimaryKey(tableAliasToId[tableAlias2]) == pureFieldName2;

        if (dynamic_pointer_cast<Operator>(child1)) {
            shared_ptr<Operator> child1O = dynamic_pointer_cast<Operator>(child1);
            bool pk = updateOperatorCardinality(child1O, tableAliasToId,
                tableStats);
            child1HasJoinPK = pk || child1HasJoinPK;
            child1Card = child1O->getEstimatedCardinality();
            child1Card = child1Card > 0 ? child1Card : 1;
        }
        else if (dynamic_pointer_cast<SeqScan>(child1)) {
            child1Card = (*tableStats.getValue((dynamic_pointer_cast<SeqScan>(child1))
                ->getTableName()))->estimateTableCardinality(1.0);
        }

        if (dynamic_pointer_cast<Operator>(child2)) {
            shared_ptr<Operator> child2O = dynamic_pointer_cast<Operator>(child2);
            bool pk = updateOperatorCardinality(child2O, tableAliasToId,
                tableStats);
            child2HasJoinPK = pk || child2HasJoinPK;
            child2Card = child2O->getEstimatedCardinality();
            child2Card = child2Card > 0 ? child2Card : 1;
        }
        else if (dynamic_pointer_cast<SeqScan>(child2)) {
            child2Card = (*tableStats.getValue((dynamic_pointer_cast<SeqScan>(child2))
                ->getTableName()))->estimateTableCardinality(1.0);
        }

        j->setEstimatedCardinality(JoinOptimizer::estimateTableJoinCardinality(j
            ->getJoinPredicate()->getOperator(), tableAlias1, tableAlias2,
            pureFieldName1, pureFieldName2, child1Card, child2Card,
            child1HasJoinPK, child2HasJoinPK, tableStats, tableAliasToId));
        return child1HasJoinPK || child2HasJoinPK;
	}
	
    bool OperatorCardinality::updateAggregateCardinality(shared_ptr<Aggregate> a, 
		map<string, size_t>& tableAliasToId, ConcurrentMap<string, shared_ptr<TableStats>>& tableStats)
	{
        shared_ptr<OpIterator> child = a->getChildren()[0];
        int childCard = 1;
        bool hasJoinPK = false;
        if (dynamic_pointer_cast<Operator>(child)) {
            shared_ptr<Operator> oChild = dynamic_pointer_cast<Operator>(child);
            hasJoinPK = updateOperatorCardinality(oChild, tableAliasToId,
                tableStats);
            childCard = oChild->getEstimatedCardinality();
        }

        if (a->groupField() == Aggregator::NO_GROUPING) {
            a->setEstimatedCardinality(1);
            return hasJoinPK;
        }

        if (dynamic_pointer_cast<SeqScan>(child)) {
            childCard = (*tableStats.getValue((dynamic_pointer_cast<SeqScan>(child))->getTableName()))
                ->estimateTableCardinality(1.0);
        }

        vector<string> tmp = split(a->groupFieldName(), "[.]");
        string tableAlias = tmp[0];
        string pureFieldName = tmp[1];
        
        double groupFieldAvgSelectivity = 1.0;
        if (tableAliasToId.find(tableAlias) != tableAliasToId.end()) {
            size_t tableId = tableAliasToId[tableAlias];
            groupFieldAvgSelectivity = (*tableStats.getValue(
                Database::getCatalog()->getTableName(tableId)))
                ->avgSelectivity(
                    Database::getCatalog()->getTupleDesc(tableId)
                    ->fieldNameToIndex(pureFieldName),
                    Predicate::Op::EQUALS);
            a->setEstimatedCardinality((int)(min((double)childCard,
                1.0 / groupFieldAvgSelectivity)));
            return hasJoinPK;
        }
        a->setEstimatedCardinality(childCard);
        return hasJoinPK;
	}
}


