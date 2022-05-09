#include"JoinOptimizer.h"
#include"JoinPredicate.h"
#include"Join.h"
#include"Database.h"
namespace Simpledb {
	shared_ptr<OpIterator> JoinOptimizer::instantiateJoin(shared_ptr<LogicalJoinNode> lj, shared_ptr<OpIterator> plan1, shared_ptr<OpIterator> plan2)
	{
        int t1id = 0, t2id = 0;
        shared_ptr<OpIterator> j;

        t1id = plan1->getTupleDesc()->fieldNameToIndex(lj->f1QuantifiedName);
        if (t1id == -1)
            throw runtime_error("Unknown field " + lj->f1QuantifiedName);

        shared_ptr<LogicalSubplanJoinNode> tmplj = dynamic_pointer_cast<LogicalSubplanJoinNode>(lj);
        if (tmplj != nullptr) {
            t2id = 0;
        }
        else {
            t2id = plan2->getTupleDesc()->fieldNameToIndex(lj->f2QuantifiedName);
            if (t2id == 0)
                throw runtime_error("Unknown field " + lj->f2QuantifiedName);
        }

        shared_ptr<JoinPredicate> p = make_shared<JoinPredicate>(t1id, lj->p, t2id);

        j = make_shared<Join>(p, plan1, plan2);
        //if (lj->p == Predicate::Op::EQUALS) {
        //    
        //    try {
        //        // dynamically load HashEquiJoin -- if it doesn't exist, just
        //        // fall back on regular join
        //        Class< ? > c = Class.forName("simpledb.execution.HashEquiJoin");
        //        java.lang.reflect.Constructor< ? > ct = c.getConstructors()[0];
        //        j = (OpIterator)ct
        //            .newInstance(new Object[]{ p, plan1, plan2 });
        //    }
        //    catch (Exception e) {
        //        j = new Join(p, plan1, plan2);
        //    }
        //}
        //else {
        //    j = new Join(p, plan1, plan2);
        //}

        return j;
	}
    double JoinOptimizer::estimateJoinCost(shared_ptr<LogicalJoinNode> j, int card1, int card2, double cost1, double cost2)
    {
        shared_ptr<LogicalSubplanJoinNode> tmpj = dynamic_pointer_cast<LogicalSubplanJoinNode>(j);
        if (tmpj != nullptr) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        }
        else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            return cost1 + card1 * cost2 + card1 * card2;
        }
    }
    int JoinOptimizer::estimateJoinCardinality(shared_ptr<LogicalJoinNode> j, int card1, int card2,
        bool t1pkey, bool t2pkey, ConcurrentMap<string, shared_ptr<TableStats>>& stats)
    {
        shared_ptr<LogicalSubplanJoinNode> tmpj = dynamic_pointer_cast<LogicalSubplanJoinNode>(j);
        if (tmpj != nullptr) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        }
        else {
            auto tmpMap = _p->getTableAliasToIdMapping();
            return estimateTableJoinCardinality(j->p, j->t1Alias, j->t2Alias,
                j->f1PureName, j->f2PureName, card1, card2, t1pkey, t2pkey,
                stats, tmpMap);
        }
    }
    int JoinOptimizer::estimateTableJoinCardinality(Predicate::Op joinOp, const string& table1Alias, const string& table2Alias,
        const string& field1PureName, const string& field2PureName, int card1, int card2,
        bool t1pkey, bool t2pkey, ConcurrentMap<string, shared_ptr<TableStats>>& stats, map<string, size_t>& tableAliasToId)
    {
        int card = 1;
        // some code goes here
        switch (joinOp)
        {
        case Simpledb::Predicate::Op::EQUALS:
            if (t1pkey && !t2pkey) {
                card = card2;
            }
            else if (!t1pkey && t2pkey) {
                card = card1;
            }
            else if (t1pkey && t2pkey) {
                card = min(card1, card2);
            }
            else {
                card = max(card1, card2);
            }
            break;
        case Simpledb::Predicate::Op::NOT_EQUALS:
            if (t1pkey && !t2pkey) {
                card = card1 * card2 - card2;
            }
            else if (!t2pkey && t2pkey) {
                card = card1 * card2 - card1;
            }
            else if (t1pkey && t2pkey) {
                card = card1 * card2 - min(card1, card2);
            }
            else {
                card = card1 * card2 - max(card1, card2);
            }
            break;
        default:
            card = round(0.3 * card1 * card2);
        }
        return card;
    }
    vector<shared_ptr<LogicalJoinNode>> JoinOptimizer::orderJoins(map<string, 
        shared_ptr<TableStats>>& stats, map<string, double>& filterSelectivities, bool explain)
    {
        // some code goes here
        //Replace the following
        return _joins;
    }
    shared_ptr<CostCard> JoinOptimizer::computeCostAndCardOfSubplan(ConcurrentMap<string, shared_ptr<TableStats>>& stats,
        map<string, double>& filterSelectivities, shared_ptr<LogicalJoinNode> joinToRemove,
        set<shared_ptr<LogicalJoinNode>>& joinSet, double bestCostSoFar, shared_ptr<PlanCache> pc)
    {
        shared_ptr<LogicalJoinNode> j = joinToRemove;     
        try
        {
            _p->getTableId(j->t1Alias);
        }
        catch (const std::exception&)
        {
            throw runtime_error("Unknown table " + j->t1Alias);
        }
        try
        {
            _p->getTableId(j->t2Alias);
        }
        catch (const std::exception&)
        {
            throw runtime_error("Unknown table " + j->t2Alias);
        }

        string table1Name = Database::getCatalog()->getTableName(
            _p->getTableId(j->t1Alias));
        string table2Name = Database::getCatalog()->getTableName(
            _p->getTableId(j->t2Alias));
        string table1Alias = j->t1Alias;
        string table2Alias = j->t2Alias;

        set<shared_ptr<LogicalJoinNode>> news(joinSet);
        news.erase(j);

        double t1cost, t2cost;
        int t1card, t2card;
        bool leftPkey, rightPkey;

        vector<shared_ptr<LogicalJoinNode>> prevBest;
        if (news.empty()) { // base case -- both are base relations
            t1cost = (*stats.getValue(table1Name))->estimateScanCost();
            t1card = (*stats.getValue(table1Name))->estimateTableCardinality(
                filterSelectivities.at(j->t1Alias));
            leftPkey = isPkey(j->t1Alias, j->f1PureName);

            t2cost = table2Alias == "" ? 0 : (*stats.getValue(table2Name))->estimateScanCost();
            t2card = table2Alias == "" ? 0 : (*stats.getValue(table2Name))->estimateTableCardinality(
                    filterSelectivities.at(j->t2Alias));
            rightPkey = table2Alias != "" && isPkey(table2Alias, j->f2PureName);
        }
        else {
            // news is not empty -- figure best way to join j to news
            prevBest = pc->getOrder(news);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest.empty()) {
                return nullptr;
            }

            double prevBestCost = pc->getCost(news);
            int bestCard = pc->getCard(news);

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                t1cost = prevBestCost; // left side just has cost of whatever
                                       // left subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j->t2Alias == "" ? 0 : (*stats.getValue(table2Name))->estimateScanCost();
                t2card = j->t2Alias == "" ? 0 : (*stats.getValue(table2Name))->estimateTableCardinality(
                        filterSelectivities.at(j->t2Alias));
                rightPkey = j->t2Alias != "" && isPkey(j->t2Alias,
                    j->f2PureName);
            }
            else if (doesJoin(prevBest, j->t2Alias)) { // j.t2 is in prevbest
                                                     // (both shouldn't be)
                t2cost = prevBestCost; // left side just has cost of whatever
                                       // left subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = (*stats.getValue(table1Name))->estimateScanCost();
                t1card = (*stats.getValue(table1Name))->estimateTableCardinality(
                    filterSelectivities.at(j->t1Alias));
                leftPkey = isPkey(j->t1Alias, j->f1PureName);

            }
            else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return nullptr;
            }
        }

        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        shared_ptr<LogicalJoinNode> j2 = j->swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            bool tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return nullptr;

        shared_ptr<CostCard> cc = make_shared<CostCard>();

        cc->card = estimateJoinCardinality(j, t1card, t2card, leftPkey,
            rightPkey, stats);
        cc->cost = cost1;
        cc->plan = vector<shared_ptr<LogicalJoinNode>>(prevBest);
        cc->plan.push_back(j); // prevbest is left -- add new join to end
        return cc;
    }
    bool JoinOptimizer::doesJoin(vector<shared_ptr<LogicalJoinNode>>& joinlist, const string& table)
    {
        for (auto& j : joinlist) {
            if (j->t1Alias == table
                || (j->t2Alias != "" && j->t2Alias == table))
                return true;
        }
        return false;
    }
    bool JoinOptimizer::isPkey(const string& tableAlias, const string& field)
    {
        size_t tid1 = _p->getTableId(tableAlias);
        string pkey1 = Database::getCatalog()->getPrimaryKey(tid1);

        return pkey1 == field;
    }
    bool JoinOptimizer::hasPkey(vector<shared_ptr<LogicalJoinNode>>& joinlist)
    {
        for (auto& j : joinlist) {
            if (isPkey(j->t1Alias, j->f1PureName)
                || (j->t2Alias != "" && isPkey(j->t2Alias, j->f2PureName)))
                return true;
        }
        return false;
    }
}