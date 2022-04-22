#include"LogicalPlan.h"
#include"Database.h"
#include"SeqScan.h"
#include"IntField.h"
#include"StringField.h"
#include"Filter.h"
#include"JoinOptimizer.h"
#include<stdexcept>
namespace Simpledb {
	size_t LogicalPlan::getTableId(const string& alias)
	{
		if (_tableMap.find(alias) != _tableMap.end()) {
			return _tableMap[alias];
		}
		throw runtime_error("no such element");
	}
    void LogicalPlan::addFilter(string& field, Predicate::Op p, const string& constantValue)
    {
        field = disambiguateName(field);
        vector<string> tmp = split(field, "[.]");
        string table = tmp[0];

        shared_ptr<LogicalFilterNode> lf = make_shared<LogicalFilterNode>(table, tmp[1], p, constantValue);
        _filters.push_back(lf);
    }
    void LogicalPlan::addJoin(string& joinField1, string& joinField2, Predicate::Op pred)
    {
        joinField1 = disambiguateName(joinField1);
        joinField2 = disambiguateName(joinField2);
        vector<string> tmp1 = split(joinField1, "[.]");
        vector<string> tmp2 = split(joinField2, "[.]");
        string table1Alias = tmp1[0];
        string table2Alias = tmp2[0];
        string pureField1 = tmp1[1];
        string pureField2 = tmp2[1];

        if (table1Alias == table2Alias)
            throw runtime_error("Cannot join on two fields from same table");
        shared_ptr<LogicalJoinNode> lj = make_shared<LogicalJoinNode>(table1Alias, table2Alias, pureField1, pureField2, pred);
        cout << ("Added join between " + joinField1 + " and " + joinField2) << endl;
        _joins.push_back(lj);
    }
    void LogicalPlan::addJoin(string& joinField1, shared_ptr<OpIterator> joinField2, Predicate::Op pred)
    {
        joinField1 = disambiguateName(joinField1);
        vector<string> tmp = split(joinField1, "[.]");
        string table1 = tmp[0];
        string pureField = tmp[1];

        shared_ptr<LogicalSubplanJoinNode> lj = make_shared<LogicalSubplanJoinNode>(table1, pureField, joinField2, pred);
        cout << ("Added subplan join on " + joinField1) << endl;
        _joins.push_back(lj);
    }
    void LogicalPlan::addScan(size_t table, string& name)
    {
        cout << ("Added scan of table " + name) << endl;
        _tables.push_back(make_shared<LogicalScanNode>(table, name));
        _tableMap[name] = table;
    }
    void LogicalPlan::addProjectField(string& fname, string& aggOp)
    {
        fname = disambiguateName(fname);
        if (fname == "*")
            fname = "null.*";
        cout << ("Added select list field " + fname) << endl;
        if (aggOp != "") {
            cout << ("\t with aggregator " + aggOp) << endl;
        }
        _selectList.push_back(make_shared<LogicalSelectListNode>(aggOp, fname));
    }
    void LogicalPlan::addAggregate(string& op, string& afield, string& gfield)
    {
        afield = disambiguateName(afield);
        if (gfield != "")
            gfield = disambiguateName(gfield);
        _aggOp = op;
        _aggField = afield;
        _groupByField = gfield;
        _hasAgg = true;
    }
    void LogicalPlan::addOrderBy(string& field, bool asc)
    {
        field = disambiguateName(field);
        _oByField = field;
        _oByAsc = asc;
        _hasOrderBy = true;
    }
    Aggregator::Op LogicalPlan::getAggOp(string& s)
    {
        for (auto& c : s) {
            c = toupper(c);
        }
        if (s == "AVG") return Aggregator::Op::AVG;
        if (s == "SUM") return Aggregator::Op::SUM;
        if (s == "COUNT") return Aggregator::Op::COUNT;
        if (s == "MIN") return Aggregator::Op::MIN;
        if (s == "MAX") return Aggregator::Op::MAX;
        throw runtime_error("Unknown predicate " + s);
    }
    shared_ptr<OpIterator> LogicalPlan::physicalPlan(shared_ptr<TransactionId> t,
        ConcurrentMap<string, shared_ptr<TableStats>>& baseTableStats, bool explain)
    {
        map<string, string> equivMap;
        map<string, double> filterSelectivities;
        map<string, shared_ptr<TableStats>> statsMap;
        for (auto& table : _tables) {
            shared_ptr<SeqScan> ss = nullptr;
            try
            {
                ss = make_shared<SeqScan>(t, Database::getCatalog()->getDatabaseFile(table->t)->getId(), table->alias);
            }
            catch (const std::exception&)
            {
                throw runtime_error("Unknown table " + table->t);
            }
            _subplanMap[table->alias] = ss;
            string baseTableName = Database::getCatalog()->getTableName(table->t);
            shared_ptr<TableStats>* p = baseTableStats.getValue(baseTableName);
            if (p != nullptr)
                statsMap[baseTableName] = *p;
            filterSelectivities[table->alias] = 1.0;
        }


        for (auto& lf : _filters) {
            shared_ptr<OpIterator> subplan = _subplanMap[lf->tableAlias];
            if (subplan == nullptr) {
                throw runtime_error("Unknown table in WHERE clause " + lf->tableAlias);
            }

            shared_ptr<Field> f;
            shared_ptr<Type> ftyp;
            shared_ptr<TupleDesc> td = _subplanMap[lf->tableAlias]->getTupleDesc();

            try {//td.fieldNameToIndex(disambiguateName(lf.fieldPureName))
                ftyp = td->getFieldType(td->fieldNameToIndex(lf->fieldQuantifiedName));
            }
            catch (const std::exception&) {
                throw runtime_error("Unknown field in filter expression " + lf->fieldQuantifiedName);
            }
            if (ftyp->type() == Type::TYPE::INT_TYPE)
                f = make_shared<IntField>(stoi(lf->c));
            else
                f = make_shared<StringField>(lf->c, Type::STRING_LEN);

            shared_ptr<Predicate> p = nullptr;
            try {
                p = make_shared<Predicate>(subplan->getTupleDesc()->fieldNameToIndex(lf->fieldQuantifiedName), lf->p, f);
            }
            catch (const std::exception&) {
                throw runtime_error("Unknown field " + lf->fieldQuantifiedName);
            }
            _subplanMap[lf->tableAlias] = make_shared<Filter>(p, subplan);

            shared_ptr<TableStats> s = statsMap[Database::getCatalog()->getTableName(getTableId(lf->tableAlias))];

            double sel = s->estimateSelectivity(subplan->getTupleDesc()->fieldNameToIndex(lf->fieldQuantifiedName), lf->p, f);
            filterSelectivities[lf->tableAlias] =  filterSelectivities[lf->tableAlias] * sel;

            //s.addSelectivityFactor(estimateFilterSelectivity(lf,statsMap));
        }

        //shared_ptr<JoinOptimizer> jo = make_shared<JoinOptimizer>(this, joins);

        //joins = jo.orderJoins(statsMap, filterSelectivities, explain);

        //for (LogicalJoinNode lj : joins) {
        //    OpIterator plan1;
        //    OpIterator plan2;
        //    boolean isSubqueryJoin = lj instanceof LogicalSubplanJoinNode;
        //    String t1name, t2name;

        //    if (equivMap.get(lj.t1Alias) != null)
        //        t1name = equivMap.get(lj.t1Alias);
        //    else
        //        t1name = lj.t1Alias;

        //    if (equivMap.get(lj.t2Alias) != null)
        //        t2name = equivMap.get(lj.t2Alias);
        //    else
        //        t2name = lj.t2Alias;

        //    plan1 = subplanMap.get(t1name);

        //    if (isSubqueryJoin) {
        //        plan2 = ((LogicalSubplanJoinNode)lj).subPlan;
        //        if (plan2 == null)
        //            throw new ParsingException("Invalid subquery.");
        //    }
        //    else {
        //        plan2 = subplanMap.get(t2name);
        //    }

        //    if (plan1 == null)
        //        throw new ParsingException("Unknown table in WHERE clause " + lj.t1Alias);
        //    if (plan2 == null)
        //        throw new ParsingException("Unknown table in WHERE clause " + lj.t2Alias);

        //    OpIterator j;
        //    j = JoinOptimizer.instantiateJoin(lj, plan1, plan2);
        //    subplanMap.put(t1name, j);

        //    if (!isSubqueryJoin) {
        //        subplanMap.remove(t2name);
        //        equivMap.put(t2name, t1name);  //keep track of the fact that this new node contains both tables
        //        //make sure anything that was equiv to lj.t2 (which we are just removed) is
        //        // marked as equiv to lj.t1 (which we are replacing lj.t2 with.)
        //        for (Map.Entry<String, String> s : equivMap.entrySet()) {
        //            String val = s.getValue();
        //            if (val.equals(t2name)) {
        //                s.setValue(t1name);
        //            }
        //        }

        //        // subplanMap.put(lj.t2, j);
        //    }

        //}

        //if (subplanMap.size() > 1) {
        //    throw new ParsingException("Query does not include join expressions joining all nodes!");
        //}

        //OpIterator node = subplanMap.entrySet().iterator().next().getValue();

        ////walk the select list, to determine order in which to project output fields
        //List<Integer> outFields = new ArrayList<>();
        //List<Type> outTypes = new ArrayList<>();
        //for (int i = 0; i < selectList.size(); i++) {
        //    LogicalSelectListNode si = selectList.get(i);
        //    if (si.aggOp != null) {
        //        outFields.add(groupByField != null ? 1 : 0);
        //        TupleDesc td = node.getTupleDesc();
        //        //                int  id;
        //        try {
        //            //                    id = 
        //            td.fieldNameToIndex(si.fname);
        //        }
        //        catch (NoSuchElementException e) {
        //            throw new ParsingException("Unknown field " + si.fname + " in SELECT list");
        //        }
        //        outTypes.add(Type.INT_TYPE);  //the type of all aggregate functions is INT

        //    }
        //    else if (hasAgg) {
        //        if (groupByField == null) {
        //            throw new ParsingException("Field " + si.fname + " does not appear in GROUP BY list");
        //        }
        //        outFields.add(0);
        //        TupleDesc td = node.getTupleDesc();
        //        int  id;
        //        try {
        //            id = td.fieldNameToIndex(groupByField);
        //        }
        //        catch (NoSuchElementException e) {
        //            throw new ParsingException("Unknown field " + groupByField + " in GROUP BY statement");
        //        }
        //        outTypes.add(td.getFieldType(id));
        //    }
        //    else if (si.fname.equals("null.*")) {
        //        TupleDesc td = node.getTupleDesc();
        //        for (i = 0; i < td.numFields(); i++) {
        //            outFields.add(i);
        //            outTypes.add(td.getFieldType(i));
        //        }
        //    }
        //    else {
        //        TupleDesc td = node.getTupleDesc();
        //        int id;
        //        try {
        //            id = td.fieldNameToIndex(si.fname);
        //        }
        //        catch (NoSuchElementException e) {
        //            throw new ParsingException("Unknown field " + si.fname + " in SELECT list");
        //        }
        //        outFields.add(id);
        //        outTypes.add(td.getFieldType(id));

        //    }
        //}

        //if (hasAgg) {
        //    TupleDesc td = node.getTupleDesc();
        //    Aggregate aggNode;
        //    try {
        //        aggNode = new Aggregate(node,
        //            td.fieldNameToIndex(aggField),
        //            groupByField == null ? Aggregator.NO_GROUPING : td.fieldNameToIndex(groupByField),
        //            getAggOp(aggOp));
        //    }
        //    catch (NoSuchElementException | IllegalArgumentException e) {
        //        throw new simpledb.ParsingException(e);
        //    }
        //    node = aggNode;
        //}

        //if (hasOrderBy) {
        //    node = new OrderBy(node.getTupleDesc().fieldNameToIndex(oByField), oByAsc, node);
        //}

        //return new Project(outFields, outTypes, node);
    }
    string LogicalPlan::disambiguateName(string& name)
	{
        vector<string> fields = split(name, "[.]");
        size_t sz = fields.size();
        if (sz == 2 && (fields[0] != "null"))
            return name;
        if (sz > 2)
            throw new runtime_error("Field " + name + " is not a valid field reference.");
        if (sz == 2)
            name = fields[1];
        if (name == "*")
            return name;
        //now look for occurrences of name in all of the tables
        string tableName = "";
        for (auto& node : _tables) {
            try
            {
                shared_ptr<TupleDesc> td = Database::getCatalog()->getDatabaseFile(node->t)->getTupleDesc();
                int id = td->fieldNameToIndex(name);
                if (id == -1)
                    continue;
                if (tableName == "") {
                    tableName = node->alias;
                }                  
                else {
                    throw runtime_error("Field " + name + " appears in multiple tables; disambiguate by referring to it as tablename." + name);
                }
            }
            catch (const std::exception&)
            {
                //ignore
            }
        }
        if (tableName != "")
            return tableName + "." + name;
        else
            throw new runtime_error("Field " + name + " does not appear in any tables.");
	}
}