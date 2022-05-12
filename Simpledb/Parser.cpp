#include"Parser.h"
#include"Delete.h"
#include"Insert.h"
#include"IntField.h"
#include"StringField.h"
#include"OperatorCardinality.h"
namespace Simpledb {
    bool Parser::explain = false;
    Predicate::Op Parser::getOp(hsql::OperatorType type)
    {
        switch (type)
        {
        case hsql::kOpEquals:
        case hsql::kOpLike:
        case hsql::kOpILike:
            return Predicate::Op::EQUALS;
        case hsql::kOpNotLike:
        case hsql::kOpNotEquals:
            return Predicate::Op::NOT_EQUALS;
        case hsql::kOpLess:
            return Predicate::Op::LESS_THAN;
        case hsql::kOpLessEq:
            return Predicate::Op::LESS_THAN_OR_EQ;
        case hsql::kOpGreater:
            return Predicate::Op::GREATER_THAN;
        case hsql::kOpGreaterEq:
            return Predicate::Op::GREATER_THAN_OR_EQ;
        default:
            throw runtime_error("Unknown predicate");
        }
     
    }
    
    void Parser::processExpression(shared_ptr<TransactionId> tid, hsql::Expr* expr, shared_ptr<LogicalPlan> lp)
    {
        if (expr->isType(hsql::ExprType::kExprOperator) && expr->opType == hsql::OperatorType::kOpAnd) {
            
            /*if (expr->exprList != nullptr) {
                size_t sz = expr->exprList->size();
                for (int i = 0; i < sz; ++i) {
                    hsql::Expr* subExpr = expr->exprList->at(i);
                    if (subExpr->opType == hsql::OperatorType::kOpIsNull) {
                        throw runtime_error("Nested queries are currently unsupported.");
                    }
                    processExpression(tid, subExpr, lp);
                }
            }
            else {
                throw runtime_error("empty exprList");
            }*/    
            if (expr->expr != nullptr && expr->expr2 != nullptr) {
                processExpression(tid, expr->expr, lp);
                processExpression(tid, expr->expr2, lp);
            }
            else {
                throw runtime_error("error AND operands");
            }
        }
        else if (expr->isType(hsql::ExprType::kExprOperator) && expr->opType == hsql::OperatorType::kOpOr) {
            throw runtime_error("OR expressions currently unsupported.");
        }
        else {
            if (expr->expr != nullptr && expr->expr2 != nullptr) {
                /*if (expr->exprList->size() != 2) {
                    throw runtime_error("Only simple binary expresssions of the form A op B are currently supported.");
                }*/
                bool isJoin = false;
                Predicate::Op op = getOp(expr->opType);
                bool op1const = false, op2const = false;
                hsql::Expr* expr1 = expr->expr;
                hsql::Expr* expr2 = expr->expr2;
                if (isConstant(expr1->type))
                    op1const = true;
                if (isConstant(expr2->type))
                    op2const = true;
                if (op1const && op2const) {
                    isJoin = (expr1->type == hsql::ExprType::kExprColumnRef
                        && expr2->type == hsql::ExprType::kExprColumnRef);
                }
                else if (expr1->type == hsql::ExprType::kExprSelect
                    || expr2->type == hsql::ExprType::kExprSelect) {
                    isJoin = true;
                }
                else if (expr1->opType != hsql::OperatorType::kOpNone
                    || expr1->opType != hsql::OperatorType::kOpNone) {
                    throw runtime_error("Only simple binary expresssions of the form A op B are currently supported,\
                    where A or B are fields, constants, or subqueries.");
                }
                else{
                    isJoin = false;
                }
                
                if (isJoin) {// join node
                    string tab1field = "", tab2field = "";
                    if (!op1const) {
                        // left op is a nested query
                        // generate a virtual table for the left op
                        // this isn't a valid ZQL query
                    }
                    else {
                        tab1field = expr1->getName();
                    }

                    if (!op2const) {// right op is a nested query
                        shared_ptr<LogicalPlan> sublp = parseQueryLogicalPlan(tid, expr2->select);
                        shared_ptr<OpIterator> pp = sublp->physicalPlan(tid, TableStats::getStatsMap(), explain);
                        lp->addJoin(tab1field, tab2field, op);
                    }
                    else {
                        tab2field = expr2->getName();
                        lp->addJoin(tab1field, tab2field, op);
                    }

                }
                else {// select node
                    string column, compValue;
                    hsql::Expr* columnExpr = nullptr;
                    hsql::Expr* valExpr = nullptr;
                    if (expr1->type == hsql::ExprType::kExprColumnRef) {
                        columnExpr = expr1;
                        valExpr = expr2;
                    }
                    else {
                        columnExpr = expr2;
                        valExpr = expr1;
                    }
                    column = columnExpr->getName();
                    if (valExpr->type == hsql::ExprType::kExprLiteralInt)
                        compValue = to_string(valExpr->ival);
                    else if (valExpr->type == hsql::ExprType::kExprLiteralFloat)
                        compValue = to_string(valExpr->fval);
                    else
                        compValue = valExpr->getName();
                    lp->addFilter(column, op, compValue);
                }
            }
            else {
                throw runtime_error("empty exprList");
            }
        }
    }
    
    shared_ptr<LogicalPlan> Parser::parseQueryLogicalPlan(shared_ptr<TransactionId> tid, const hsql::SelectStatement* q)
    {
        vector<hsql::TableRef*>* from = q->fromTable->list;
        shared_ptr<LogicalPlan> lp = make_shared<LogicalPlan>();
        lp->setQuery(_curQueryStr);
        size_t sz = from->size();
        // walk through tables in the FROM clause
        for (int i = 0; i < sz; i++) {
            hsql::TableRef* fromIt = from->at(i);
            try
            {
                // will fall through if table doesn't exist
                size_t id = Database::getCatalog()->getTableId(fromIt->name);
                string name;
                if (fromIt->alias != nullptr)
                    name = fromIt->alias->name;
                else
                    name = fromIt->name;
                lp->addScan(id, name);
            }
            catch (const std::exception&)
            {
                throw runtime_error(string("Table ") + fromIt->name + " is not in catalog");
            }
        }

        // now parse the where clause, creating Filter and Join nodes as needed
        hsql::Expr* w = q->whereClause;
        if (w != nullptr) {
            processExpression(tid, w, lp);
        }

        // now look for group by fields
        hsql::GroupByDescription* gby = q->groupBy;
        string groupByField = "";
        if (gby != nullptr) {
            vector<hsql::Expr*>* gbs = gby->columns;
            if (gbs->size() > 1) {
                throw runtime_error(
                    "At most one grouping field expression supported.");
            }
            if (gbs->size() == 1) {
                hsql::Expr* gbe = gbs->at(0);
                if (!isConstant(gbe->type)) {
                    throw runtime_error(
                        string("Complex grouping expressions (") + gbe->name
                        + ") not supported.");
                }
                groupByField = gbe->name;
                cout << "GROUP BY FIELD : " << groupByField << endl;
            }

        }

        // walk the select list, pick out aggregates, and check for query
        // validity
        vector<hsql::Expr*>* selectList = q->selectList;
        string aggField = "";
        string aggFun = "";
        if (selectList != nullptr) {
            sz = selectList->size();
            for (int i = 0; i < sz; ++i) {
                hsql::Expr* si = selectList->at(i);
                if (si->type != hsql::ExprType::kExprFunctionRef
                    && !isConstant(si->type)) {
                    throw runtime_error("Expressions in SELECT list are not supported.");
                }
                if (si->type == hsql::ExprType::kExprFunctionRef) {
                    if (aggField != "" || si->exprList->size() > 1) {
                        throw runtime_error("Aggregates over multiple fields not supported.");
                    }
                    aggField = si->exprList->at(0)->name;
                    aggFun = si->name;
                    cout << "Aggregate field is " << aggField << ", agg fun is : " << aggFun << endl;
                    lp->addProjectField(aggField, aggFun);
                }
                else {
                    if (si->table != nullptr && si->name != nullptr) {
                        string tmp = string(si->table) + "." + si->name;
                        if (groupByField != ""
                            && !(groupByField == tmp
                                || groupByField == si->name)) {
                            throw runtime_error(string("Non-aggregate field ") + si->name + " does not appear in GROUP BY list.");
                        }
                        string tmp1("");
                        lp->addProjectField(tmp, tmp1);
                    }
                }
            }
        }

        if (groupByField != "" && aggFun == "") {
            throw runtime_error("GROUP BY without aggregation.");
        }

        if (aggFun != "") {
            lp->addAggregate(aggFun, aggField, groupByField);
        }
        // sort the data
        vector<hsql::OrderDescription*>* orderBy = q->order;
        if (orderBy != nullptr) {

            if (orderBy->size() > 1) {
                throw runtime_error(
                    "Multi-attribute ORDER BY is not supported.");
            }
            hsql::OrderDescription* oby = orderBy->at(0);
            if (!isConstant(oby->expr->type)) {
                throw runtime_error(
                    "Complex ORDER BY's are not supported");
            }
            hsql::Expr* f = oby->expr;
            string field(f->name);
            bool isAsc = oby->type == hsql::OrderType::kOrderAsc;
            lp->addOrderBy(field, isAsc);

        }
        return lp;
    }
    
    shared_ptr<Query> Parser::handleQueryStatement(const hsql::SelectStatement* s, shared_ptr<TransactionId> tId)
    {
        shared_ptr<Query> query = make_shared<Query>(tId);

        shared_ptr<LogicalPlan> lp = parseQueryLogicalPlan(tId, s);
        shared_ptr<OpIterator> physicalPlan = lp->physicalPlan(tId,
            TableStats::getStatsMap(), explain);
        query->setPhysicalPlan(physicalPlan);
        query->setLogicalPlan(lp);

        if (physicalPlan != nullptr) {
            try
            {
                cout << "The query plan is:" << endl;
                
            }
            catch (const std::exception&)
            {
                auto map = lp->getTableAliasToIdMapping();
                OperatorCardinality::updateOperatorCardinality(dynamic_pointer_cast<Operator>(physicalPlan),
                    map, TableStats::getStatsMap());
            }
        }

        return query;
    }
    
    shared_ptr<Query> Parser::handleInsertStatement(const hsql::InsertStatement* s, shared_ptr<TransactionId> tId)
    {
        size_t tableId;
        try
        {
            tableId = Database::getCatalog()->getTableId(s->tableName); // will fall through if table doesn't exist
        }
        catch (const std::exception&)
        {
            throw runtime_error(string("Unknown table : ") + s->tableName);
        }

        shared_ptr<TupleDesc> td = Database::getCatalog()->getTupleDesc(tableId);
        shared_ptr<Tuple> t = make_shared<Tuple>(td);
        int i = 0;
        shared_ptr<OpIterator> newTups;

        if (s->values != nullptr) {
            size_t sz = s->values->size();
            if (td->numFields() != sz) {
                throw runtime_error(string("INSERT statement does not contain same number of fields as table ")
                    + s->tableName);
            }
            for (auto e : *(s->values)) {
                if (!isConstant(e->type)) {
                    throw runtime_error("Complex expressions not allowed in INSERT statements.");
                }
                if (e->type == hsql::ExprType::kExprLiteralInt) {
                    if (td->getFieldType(i)->type() != Type::TYPE::INT_TYPE) {
                        throw runtime_error(string("Value ") + e->name + " is an integer, expected a string.");
                    }
                    shared_ptr<IntField> f = make_shared<IntField>(e->ival);
                    t->setField(i, f);
                }
                else if (e->type == hsql::ExprType::kExprLiteralString) {
                    if (td->getFieldType(i)->type() != Type::TYPE::STRING_TYPE) {
                        throw runtime_error(string("Value ") + e->name + " is a string, expected an integer.");
                    }
                    shared_ptr<StringField> f = make_shared<StringField>(e->name, Type::STRING_LEN);
                    t->setField(i, f);
                }
                else {
                    throw runtime_error("Only string or int fields are supported.");
                }
                i++;
            }
            vector<shared_ptr<Tuple>> tups;
            tups.push_back(t);
            newTups = make_shared<TupleIterator>(td, tups);

        }
        else {
            hsql::SelectStatement* select = s->select;
            shared_ptr<LogicalPlan> lp = parseQueryLogicalPlan(tId, select);
            newTups = lp->physicalPlan(tId, TableStats::getStatsMap(), explain);
        }
        shared_ptr<Query> insertQ = make_shared<Query>(tId);
        insertQ->setPhysicalPlan(make_shared<Insert>(tId, newTups, tableId));
        return insertQ;
    }
    
    shared_ptr<Query> Parser::handleDeleteStatement(const hsql::DeleteStatement* s, shared_ptr<TransactionId> tid)
    {
        size_t id;
        try
        {
            // will fall through if table doesn't exist
            id = Database::getCatalog()->getTableId(s->tableName);
        }
        catch (const std::exception&)
        {
            throw runtime_error(string("Unknown table : ") + s->tableName);
        }

        string name = s->tableName;
        shared_ptr<Query> sdbq = make_shared<Query>(tid);
        shared_ptr<LogicalPlan> lp = make_shared<LogicalPlan>();
        lp->setQuery(_curQueryStr);
        lp->addScan(id, name);
        if (s->expr != nullptr)
            processExpression(tid, s->expr, lp);
        string str1("null.*"), str2("");
        lp->addProjectField(str1, str2);

        shared_ptr<OpIterator> op = make_shared<Delete>(tid, lp->physicalPlan(tid,
            TableStats::getStatsMap(), false));
        sdbq->setPhysicalPlan(op);
        return sdbq;
    }
       
    void Parser::handleTransactStatement(const hsql::TransactionStatement* s)
    {
        hsql::TransactionCommand command = s->command;
        switch (command)
        {
        case hsql::kBeginTransaction:
            if (_curtrans != nullptr)
                throw runtime_error(
                    "Can't start new transactions until current transaction has been committed or rolledback.");
            _curtrans = make_shared<Transaction>();
            _curtrans->start();
            _inUserTrans = true;
            cout << "Started a new transaction tid = "
                << _curtrans->getId()->getId() << endl;
            break;
        case hsql::kCommitTransaction:
            if (_curtrans == nullptr)
                throw runtime_error(
                    "No transaction is currently running");
            _curtrans->commit();
            _curtrans = nullptr;
            _inUserTrans = false;
            cout << "Transaction " << _curtrans->getId()->getId()
                << " committed." << endl;
            break;
        case hsql::kRollbackTransaction:
            if (_curtrans == nullptr)
                throw runtime_error(
                    "No transaction is currently running");
            _curtrans->abort();
            _curtrans = nullptr;
            _inUserTrans = false;
            cout << "Transaction " << _curtrans->getId()->getId()
                << " aborted." << endl;
            break;
        default:
            throw runtime_error("Unsupported operation");
            break;
        }
    }
    
    shared_ptr<LogicalPlan> Parser::generateLogicalPlan(shared_ptr<TransactionId> tid, const string& s)
    {
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(s, &result);
        _curQueryStr = s;
        if (result.isValid() && result.size() > 0) {
            try
            {
                const hsql::SQLStatement* pStatement = result.getStatement(0);
                if (dynamic_cast<const hsql::SelectStatement*>(pStatement)) {
                    return parseQueryLogicalPlan(tid, dynamic_cast<const hsql::SelectStatement*>(pStatement));
                }
            }
            catch (const std::exception&)
            {
                throw runtime_error("Invalid SQL expression: \n \t " + s);
            }
        }
        throw runtime_error("Cannot generate logical plan for expression : " + s);
    }

    void Parser::processNextStatement(const string& strQuery)
	{
        try
        {
            hsql::SQLParserResult result;
            hsql::SQLParser::parse(strQuery, &result);
            _curQueryStr = strQuery;
            if (result.isValid()) {
                for (auto i = 0u; i < result.size(); ++i) {
                    // Print a statement summary.
                    const hsql::SQLStatement* pStatement = result.getStatement(i);
                    shared_ptr<Query> query = nullptr;
                    if (pStatement->isType(hsql::StatementType::kStmtTransaction)) {

                    }
                    else {
                        if (!_inUserTrans) {
                            _curtrans = make_shared<Transaction>();
                            _curtrans->start();
                            cout << "Started a new transaction tid = "
                                << _curtrans->getId()->getId() << endl;
                        }
                        try
                        {
                            if (pStatement->isType(hsql::StatementType::kStmtInsert)) {

                            }
                            else if (pStatement->isType(hsql::StatementType::kStmtDelete)) {

                            }
                            else if (pStatement->isType(hsql::StatementType::kStmtSelect)) {

                            }
                            else {
                                cout << ("Can't parse "
                                    + strQuery
                                    + "\n -- parser only handles SQL transactions, insert, delete, and select statements") << endl;
                            }
                            if (query != nullptr) {
                                query->execute();
                            }
                            if (!_inUserTrans && _curtrans != nullptr) {
                                _curtrans->commit();
                                cout << "Transaction "
                                    << _curtrans->getId()->getId() << " committed.";
                            }
                        }
                        catch (const std::exception& e)
                        {
                            // Whenever error happens, abort the current transaction
                            if (_curtrans != nullptr) {
                                _curtrans->abort();
                                cout << "Transaction "
                                    << _curtrans->getId()->getId()
                                    << " aborted because of unhandled error" << endl;
                            }
                            _inUserTrans = false;
                            throw new runtime_error(e.what());
                        }
                    }
                }
            }
            else {
                fprintf(stderr, "Given string is not a valid SQL query.\n");
                fprintf(stderr, "%s (L%d:%d)\n",
                    result.errorMsg(),
                    result.errorLine(),
                    result.errorColumn());
            }
        }
        catch (const std::exception& e)
        {
            cout << "process Statement failed :" << e.what() << endl;
        }

	}
    


    bool Parser::isConstant(hsql::ExprType exprType)
    {
        if (exprType == hsql::ExprType::kExprLiteralInt
            || exprType == hsql::ExprType::kExprLiteralFloat
            || exprType == hsql::ExprType::kExprLiteralNull
            || exprType == hsql::ExprType::kExprLiteralString
            || exprType == hsql::ExprType::kExprStar)
            return true;
        return false;
    }
}