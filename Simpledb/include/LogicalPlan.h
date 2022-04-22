#pragma once
#include"LogicalJoinNode.h"
#include"LogicalSubplanJoinNode.h"
#include"LogicalScanNode.h"
#include"LogicalFilterNode.h"
#include"LogicalSelectListNode.h"
#include"TransactionId.h"
#include"OpIterator.h"
#include"Aggregator.h"
#include"TableStats.h"
#include<vector>
#include<map>
namespace Simpledb {
	class LogicalPlan {
	public:
        /** Constructor -- generate an empty logical plan */
        LogicalPlan()
            :_hasAgg(false), _oByAsc(false), _hasOrderBy(false) {}
       /**  Set the text of the query representing this logical plan.  Does NOT parse the
            specified query -- this method is just used so that the object can print the
            SQL it represents.

            @param query the text of the query associated with this plan
        */
        void setQuery(const string& query) {
            _query = query;
        }
        /** Get the query text associated with this plan via {@link #setQuery}.
        */
        string getQuery() {
            return _query;
        }
        /** Given a table alias, return id of the table object (this id can be supplied to {@link Catalog#getDatabaseFile(int)}).
        Aliases are added as base tables are added via {@link #addScan}.

        @param alias the table alias to return a table id for
        @return the id of the table corresponding to alias, throw runtime exception if the alias is unknown
        */
        size_t getTableId(const string& alias);
        map<string, size_t> getTableAliasToIdMapping(){
            return _tableMap;
        }
        /** Add a new filter to the logical plan
        *   @param field The name of the over which the filter applies;
        *   this can be a fully qualified field (tablename.field or
        *   alias.field), or can be a unique field name without a
        *   tablename qualifier.  If it is an ambiguous name, it will
        *   throw a runtime exception
        *   @param p The predicate for the filter
        *   @param constantValue the constant to compare the predicate
        *   against; if field is an integer field, this should be a
        *   String representing an integer
        *   @throws runtime exception if field is not in one of the tables
        *   added via {@link #addScan} or if field is ambiguous (e.g., two
        *   tables contain a field named field.)
        */
        void addFilter(string& field, Predicate::Op p,
            const string& constantValue);
        /** Add a join between two fields of two different tables.
        *  @param joinField1 The name of the first join field; this can
        *  be a fully qualified name (e.g., tableName.field or
        *  alias.field) or may be an unqualified unique field name.  If
        *  the name is ambiguous or unknown, a runtime error will be
        *  thrown.
        *  @param joinField2 The name of the second join field
        *  @param pred The join predicate
        *  @throws runtime error if either of the fields is ambiguous,
        *      or is not in one of the tables added via {@link #addScan}
        */
        void addJoin(string& joinField1, string& joinField2, Predicate::Op pred);
        /** Add a join between a field and a subquery.
        *  @param joinField1 The name of the first join field; this can
        *  be a fully qualified name (e.g., tableName.field or
        *  alias.field) or may be an unqualified unique field name.  If
        *  the name is ambiguous or unknown, a runtime error will be
        *  thrown.
        *  @param joinField2 the subquery to join with -- the join field
        *    of the subquery is the first field in the result set of the query
        *  @param pred The join predicate.
        *  @throws runtime error if either of the fields is ambiguous,
        *      or is not in one of the tables added via {@link #addScan}
        */
        void addJoin(string& joinField1, shared_ptr<OpIterator> joinField2, Predicate::Op pred);
        /** Add a scan to the plan. One scan node needs to be added for each alias of a table
            accessed by the plan.
            @param table the id of the table accessed by the plan (can be resolved to a DbFile using {@link Catalog#getDatabaseFile}
            @param name the alias of the table in the plan
        */
        void addScan(size_t table, string& name);
        /** Add a specified field/aggregate combination to the select list of the query.
            Fields are output by the query such that the rightmost field is the first added via addProjectField.
            @param fname the field to add to the output
            @param aggOp the aggregate operation over the field.
            @throws runtime error
         */
        void addProjectField(string& fname, string& aggOp);
        /** Add an aggregate over the field with the specified grouping to
            the query.  SimpleDb only supports a single aggregate
            expression and GROUP BY field.
            @param op the aggregation operator
            @param afield the field to aggregate over
            @param gfield the field to group by
            @throws runtime error
        */
        void addAggregate(string& op, string& afield, string& gfield);
        /** Add an ORDER BY expression in the specified order on the specified field.  SimpleDb only supports
            a single ORDER BY field.
            @param field the field to order by
            @param asc true if should be ordered in ascending order, false for descending order
            @throws runtime error
        */
        void addOrderBy(string& field, bool asc);
        /** Convert the aggregate operator name s into an Aggregator.op operation.
         *  @throws runtime error if s is not a valid operator name
         */
        static Aggregator::Op getAggOp(string& s);
        /** Convert this LogicalPlan into a physicalPlan represented by a {@link OpIterator}.  Attempts to
         *   find the optimal plan by using {@link JoinOptimizer#orderJoins} to order the joins in the plan.
         *  @param t The transaction that the returned OpIterator will run as a part of
         *  @param baseTableStats a HashMap providing a {@link TableStats}
         *    object for each table used in the LogicalPlan.  This should
         *    have one entry for each table referenced by the plan, not one
         *    entry for each table alias (so a table t aliases as t1 and
         *    t2 would have just one entry with key 't' in this HashMap).
         *  @param explain flag indicating whether output visualizing the physical
         *    query plan should be given.
         *  @throws runtime error if the logical plan is not valid
         *  @return A OpIterator representing this plan.
         */
        shared_ptr<OpIterator> physicalPlan(shared_ptr<TransactionId> t,
            ConcurrentMap<string, shared_ptr<TableStats>>& baseTableStats, bool explain);
	private:
        /** Given a name of a field, try to figure out what table it belongs to by looking
        *   through all of the tables added via {@link #addScan}.
        *  @return A fully qualified name of the form tableAlias.name.  If the name parameter is already qualified
        *   with a table name, simply returns name.
        *  @throws runtime exception if the field cannot be found in any of the tables, or if the
        *   field is ambiguous (appears in multiple tables)
        */
        string disambiguateName(string& name);



        vector<shared_ptr<LogicalJoinNode>> _joins;
        vector<shared_ptr<LogicalScanNode>> _tables;
        vector<shared_ptr<LogicalFilterNode>> _filters;
        vector<shared_ptr<LogicalSelectListNode>> _selectList;
        map<string, shared_ptr<OpIterator>> _subplanMap;
        map<string, size_t> _tableMap;      
        string _groupByField;
        bool _hasAgg;
        string _aggOp;
        string _aggField;
        bool _oByAsc;
        bool _hasOrderBy;
        string _oByField;
        string _query;
	};
}