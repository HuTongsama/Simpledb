#pragma once
#include"Predicate.h"
#include"Common.h"
namespace Simpledb {
	/** A LogicalFilterNode represents the parameters of a filter in the WHERE clause of a query.
	<p>
	Filter is of the form t.f p c
	<p>
	Where t is a table, f is a field in t, p is a predicate, and c is a constant
	*/
	class LogicalFilterNode {
	public:
        /** The alias of a table (or the name if no alias) over which the filter ranges */
        string tableAlias;
        /** The predicate in the filter */
        Predicate::Op p;
        /* The constant on the right side of the filter */
        string c;
        /** The field from t which is in the filter. The pure name, without alias or tablename*/
        string fieldPureName;
        string fieldQuantifiedName;

        LogicalFilterNode(const string& table, const string& field, Predicate::Op pred, const string& constant) {
            tableAlias = table;
            p = pred;
            c = constant;
            vector<string> tmps = split(field, "[.]");
            if (tmps.size() > 1)
                fieldPureName = tmps[tmps.size() - 1];
            else
                fieldPureName = field;
            fieldQuantifiedName = tableAlias + "." + fieldPureName;
        }
	};
}