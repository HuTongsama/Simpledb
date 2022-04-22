#pragma once
#include<string>
using namespace std;
namespace Simpledb {
	/** A LogicalScanNode represents table in the FROM list in a
	* LogicalQueryPlan */
	class LogicalScanNode {
	public:
        /** The name (alias) of the table as it is used in the query */
        string alias;
        /** The table identifier (can be passed to {@link Catalog#getDatabaseFile})
         *   to retrieve a DbFile */
        size_t t;

        LogicalScanNode(size_t table, const string& tableAlias) {
            alias = tableAlias;
            t = table;
        }
	};
}