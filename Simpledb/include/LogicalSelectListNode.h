#pragma once
#include<string>
using namespace std;
namespace Simpledb {
	/** A LogicalSelectListNode represents a clause in the select list in
	 * a LogicalQueryPlan
	 */
	class LogicalSelectListNode {
	public:
        /** The field name being selected; the name may be (optionally) be
         * qualified with a table name or alias.
         */
        string fname;
        /** The aggregation operation over the field (if any) */
        string aggOp;

        LogicalSelectListNode(const string& aggOp, const string& fname) {
            this->aggOp = aggOp;
            this->fname = fname;
        }
	};
}