#pragma once
#include"sqlParser/SQLParser.h"

#include<string>
#include"Query.h"
#include"Transaction.h"
#ifdef WIN32
#ifdef _DEBUG
#pragma comment(lib,"sql-parser-vc143-mt-gd-x32.lib")
#else
#pragma comment(lib,"sql-parser-vc143-mt-x32.lib")
#endif // _DEBUG
#else
#ifdef _DEBUG
#pragma comment(lib,"sql-parser-vc143-mt-gd-x64.lib")
#else
#pragma comment(lib,"sql-parser-vc143-mt-x64.lib")
#endif // _DEBUG
#endif // WIN32



using namespace std;
namespace Simpledb {
	class Parser {
	public:
		static bool explain;
		static Predicate::Op getOp(hsql::OperatorType type);

		void processExpression(shared_ptr<TransactionId> tid, hsql::Expr* expr, shared_ptr<LogicalPlan> lp);
		shared_ptr<LogicalPlan> parseQueryLogicalPlan(shared_ptr<TransactionId> tid, const hsql::SelectStatement* select);
		shared_ptr<Query> handleQueryStatement(const hsql::SelectStatement* s, shared_ptr<TransactionId> tId);
		shared_ptr<Query> handleInsertStatement(const hsql::InsertStatement* s, shared_ptr<TransactionId> tId);
		shared_ptr<Query> handleDeleteStatement(const hsql::DeleteStatement* s, shared_ptr<TransactionId> tid);
		void handleTransactStatement(const hsql::TransactionStatement* s);
		shared_ptr<LogicalPlan> generateLogicalPlan(shared_ptr<TransactionId> tid, const string& s);
		void processNextStatement(const string& strQuery);
		void setTransaction(shared_ptr<Transaction> t) {
			_curtrans = t;
		}
		shared_ptr<Transaction> getTransaction() {
			return _curtrans;
		}
	private:
		static bool isConstant(hsql::ExprType exprType);
		shared_ptr<Transaction> _curtrans = nullptr;
		bool _inUserTrans = false;
		string _curQueryStr;
	};
}