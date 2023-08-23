#include"Query.h"
namespace Simpledb {
	void Query::start()
	{
		_op->open();
		_started = true;
	}
	Tuple* Query::next()
	{
		if (!_started)
			throw runtime_error("Database not started.");

		return _op->next();
	}
	void Query::execute()
	{
        shared_ptr<TupleDesc> td = getOutputTupleDesc();

        string names = "";
        size_t fields = td->numFields();
        for (int i = 0; i < fields; i++) {
            names += td->getFieldName(i) + "\t";
        }
        cout << names << endl;
        size_t sz = names.size() + fields * 4;
        for (int i = 0; i < sz; i++) {
            cout<<("-");
        }
        cout << endl;;

        start();
        int cnt = 0;
        while (hasNext()) {
            Tuple* tup = next();
            cout << tup->toString() << endl;
            cnt++;
        }
        cout << ("\n " + to_string(cnt) + " rows.") << endl;
        close();
	}
}