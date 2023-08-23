#include "Common.h"

namespace Simpledb {
	string trim(const string& str)
	{
		const static char* typeOfWhitespaces = " \t\n\r\f\v";
		size_t start = str.find_first_not_of(typeOfWhitespaces);
		size_t end = str.find_last_not_of(typeOfWhitespaces);
		string result = "";
		if (start != string::npos
			&& end != string::npos)
		{
			size_t len = end - start + 1;
			result = str.substr(start, len);
		}
		return result;
	}

	vector<string> split(const string& str, const string& delim)
	{
		vector<string> result;
		size_t found = str.find_first_of(delim);
		string tmp = str;
		size_t sz = delim.length();
		while (found != string::npos) {
			string s = tmp.substr(0, found);
			result.push_back(s);
			tmp = tmp.substr(found + sz);
			found = tmp.find_first_of(delim);
		}
		result.push_back(tmp);
		return result;
	}

	bool equalsIgnoreCase(const string& str1, const string& str2)
	{
		string tmp1, tmp2;
		for (auto c : str1)
			tmp1 += tolower(c);
		for (auto c : str2)
			tmp2 += tolower(c);
		return tmp1 == tmp2;
	}

	void Thread::run()
	{
        {
            unique_lock<mutex> lock(_joinMutex);
            _isAlive = true;
            _isCompleted = false;
        }
		runInner();
        unique_lock<mutex> lock(_joinMutex);
		_isCompleted = true;
        _isAlive = false;
        _joinCond.notify_all();
	}

	bool Thread::isCompleted()
	{
		return _isCompleted;
	}

	const string& Thread::exception()
	{
		return _error;
	}

	bool Thread::isAlive()
	{
		return _isAlive;
	}

	void Thread::start()
	{
		thread t(&Thread::run, this);
		this_thread::sleep_for(chrono::milliseconds(500));
		t.detach();
	}

	void Thread::join(int milliseconds)
	{
		if (milliseconds < 0)
			throw runtime_error("error join parameter");
		unique_lock<mutex> lock(_joinMutex);
		if (_isAlive) {
			if (milliseconds == 0) {
				_joinCond.wait(lock);
			}
			else {
				_joinCond.wait_for(lock, std::chrono::milliseconds(milliseconds));
			}
		}
	}


}
