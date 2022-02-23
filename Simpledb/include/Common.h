#pragma once
#include<string>
#include<vector>
#include<memory>
#include<sstream>
using namespace std;
namespace Simpledb {
	using Unique_File = unique_ptr<FILE, decltype(&fclose)>;
	string trim(const string& str);
	vector<string> split(const string& str, const string& delim);
	bool equalsIgnoreCase(const string& str1, const string& str2);
	template<typename T>
	string combineArgs(stringstream& ss, const string& delim, const T& arg) {
		ss << arg;
		return ss.str();
	}
	template<typename T,typename ... Args>
	string combineArgs(stringstream& ss, const string& delim, const T& arg1, const Args& ... rest) {
		ss << arg1 << delim;
		return combineArgs(ss, delim, rest...);
	}

}