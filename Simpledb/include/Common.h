#pragma once
#include<string>
#include<vector>
#include<memory>
#include<sstream>
#include<type_traits>
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
	
	template<typename T ,
		typename = typename enable_if<
		is_same<T,int>::value ||
		is_same<T,float>::value ||
		is_same<T,double>::value
		,T>::type>
	vector<unsigned char> serializeCommonType(T value) {
		/*static_assert(
			is_same<T, int>::value ||
			is_same<T, float>::value ||
			is_same<T, double>::value,
			"error serializeCommonType");*/
		static size_t sz = sizeof(value);
		unsigned char buffer[128];
		memcpy_s(buffer, sz, &value, sz);
		return vector<unsigned char>(buffer, buffer + sz);
	}
}