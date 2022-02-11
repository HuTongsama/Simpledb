#pragma once
#include<string>
#include<vector>
#include<memory>
using namespace std;
namespace Simpledb {
	using Unique_File = unique_ptr<FILE, decltype(&fclose)>;
	string trim(const string& str);
	vector<string> split(const string& str, const string& delim);
	bool equalsIgnoreCase(const string& str1, const string& str2);
	/*bool writeInt(int value, FILE* pStream);
	bool writeInt64(int64_t value, FILE* pStream);
	bool writeUTF8(const string& str,FILE* pStream);
	bool writeBytes(const unsigned char* bytes, size_t byteCount, FILE* pStream);
	bool readInt(int& ret, FILE* pStream);
	bool readInt64(int64_t& ret, FILE* pStream);
	bool readUTF8(string& str, FILE* pStream);
	bool readBytes(vector<unsigned char>& ret, size_t readLen, FILE* pStream);
	void readFromTmpfile(FILE* tmpfile, FILE* destFile);
	size_t FileLength(FILE* file);*/
}