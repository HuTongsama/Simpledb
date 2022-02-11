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

	//template<typename T>
	//bool writeBaseType(T value, FILE* pStream) {
	//	static const int sz = sizeof(T);
	//	char typeArr[sz] = { 0 };
	//	errno_t err = memcpy_s(typeArr, sz, &value, sz);
	//	if (err != 0)
	//		return false;

	//	size_t w = fwrite(typeArr, 1, sz, pStream);
	//	if (w != sz)
	//		return false;
	//	return true;
	//}
	//template<typename T>
	//bool readBaseType(T& ret, FILE* pStream) {
	//	static const int sz = sizeof(T);
	//	char typeArr[sz] = { 0 };
	//	size_t rSz = fread(typeArr, 1, sz, pStream);
	//	if (rSz != sz)
	//		return false;
	//	T result;
	//	errno_t err = memcpy_s(&result, sz, typeArr, sz);
	//	if (err != 0)
	//		return false;
	//	ret = result;
	//	return true;
	//}
	//bool writeInt(int value, FILE* pStream)
	//{
	//	return writeBaseType(value, pStream);
	//}

	//bool writeInt64(int64_t value, FILE* pStream)
	//{
	//	return writeBaseType(value, pStream);
	//}

	//bool writeUTF8(const string& str, FILE* pStream)
	//{
	//	size_t byteCount = str.length();
	//	writeInt64(byteCount, pStream);
	//	writeBytes((const unsigned char*)str.c_str(), byteCount, pStream);
	//	return false;
	//}

	//bool writeBytes(const unsigned char* bytes, size_t byteCount, FILE* pStream)
	//{
	//	size_t w = fwrite(bytes, 1, byteCount, pStream);
	//	if (w != byteCount)
	//		return false;
	//	return true;
	//}

	//bool readInt(int& ret, FILE* pStream)
	//{
	//	return readBaseType(ret, pStream);
	//}

	//bool readInt64(int64_t& ret, FILE* pStream)
	//{
	//	return readBaseType(ret,pStream);
	//}

	//bool readUTF8(string& str, FILE* pStream)
	//{
	//	int64_t readLen = 0;
	//	if (!readInt64(readLen, pStream))
	//		return false;
	//	vector<unsigned char> byteVec;
	//	if (!readBytes(byteVec, readLen, pStream))
	//		return false;
	//	string s(byteVec.begin(), byteVec.end());
	//	str = s;
	//	return true;
	//}

	//bool readBytes(vector<unsigned char>& ret, size_t readLen, FILE* pStream)
	//{
	//	unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned char) * readLen);
	//	if (buffer == nullptr)
	//		return false;
	//	size_t result = fread(buffer, 1, readLen, pStream);
	//	if (result != readLen)
	//	{
	//		free(buffer);
	//		return false;
	//	}
	//	ret = vector<unsigned char>(buffer, buffer + readLen);
	//	free(buffer);
	//	return true;
	//}

	//void readFromTmpfile(FILE* tmpfile, FILE* destFile)
	//{
	//	rewind(tmpfile);
	//	char buffer[256] = { 0 };
	//	while (!feof(tmpfile)) {
	//		size_t r = fread(buffer, 1, 256, tmpfile);
	//		fwrite(buffer, 1, r, destFile);
	//	}
	//}

	//size_t FileLength(FILE* file)
	//{
	//	size_t oldPos = ftell(file);
	//	fseek(file, 0, SEEK_END);
	//	size_t len = ftell(file);
	//	fseek(file, oldPos, SEEK_SET);
	//	return len;
	//}

}
