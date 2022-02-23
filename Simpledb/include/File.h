#pragma once
#include "Noncopyable.h"
#include<string>
#include<vector>
using namespace std;
namespace Simpledb {

	class File : public Noncopyable {
	public:
		File(const string& fileName,const string& mode = "ab+");
		//construct from tmpfile
		File(FILE* tmpFile);
		~File();
		FILE* originPtr() { return _pFile; }
		string fileName() { return _fileName; }
		int64_t length();
		int64_t position();
		void deleteOnExit() { _deleteOnExit = true; }
		void flush();
		void seek(int64_t pos);
		void writeChar(char c);
		void writeInt(int value);
		void writeInt64(int64_t value);
		void writeUTF8(const string& str);
		void writeBytes(const unsigned char* bytes, size_t byteCount);
		char readChar();
		int readInt();
		int64_t readInt64();
		string readUTF8();
		vector<unsigned char> readBytes(size_t readLen);
		void readFromTmpfile(FILE* tmpFile);
		void remove();
		//clear
		void reset();
		void close();
	private:
		template<typename T>
		T readBaseType();
		template<typename T>
		void writeBaseType(T value);

		string _fileName;
		string _mode;
		FILE* _pFile = nullptr;
		bool _deleteOnExit = false;
	};
	template<typename T>
	inline T File::readBaseType()
	{
		static const int sz = sizeof(T);
		char typeArr[sz] = { 0 };
		size_t rSz = fread(typeArr, 1, sz, _pFile);
		if (rSz != sz){
			throw "read data failed";
		}
		T result;
		errno_t err = memcpy_s(&result, sz, typeArr, sz);
		if (err != 0) {
			throw "read data failed";
		}
		return result;
	}
	template<typename T>
	inline void File::writeBaseType(T value)
	{
		static const int sz = sizeof(T);
		char typeArr[sz] = { 0 };
		errno_t err = memcpy_s(typeArr, sz, &value, sz);
		if (err != 0) {
			throw "write data failed";
		}

		size_t w = fwrite(typeArr, 1, sz, _pFile);
		if (w != sz) {
			throw "write data failed";
		}
	}
}