#include "File.h"
#include<stdexcept>
#pragma warning(disable:4996)
namespace Simpledb {
	File::File(const string& fileName)
	{
		_fileName = fileName;
		_mode = "rb+";
		_pFile = fopen(fileName.c_str(), _mode.c_str());
		if (_pFile == nullptr) {
			_pFile = fopen(fileName.c_str(), "w");
			fclose(_pFile);
			_pFile = fopen(fileName.c_str(), _mode.c_str());
		}
	}
	File::File(FILE* tmpFile)
	{
		_pFile = tmpFile;
		_fileName = "";
		_mode = "";
	}
	File::~File()
	{
		if (_pFile != nullptr) {
			fclose(_pFile);
		}
	}
	size_t File::length()
	{
		lock_guard<mutex> lock(_fileMutex);
		size_t oldPos = ftell(_pFile);
		fseek(_pFile, 0, SEEK_END);
		size_t len = ftell(_pFile);
		fseek(_pFile, oldPos, SEEK_SET);
		return len;
	}
	size_t File::position()
	{
		lock_guard<mutex> lock(_fileMutex);
		return ftell(_pFile);
	}
	void File::flush()
	{
		lock_guard<mutex> lock(_fileMutex);
		fflush(_pFile);
	}
	void File::seek(size_t pos)
	{
		lock_guard<mutex> lock(_fileMutex);
		fseek(_pFile, pos, SEEK_SET);
	}
	void File::writeChar(char c)
	{
		lock_guard<mutex> lock(_fileMutex);
		writeBaseType(c);
	}
	void File::writeInt(int value)
	{
		lock_guard<mutex> lock(_fileMutex);
		writeBaseType(value);
	}
	void File::writeInt64(int64_t value)
	{
		lock_guard<mutex> lock(_fileMutex);
		writeBaseType(value);
	}
	void File::writeUTF8(const string& str)
	{
		lock_guard<mutex> lock(_fileMutex);
		size_t byteCount = str.length();
		writeInt64(byteCount);
		writeBytes((const unsigned char*)str.c_str(), byteCount);
	}
	void File::writeBytes(const unsigned char* bytes, size_t byteCount)
	{
		lock_guard<mutex> lock(_fileMutex);
		size_t w = fwrite(bytes, 1, byteCount, _pFile);		
	}
	char File::readChar()
	{
		lock_guard<mutex> lock(_fileMutex);
		return readBaseType<char>();
	}
	int File::readInt()
	{
		lock_guard<mutex> lock(_fileMutex);
		return readBaseType<int>();
	}
	int64_t File::readInt64()
	{
		lock_guard<mutex> lock(_fileMutex);
		return readBaseType<int64_t>();
	}
	string File::readUTF8()
	{
		lock_guard<mutex> lock(_fileMutex);
		int64_t readLen = readInt64();
		vector<unsigned char> byteVec = readBytes(readLen);
		string s(byteVec.begin(), byteVec.end());
		return s;
	}
	vector<unsigned char> File::readBytes(size_t readLen)
	{
		lock_guard<mutex> lock(_fileMutex);
		unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned char) * readLen);
		if (buffer == nullptr) {
			return vector<unsigned char>();
		}
		size_t result = fread(buffer, 1, readLen, _pFile);
		if (result != readLen)
		{
			free(buffer);
			return vector<unsigned char>();
		}
		vector<unsigned char> ret = vector<unsigned char>(buffer, buffer + readLen);
		free(buffer);
		return ret;
	}
	void File::readFromTmpfile(FILE* tmpFile)
	{
		size_t oldPos = ftell(tmpFile);
		rewind(tmpFile);
		char buffer[256] = { 0 };
		while (!feof(tmpFile)) {
			size_t r = fread(buffer, 1, 256, tmpFile);
			fwrite(buffer, 1, r, _pFile);
		}
		fseek(tmpFile, oldPos, SEEK_SET);
	}
	void File::remove()
	{
		fclose(_pFile);
		if (_fileName != "")
			std::remove(_fileName.c_str());
	}
	void File::reset()
	{
		fclose(_pFile);
		if (_fileName != "") {
			_pFile = fopen(_fileName.c_str(), "wb");
			fclose(_pFile);
			_pFile = fopen(_fileName.c_str(), _mode.c_str());
		}
		else {
			_pFile = tmpfile();
		}
	}
	void File::close()
	{
		fclose(_pFile);
	}
	shared_ptr<File> File::createTempFile()
	{
		char nameBuf[L_tmpnam_s] = { 0 };
		tmpnam_s(nameBuf);
		shared_ptr<File> temp = make_shared<File>(nameBuf);
		return temp;
	}
}

