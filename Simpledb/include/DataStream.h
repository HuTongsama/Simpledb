#pragma once
#include"Noncopyable.h"
#include<vector>
namespace Simpledb {
	class DataStream : public Noncopyable {
	public:
		DataStream(size_t byteSize = 1024);
		DataStream(const char* bytes, size_t byteSize);
		~DataStream();
		void reset();
		void seek(size_t pos);
		size_t skipBytes(size_t skipSize);
		size_t read(char* dst, size_t dstSize);
		size_t write(char* src, size_t srcSize);
		char readChar();
		void writeChar(char c);
		double readDouble();
		void writeDouble(double d);
		float readFloat();
		void writeFloat(float f);
		int readInt();
		void writeInt(int i);
		std::vector<unsigned char> toByteArray();
	private:
		void release();
		template<typename T>
		T readCommonType();
		void readBytes(char* dst, size_t dstSize);
		template<typename T>
		void writeCommonType(T value);
		void writeBytes(char* src, size_t srcSize);

		char* _bytes;
		size_t _byteSize;
		size_t _curPosition;
	};
	template<typename T>
	inline T DataStream::readCommonType()
	{
		T t;
		static size_t sz = sizeof(T);
		readBytes((char *) &t, sz);
		return t;
	}
	template<typename T>
	inline void DataStream::writeCommonType(T value)
	{
		static size_t sz = sizeof(T);
		writeBytes((char*) &value, sz);
	}
}
