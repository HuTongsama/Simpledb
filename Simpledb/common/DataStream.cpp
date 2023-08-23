#include "DataStream.h"
#include<string>
#include<stdexcept>
using namespace std;
namespace Simpledb {
	DataStream::DataStream()
		:_byteSize(1024), _curPosition(0)
	{
		_bytes = new char[_byteSize]();
	}
	DataStream::DataStream(const char* bytes, size_t byteSize)
		:_byteSize(byteSize), _curPosition(0)
	{
		_bytes = new char[_byteSize]();
		memcpy_s(_bytes, _byteSize, bytes, _byteSize);

	}
	DataStream::~DataStream()
	{
		release();
	}
	void DataStream::reset()
	{
		memset(_bytes, 0, _byteSize);
		_curPosition = 0;
	}
	void DataStream::seek(size_t pos)
	{
		if (pos >= 0 && pos < _byteSize) {
			_curPosition = pos;
		}
		else {
			throw out_of_range("error dataStream position");
		}
	}
	size_t DataStream::skipBytes(size_t skipSize)
	{
		size_t realSz = skipSize;
		if (_curPosition + skipSize >= _byteSize) {
			realSz = _byteSize - _curPosition;
		}
		_curPosition += realSz;
		return realSz;
	}
	size_t DataStream::read(char* dst, size_t dstSize)
	{
		readBytes(dst, dstSize);
		return dstSize;
	}
	size_t DataStream::write(char* src, size_t srcSize)
	{
		writeBytes(src, srcSize);
		return srcSize;
	}
	char DataStream::readChar()
	{
		return readCommonType<char>();
	}
	void DataStream::writeChar(char c)
	{
		writeCommonType(c);
	}
	double DataStream::readDouble()
	{	
		return readCommonType<double>();
	}
	void DataStream::writeDouble(double d)
	{
		writeCommonType(d);
	}
	float DataStream::readFloat()
	{
		return readCommonType<float>();
	}
	void DataStream::writeFloat(float f)
	{
		writeCommonType(f);
	}
	int DataStream::readInt()
	{
		return readCommonType<int>();
	}

	void DataStream::writeInt(int i)
	{
		writeCommonType(i);
	}

	std::vector<unsigned char> DataStream::toByteArray()
	{
		if (_byteSize == 0)
			return vector<unsigned char>();
		return vector<unsigned char>(_bytes, _bytes + _byteSize);
	}

	void DataStream::release()
	{
		if (_bytes != nullptr) {
			delete[]_bytes;
			_bytes = nullptr;
			_byteSize = 0;
			_curPosition = 0;
		}
	}
	void DataStream::readBytes(char* dst, size_t dstSize)
	{
		if (_curPosition + dstSize > _byteSize) {
			throw out_of_range("read dataStream failed");
		}
		else {
			memcpy_s(dst, dstSize, _bytes + _curPosition, dstSize);
			_curPosition += dstSize;
		}
	}
	void DataStream::writeBytes(char* src, size_t srcSize)
	{
		if (_curPosition + srcSize >= _byteSize) {
			size_t newSz = 2 * _byteSize;
			if (newSz < _byteSize) {
				throw out_of_range("write dataStream failed");
			}
			char* newBytes = new char[newSz]();
			memcpy_s(newBytes, newSz, _bytes, _byteSize);
			delete[]_bytes;
			_bytes = newBytes;
			_byteSize = newSz;
		}
		memcpy_s(_bytes + _curPosition, srcSize, src, srcSize);
		_curPosition += srcSize;
	}
}