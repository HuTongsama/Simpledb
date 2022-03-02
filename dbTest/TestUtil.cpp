#include "pch.h"
#include "TestUtil.h"
#include <fstream>
using namespace std;
vector<unsigned char> TestUtil::readFileBytes(const string& path)
{
    ifstream ifs(path, std::ifstream::binary);
    const int bufLen = 512;
    char buf[bufLen] = { 0 };
    
    vector<unsigned char> result;
    while (ifs.read(buf,bufLen)) {
        result.insert(result.end(), buf, buf + bufLen);
    }
    size_t readLen = ifs.gcount();
    if (readLen > 0) {
        result.insert(result.end(), buf, buf + readLen);
    }
    return result;
}
