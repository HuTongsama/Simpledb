#include "pch.h"
#include "TestUtil.h"
#include <fstream>
using namespace std;
vector<unsigned char> TestUtil::readFileBytes(const string& path)
{
    ifstream ifs(path);
    const int bufLen = 512;
    char buf[bufLen] = { 0 };
    size_t readLen = 0;
    vector<unsigned char> result;
    while ((readLen = ifs.readsome(buf, bufLen)) > 0) {
        result.insert(result.end(), buf, buf + readLen);
    }
    return result;
}
