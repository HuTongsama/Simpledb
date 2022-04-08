#include"HeapFileEncoder.h"
#include"Common.h"
#pragma warning(disable:4996)
namespace Simpledb {
    bool HeapFileEncoder::convert(const vector<vector<int>>& tuples, File& outFile, int npagebytes, int numFields)
    {
        File inFile(tmpfile());
        const int bufSize = 128;
        char buf[bufSize] = { 0 };
        for (auto& tuple : tuples){
            int writtenFields = 0;
            for (int field : tuple) {
                writtenFields++;
                if (writtenFields > numFields) {
                    return false;
                }
                string tmp(itoa(field, buf, 10));
                inFile.writeBytes((const unsigned char*)tmp.c_str(), tmp.size());
                if (writtenFields < numFields) {
                    inFile.writeChar(',');
                }
            }
            inFile.writeChar('\n');
        }
        inFile.seek(0);
        return convert(inFile, outFile, npagebytes, numFields);
    }
    bool HeapFileEncoder::convert(File& inFile, File& outFile, int npagebytes, int numFields)
    {
        vector<shared_ptr<Type>> typeVec(numFields, Int_Type::INT_TYPE());
        return convert(inFile, outFile, npagebytes, numFields, typeVec);
    }
    bool HeapFileEncoder::convert(File& inFile, File& outFile, int npagebytes, int numFields,
        const vector<shared_ptr<Type>>& typeVec)
    {
        return convert(inFile, outFile, npagebytes, numFields, typeVec, ',');
    }
    bool HeapFileEncoder::convert(File& inFile, File& outFile, int npagebytes,
		int numFields, const vector<shared_ptr<Type>>& typeVec, char fieldSeparator)
	{
        int nrecbytes = 0;
        size_t typeSz = typeVec.size();
        for (int i = 0; i < numFields && i < typeSz; i++) {
            if (typeVec[i] == nullptr)
                return false;
            nrecbytes += typeVec[i]->getLen();
        }
        int nrecords = (npagebytes * 8) / (nrecbytes * 8 + 1);  //floor comes for free

        //  per record, we need one bit; there are nrecords per page, so we need
        // nrecords bits, i.e., ((nrecords/32)+1) integers.
        int nheaderbytes = (nrecords / 8);
        if (nheaderbytes * 8 < nrecords)
            nheaderbytes++;  //ceiling
        int nheaderbits = nheaderbytes * 8;

        //BufferedReader br = new BufferedReader(new FileReader(inFile));
        //FileOutputStream os = new FileOutputStream(outFile);

        // our numbers probably won't be much larger than 1024 digits
        char buf[1024] = {0};
        int curpos = 0;
        int recordcount = 0;
        int npages = 0;
        int fieldNo = 0;

        bool done = false;
        bool first = true;
        //FILE* pHeaderStream = tmpfile();
        //FILE* pPageStream = tmpfile();
        File pHeader(tmpfile());
        File pPage(tmpfile());
        size_t fLength = inFile.length();
        try
        {
            while (!done) {
                if (fLength == inFile.position()) {
                    done = true;
                }
                int c = inFile.readChar();
                if (c == '\r')
                    continue;
                if (c == '\n') {
                    if (first)
                        continue;
                    recordcount++;
                    first = true;
                }
                else
                    first = false;

                if (c == fieldSeparator || c == '\n' || c == '\r') {
                    string s(buf, curpos);
                    if (typeVec[fieldNo].get() == Int_Type::INT_TYPE().get()) {
                        string tmp = trim(s);
                        int num = atoi(tmp.c_str());
                        pPage.writeInt(num);
                    }
                    else if (typeVec[fieldNo].get() == String_Type::STRING_TYPE().get()) {
                        string tmp = trim(s);
                        int overflow = static_cast<int>(Type::STRING_LEN - tmp.length());
                        if (overflow < 0) {
                            tmp = tmp.substr(0, Type::STRING_LEN);
                        }
                        pPage.writeInt((int)s.length());
                        pPage.writeBytes((const unsigned char*)s.c_str(), s.length());

                        while (overflow > 0) {
                            pPage.writeChar('\0');
                            overflow--;
                        };
                    }

                    curpos = 0;
                    if (c == '\n')
                        fieldNo = 0;
                    else
                        fieldNo++;
                }
                else if(!done) {
                    buf[curpos] = (char)c;
                    curpos++;
                    continue;
                }
                // if we wrote a full page of records, or if we're done altogether,
                // write out the header of the page.
                //
                // in the header, write a 1 for bits that correspond to records we've
                // written and 0 for empty slots.
                //
                // when we're done, also flush the page to disk, but only if it has
                // records on it.  however, if this file is empty, do flush an empty
                // page to disk.
                if (recordcount >= nrecords
                    || done && recordcount > 0
                    || done && npages == 0) {
                    int i = 0;
                    char headerbyte = 0;

                    for (i = 0; i < nheaderbits; i++) {
                        if (i < recordcount)
                            headerbyte |= (1 << (i % 8));

                        if (((i + 1) % 8) == 0) {
                            pHeader.writeChar(headerbyte);
                            headerbyte = 0;
                        }
                    }
                    if (i % 8 > 0)
                        pHeader.writeChar(headerbyte);

                    // pad the rest of the page with zeroes

                    for (i = 0; i < (npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
                        pPage.writeChar('\0');

                    // write header and body to file
                    pHeader.flush();
                    outFile.readFromTmpfile(pHeader.originPtr());
                    pPage.flush();
                    outFile.readFromTmpfile(pPage.originPtr());


                    // reset header and body for next page
                    pHeader.reset();
                    pPage.reset();
                    recordcount = 0;
                    npages++;
                }
            }
            outFile.seek(0);      
            return true;
        }
        catch (const std::exception)
        {
            return false;
        }
        
	}
}