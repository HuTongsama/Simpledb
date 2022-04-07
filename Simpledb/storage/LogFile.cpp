#include"LogFile.h"
#include"Database.h"
#include"PageFactory.h"
#include"PageIdFactory.h"
#include<chrono>
#include<ctime>
#pragma warning(disable:4996)
namespace Simpledb {

    const int LogFile::ABORT_RECORD = 1;
    const int LogFile::COMMIT_RECORD = 2;
    const int LogFile::UPDATE_RECORD = 3;
    const int LogFile::BEGIN_RECORD = 4;
    const int LogFile::CHECKPOINT_RECORD = 5;
    const int64_t LogFile::NO_CHECKPOINT_ID = -1;

    const int LogFile::INT_SIZE = 4;
    const int LogFile::LONG_SIZE = 8;
    LogFile::LogFile(const string& fileName) :
        _logFile(fileName),
        _recoveryUndecided(true),
        _currentOffset(-1),
        _totalRecords(0) {

    }
    int LogFile::getTotalRecords()
    {
        lock_guard<mutex> lock(_logFileMutex);
        return _totalRecords;
    }
    void LogFile::logAbort(const TransactionId& tid)
    {
        // must have buffer pool lock before proceeding, since this
        // calls rollback
        Database::getBufferPool()->lockBufferPool();
        lock_guard<mutex> lock(_logFileMutex);
        preAppend();
        //Debug.log("ABORT");
        //should we verify that this is a live transaction?

        // must do this here, since rollback only works for
        // live transactions (needs tidToFirstLogRecord)
        
        rollback(tid);
        _logFile.writeInt(ABORT_RECORD);
        _logFile.writeInt64(tid.getId());
        _logFile.writeInt64(_currentOffset);
        _currentOffset = _logFile.position();
        force();
        _tidToFirstLogRecord.erase(tid.getId());
        Database::getBufferPool()->unlockBufferPool();
    }
    
    void LogFile::logCommit(const TransactionId& tid)
    {
        lock_guard<mutex> lock(_logFileMutex);
        preAppend();
        //Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        _logFile.writeInt(COMMIT_RECORD);
        _logFile.writeInt64(tid.getId());
        _logFile.writeInt64(_currentOffset);
        _currentOffset = _logFile.position();
        force();
        _tidToFirstLogRecord.erase(tid.getId());
    }
    void LogFile::logWrite(const TransactionId tid, shared_ptr<Page> before, shared_ptr<Page> after)
    {
        lock_guard<mutex> lock(_logFileMutex);
        //Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        _logFile.writeInt(UPDATE_RECORD);
        _logFile.writeInt64(tid.getId());
        

        writePageData(_logFile, before);
        writePageData(_logFile, after);
        _logFile.writeInt64(_currentOffset);
        _currentOffset = _logFile.position();

        //Debug.log("WRITE OFFSET = " + currentOffset);
    }
    void LogFile::logXactionBegin(const TransactionId& tid)
    {
        //Debug.log("BEGIN");
        /*if (tidToFirstLogRecord.get(tid.getId()) != null) {
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }*/
        lock_guard<mutex> lock(_logFileMutex);
        if (_tidToFirstLogRecord.find(tid.getId()) == _tidToFirstLogRecord.end()) {
            return;
        }
        preAppend();
        _logFile.writeInt(BEGIN_RECORD);
        _logFile.writeInt64(tid.getId());
        _logFile.writeInt64(_currentOffset);
        _tidToFirstLogRecord[tid.getId()] = _currentOffset;
        _currentOffset = _logFile.position();

        //Debug.log("BEGIN OFFSET = " + currentOffset);
    }
    void LogFile::logCheckpoint()
    {
        Database::getBufferPool()->lockBufferPool();
        {
            lock_guard<mutex> lock(_logFileMutex);
            //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
            preAppend();            
            force();
            Database::getBufferPool()->flushAllPages();
            int64_t startCpOffset = 0, endCpOffset = 0;
            startCpOffset = _logFile.position();
            _logFile.writeInt(CHECKPOINT_RECORD);
            _logFile.writeInt64(-1);//no tid , but leave space for convenience
            _logFile.writeInt((int)_tidToFirstLogRecord.size());
            
            for (auto iter = _tidToFirstLogRecord.begin();
                iter != _tidToFirstLogRecord.end(); ++iter) {
                int64_t key = iter->first;
                int64_t val = iter->second;
                //Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                _logFile.writeInt64(key);
                _logFile.writeInt64(val);
            }
            //once the CP is written, make sure the CP location at the
            // beginning of the log file is updated
            endCpOffset = _logFile.position();
            _logFile.seek(0);
            _logFile.writeInt64(startCpOffset);
            _logFile.seek(endCpOffset);
            _logFile.writeInt64(endCpOffset);
            _currentOffset = _logFile.position();
            
            //Debug.log("CP OFFSET = " + currentOffset);
        }
        Database::getBufferPool()->unlockBufferPool();
       
        logTruncate();
    }
    void LogFile::logTruncate()
    {
        lock_guard<mutex> lock(_logFileMutex);
        preAppend();
        _logFile.seek(0);
        int64_t cpLoc = _logFile.readInt64();
        int64_t minLogRecord = cpLoc;
        if (cpLoc != -1) {
            _logFile.seek(cpLoc);
            int cpType = _logFile.readInt();
            int64_t cpTid = _logFile.readInt64();
            

            if (cpType != CHECKPOINT_RECORD) {
                cout << "Checkpoint pointer does not point to checkpoint record" << endl;
            }

            int numOutstanding = _logFile.readInt();
            for (int i = 0; i < numOutstanding; i++) {
                int64_t tid = _logFile.readInt64();
                int64_t firstLogRecord = _logFile.readInt64();
                
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }
        time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
        string newLogName("logtmp");
        newLogName += ctime(&time);
        //FILE* logNew = fopen(newLogName.c_str(), "ab+");
        File logNew(newLogName);
        logNew.seek(0);
        logNew.writeInt64((cpLoc - minLogRecord) + LONG_SIZE);
        // we can truncate everything before minLogRecord
        logNew.seek(minLogRecord);
        
        
        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try
            {
                int type = _logFile.readInt();
                int64_t record_tid = _logFile.readInt64();
                //Debug.log("NEW START = " + newStart);
                logNew.writeInt(type);
                logNew.writeInt64(record_tid);
                int64_t newStart = logNew.position();
                switch (type)
                {
                case UPDATE_RECORD:
                {
                    auto before = readPageData(_logFile);
                    auto after = readPageData(_logFile);
                    writePageData(logNew, before);
                    writePageData(logNew, after); 
                }
                break;
                case CHECKPOINT_RECORD:
                {
                    int numXactions = _logFile.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions > 0) {
                        int64_t xid = _logFile.readInt64();
                        int64_t xoffset = _logFile.readInt64();

                        logNew.writeInt64(xid);
                        logNew.writeInt64((xoffset - minLogRecord) + LONG_SIZE);
                        numXactions--;
                    }
                }
                break;
                case BEGIN_RECORD:
                {
                    _tidToFirstLogRecord[record_tid] = newStart;
                }
                break;
                default:
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeInt64(newStart);
                _logFile.readInt64();
            }
            catch (const std::exception& e)
            {
                break;
            }
        }

        //Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        _logFile.reset();
        _logFile.readFromTmpfile(logNew.originPtr());
        _logFile.seek(_logFile.length());
        _currentOffset = _logFile.position();
    }
    void LogFile::rollback(const TransactionId& tid)
    {
        Database::getBufferPool()->lockBufferPool();
        {
            lock_guard<mutex> lock(_logFileMutex);
            preAppend();
            // some code goes here
        }
        Database::getBufferPool()->unlockBufferPool();
    }
    void LogFile::shutdown()
    {
        lock_guard<mutex> lock(_logFileMutex);
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            _logFile.close();
        }
        catch (const std::exception& e) {
            cout << e.what() << endl;
        }
    }
    void LogFile::recover()
    {
        Database::getBufferPool()->lockBufferPool();
        {
            lock_guard<mutex> lock(_logFileMutex);
            _recoveryUndecided = false;
            // some code goes here
        }
        Database::getBufferPool()->unlockBufferPool();
    }
    void LogFile::print()
    {
        int64_t curOffset = _logFile.position();

        _logFile.seek(0);
        cout << "0: checkpoint record at offset " << _logFile.readInt64() << endl;      

        while (true) {
            try {
                int cpType = _logFile.readInt();
                int64_t cpTid = _logFile.readInt64();
                cout << (_logFile.position() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " << cpType << endl;
                cout << (_logFile.position() - LONG_SIZE) + ": TID " << cpTid << endl;
                
                switch (cpType) {
                case BEGIN_RECORD:
                    cout << " (BEGIN)" << endl;
                    cout << _logFile.position() << ": RECORD START OFFSET: " << _logFile.readInt64() << endl;
                    break;
                case ABORT_RECORD:
                    cout << " (ABORT)" << endl;
                    cout << _logFile.position() << ": RECORD START OFFSET: " << _logFile.readInt64() << endl;                  
                    break;
                case COMMIT_RECORD:
                    cout << " (COMMIT)" << endl;
                    cout << _logFile.position() + ": RECORD START OFFSET: " << _logFile.readInt64() << endl;
                    break;

                case CHECKPOINT_RECORD:
                {
                    cout << " (CHECKPOINT)" << endl;
                    int numTransactions = _logFile.readInt();
                    cout << (_logFile.position() - INT_SIZE) << ": NUMBER OF OUTSTANDING RECORDS: " << numTransactions << endl;

                    while (numTransactions > 0) {
                        int64_t tid = _logFile.readInt64();
                        int64_t firstRecord = _logFile.readInt64();
                        cout << (_logFile.position() - (LONG_SIZE + LONG_SIZE)) << ": TID: " << tid << endl;
                        cout << (_logFile.position() - LONG_SIZE) << ": FIRST LOG RECORD: " << firstRecord << endl;
                        numTransactions--;
                    }
                    cout << _logFile.position() << ": RECORD START OFFSET: " << _logFile.readInt64() << endl; 
                }
                    break;
                case UPDATE_RECORD:
                    cout << (" (UPDATE)") << endl;

                    int64_t start = _logFile.position();
                    auto before = readPageData(_logFile);

                    int64_t middle = _logFile.position();
                    auto after = readPageData(_logFile);

                    cout << start << ": before image table id " << before->getId()->getTableId() << endl;
                    cout << (start + INT_SIZE) << ": before image page number " << before->getId()->getPageNumber() << endl;
                    cout << (start + INT_SIZE) << " TO " << (middle - INT_SIZE) << ": page data" << endl;
                    
                    cout << middle << ": after image table id " << after->getId()->getTableId() << endl;
                    cout << (middle + INT_SIZE) << ": after image page number " << after->getId()->getPageNumber() << endl;
                    cout << (middle + INT_SIZE) << " TO " << (_logFile.position()) << ": page data" << endl;
                    
                    cout << _logFile.position() << ": RECORD START OFFSET: " << _logFile.readInt64() << endl;
                    break;
                }

            }
            catch (const std::exception& e) {
                cout << e.what() << endl;
                break;
            }
        }
        // Return the file pointer to its original position
        _logFile.seek(curOffset);
    }
    void LogFile::force()
    {
    }
    void LogFile::preAppend()
    {
        _totalRecords++;
        if (_recoveryUndecided) {
            _recoveryUndecided = false;
            _logFile.seek(0);
            _logFile.writeInt64(NO_CHECKPOINT_ID);
            _currentOffset = _logFile.position();
        }
    }
    void LogFile::writePageData(File& f, shared_ptr<Page> p)
    {
        shared_ptr<PageId> pid = p->getId();
        vector<size_t> pageInfo = pid->serialize();
        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data
        string pageClassName(typeid(*p).name());
        string idClassName(typeid(*pid).name());
        
        f.writeUTF8(pageClassName);
        f.writeUTF8(idClassName);
        f.writeInt64(pageInfo.size());
        for (size_t j : pageInfo) {
            f.writeInt64(j);
        }
        vector<unsigned char> pageData = p->getPageData();
        f.writeInt64(pageData.size());
        f.writeBytes(pageData.data(), pageData.size());
        //Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }
    shared_ptr<Page> LogFile::readPageData(File& f)
    {
        string pageClassName = f.readUTF8();
        string idClassName = f.readUTF8();
        shared_ptr<Page> page = nullptr;
        try
        {
            shared_ptr<PageId> pid = PageIdFactory::CreatePageId(idClassName, f);
            page = PageFactory::CreatePage(pageClassName, pid, f);
            return page;
        }
        catch (const std::exception& e)
        {
            return nullptr;
        }
        
    }
}