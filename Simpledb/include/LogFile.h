#pragma once
#include"Common.h"
#include"TransactionId.h"
#include"Page.h"
#include"File.h"
#include<map>
#include<mutex>
namespace Simpledb {

	class LogFile {
	public:
        /** Constructor.
            Initialize and back the log file with the specified file.
            We're not sure yet whether the caller is creating a brand new DB,
            in which case we should ignore the log file, or whether the caller
            will eventually want to recover (after populating the Catalog).
            So we make this decision lazily: if someone calls recover(), then
            do it, while if someone starts adding log file entries, then first
            throw out the initial log file contents.

            @param f The log file's name
        */
        LogFile(const string& fileName);
        int getTotalRecords();
        /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
        */
        void logAbort(const TransactionId& tid);
        /** Write a commit record to disk for the specified tid,
            and force the log to disk.
            @param tid The committing transaction.
        */
        void logCommit(const TransactionId& tid);
        /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page
        @see Page#getBeforeImage
        */
        void logWrite(const TransactionId tid, shared_ptr<Page> before, shared_ptr<Page> after);
        /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

        */
        void logXactionBegin(const TransactionId& tid);
        /** Checkpoint the log and write a checkpoint record. */
        void logCheckpoint();
        /** Truncate any unneeded portion of the log to reduce its space
        consumption */
        void logTruncate();
        /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
        */
        void rollback(const TransactionId& tid);
        /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
        */
        void shutdown();
        /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
        */
        void recover();
        /** Print out a human readable represenation of the log */
        void print();
        void force();
	private:
        // we're about to append a log record. if we weren't sure whether the
        // DB wants to do recovery, we're sure now -- it didn't. So truncate
        // the log.
        void preAppend();
        void writePageData(File& f, shared_ptr<Page> p);
        shared_ptr<Page> readPageData(File& f);
        



        File _logFile;
        bool _recoveryUndecided; // no call to recover() and no append to log

        static const int ABORT_RECORD;
        static const int COMMIT_RECORD;
        static const int UPDATE_RECORD;
        static const int BEGIN_RECORD;
        static const int CHECKPOINT_RECORD;
        static const int64_t NO_CHECKPOINT_ID;

        static const int INT_SIZE;
        static const int LONG_SIZE;

        int64_t _currentOffset;//protected by this
        int _totalRecords; // for PatchTest //protected by this
        map<int64_t, int64_t> _tidToFirstLogRecord;
        mutex _logFileMutex;
	};
}