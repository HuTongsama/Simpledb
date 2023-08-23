#pragma once
#include"DbFile.h"
#include"IntField.h"
#include"StringField.h"
#include"OpIterator.h"
#include"TupleIterator.h"
#include"Utility.h"
#include"HeapFile.h"
#include"File.h"
#include"Permissions.h"
#include"Database.h"
#include<thread>
#include<condition_variable>
#include<mutex>
#include<functional>
#include<Windows.h>
#include<processthreadsapi.h>

using namespace Simpledb;
class TestUtil {
public:
    /**
     * @return an IntField with value n
     */
    static shared_ptr<Field> getField(int n) {
        return make_shared<IntField>(n);
    }

    /**
    * @return a OpIterator over a list of tuples constructed over the data
    *   provided in the constructor. This iterator is already open.
    * @param width the number of fields in each tuple
    * @param tupdata an array such that the ith element the jth tuple lives
    *   in slot j * width + i
    * @require tupdata.length % width == 0
    */
    static shared_ptr<TupleIterator> createTupleList(int width, vector<int>& tupdata) {
        int i = 0;
        vector<shared_ptr<Tuple>> tupVec;
        while (i < tupdata.size()) {
            shared_ptr<Tuple> tup = make_shared<Tuple>(Utility::getTupleDesc(width));
            for (int j = 0; j < width; ++j)
                tup->setField(j, getField(tupdata[i++]));
            tupVec.push_back(tup);
        }

        shared_ptr<TupleIterator> result = make_shared<TupleIterator>(Utility::getTupleDesc(width), tupVec);
        result->open();
        return result;
    }
    struct TupData {
        shared_ptr<Type> _type;
        string _str;
        int _int;
        TupData(shared_ptr<Type> t, const string& s = "", int i = 0)
            :_type(t),_str(s),_int(i)
        {}
    };
    /**
     * @return a OpIterator over a list of tuples constructed over the data
     *   provided in the constructor. This iterator is already open.
     * @param width the number of fields in each tuple
     * @param tupdata an array such that the ith element the jth tuple lives
     *   in slot j * width + i.  Objects can be strings or ints;  tuples must all be of same type.
     * @require tupdata.length % width == 0
     */
    static shared_ptr<TupleIterator> createTupleList(int width, vector<TupData>& tupdata) {      
        vector<shared_ptr<Type>> types;
        for (int j = 0; j < width; j++) {
            types.push_back(tupdata[j]._type);
        }
        shared_ptr<TupleDesc> td = make_shared<TupleDesc>(types);
        int i = 0;
        vector<shared_ptr<Tuple>> tupVec;
        while (i < tupdata.size()) {
            shared_ptr<Tuple> tup = make_shared<Tuple>(td);
            for (int j = 0; j < width; j++) {
                shared_ptr<Field> f = nullptr;
                
                auto& t = tupdata[i];
                i++;
                if (t._type->type() == String_Type::STRING_TYPE()->type())
                    f = make_shared<StringField>(t._str, Type::STRING_LEN);
                else
                    f = make_shared<IntField>(t._int);


                tup->setField(j, f);
            }
            tupVec.push_back(tup);
        }

        shared_ptr<TupleIterator> result = make_shared<TupleIterator>(td, tupVec);
        result->open();
        return result;
    }

    /**
     * @return true if the tuples have the same number of fields and
     *   corresponding fields in the two Tuples are all equal.
     */
    static bool compareTuples(Tuple& t1, Tuple& t2) {
        if (t1.getTupleDesc()->numFields() != t2.getTupleDesc()->numFields())
            return false;

        for (int i = 0; i < t1.getTupleDesc()->numFields(); ++i) {
            if (t1.getTupleDesc()->getFieldType(i)->toString() != t2.getTupleDesc()->getFieldType(i)->toString())
                return false;
            if ((t1.getField(i) == nullptr || t2.getField(i) == nullptr) || !(t1.getField(i)->equals(*(t2.getField(i)))))
                return false;
        }

        return true;
    }

    /**
    * Check to see if the DbIterators have the same number of tuples and
    *   each tuple pair in parallel iteration satisfies compareTuples .
    * If not, throw an assertion.
    */
    static void compareDbIterators(OpIterator& expected, OpIterator& actual){
    while (expected.hasNext()) {
        EXPECT_TRUE(actual.hasNext());

        Tuple* expectedTup = expected.next();
        Tuple* actualTup = actual.next();
        EXPECT_TRUE(compareTuples(*expectedTup, *actualTup));
    }
    // Both must now be exhausted
    EXPECT_FALSE(expected.hasNext());
    EXPECT_FALSE(actual.hasNext());
    }

    /**
    * Check to see if every tuple in expected matches <b>some</b> tuple
    *   in actual via compareTuples. Note that actual may be a superset.
    * If not, throw an assertion.
    */
    static void matchAllTuples(shared_ptr<OpIterator> expected, shared_ptr<OpIterator> actual){
        // TODO(ghuo): this n^2 set comparison is kind of dumb, but we haven't
        // implemented hashCode or equals for tuples.
        bool matched = false;
        while (expected->hasNext()) {
            Tuple* expectedTup = expected->next();
            matched = false;
            actual->rewind();

            while (actual->hasNext()) {
                Tuple* next = actual->next();
                if (compareTuples(*expectedTup, *next)) {
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                throw runtime_error("expected tuple not found: " + expectedTup->toString());
            }
        }
    }

    /**
     * Verifies that the OpIterator has been exhausted of all elements.
     */
    static bool checkExhausted(OpIterator& it)
    {
        if (it.hasNext()) return false;
        try
        {
            Tuple* t = it.next();
            cout << "Got unexpected tuple: " + t->toString() << endl;
            return false;
        }
        catch (const std::exception&)
        {
            return true;
        }
    }

    static vector<unsigned char> readFileBytes(const string& path);
	
    class SkeletonFile :public DbFile {
	public:
        SkeletonFile(size_t tableId, shared_ptr<TupleDesc> td)
            :_tableId(tableId), _td(td)
        {};
        
        shared_ptr<Page> readPage(shared_ptr<PageId> id)override {
            throw runtime_error("not implemented");
        }

        size_t numPages()override {
            throw runtime_error("not implemented");
        }

        void writePage(shared_ptr<Page> p)override {
            throw runtime_error("not implemented");
        }

        vector<shared_ptr<Page>> insertTuple(shared_ptr<TransactionId> tid,shared_ptr<Tuple> t)override {
            throw runtime_error("not implemented");
        }

        vector<shared_ptr<Page>> deleteTuple(shared_ptr<TransactionId> tid,Tuple& t)override {
            throw runtime_error("not implemented");
        }

        int bytesPerPage() {
            throw runtime_error("not implemented");
        }

        size_t getId()override {
            return _tableId;
        }

        shared_ptr<DbFileIterator> iterator(shared_ptr<TransactionId> tid)override {
            throw runtime_error("not implemented");
        }

        shared_ptr<TupleDesc> getTupleDesc() {
            return _td;
        }
	private:
		size_t _tableId;
		shared_ptr<TupleDesc> _td;
	};

    class MockScan : public OpIterator {
    
    public:
        /**
         * Creates a fake SeqScan that returns tuples sequentially with 'width'
         * fields, each with the same value, that increases from low (inclusive)
         * and high (exclusive) over getNext calls.
         */
        MockScan(int low, int high, int width)
            :_low(low), _high(high), _width(width), _cur(low) {}

        void open()override {
            _cur = _low;
        }

        void close()override {
        }

        void rewind()override {
            _cur = _low;
        }

        shared_ptr<TupleDesc> getTupleDesc()override {
            return Utility::getTupleDesc(_width);
        }
        bool hasNext()override {
            return _cur < _high;
        }

        Tuple* next()override {
            if (_cur >= _high)
                throw runtime_error("no such element");
            _t = make_shared<Tuple>(getTupleDesc());
            for (int i = 0; i < _width; ++i)
                _t->setField(i, make_shared<IntField>(_cur));
            _cur++;
            return _t.get();
        }
    protected:
        Tuple* readNext() {
            if (_cur >= _high) 
                return nullptr;

            _t = make_shared<Tuple>(getTupleDesc());
            for (int i = 0; i < _width; ++i)
                _t->setField(i, make_shared<IntField>(_cur));
            _cur++;
            return _t.get();
        }
    private:
        int _cur;
        int _low;
        int _high;
        int _width;
        shared_ptr<Tuple> _t;
    };

    /**
     * Helper class that attempts to acquire a lock on a given page in a new
     * thread.
     *
     * @return a handle to the Thread that will attempt lock acquisition after it
     *   has been started
     */
    class LockGrabber {
    public:
        /**
         * @param tid the transaction on whose behalf we want to acquire the lock
         * @param pid the page over which we want to acquire the lock
         * @param perm the desired lock permissions
         */
        LockGrabber(shared_ptr<TransactionId> tid, shared_ptr<PageId> pid, Permissions perm) {
            _tid = tid;
            _pid = pid;
            _perm = perm;
            _acquired = false;
        }

        //call run in a new thread
        void start() {
            thread t(&LockGrabber::run, this);
            _handle = t.native_handle();
            t.detach();
            
        }
        //for test clean up
        void stop() {
#ifdef _WINDOWS_
            TerminateThread(_handle, 0);
            _handle = nullptr;
#endif // _WINDOWS_                
        }
        void run() {
            try{
                Database::getBufferPool()->getPage(_tid, _pid, _perm);
                lock_guard<mutex> lock(_alock);
                _acquired = true;
            }
            catch (const std::exception& e){               
                lock_guard<mutex> lock(_elock);
                _error = e.what();
                cout << _error << endl;
                Database::getBufferPool()->transactionComplete(_tid, false);

            }
        }

        /**
         * @return true if we successfully acquired the specified lock
         */
        bool acquired() {
            lock_guard<mutex> lock(_alock);
            return _acquired;
        }

        /**
         * @return an error string if one occured during lock acquisition;
         *   null otherwise
         */
        string getError() {
            lock_guard<mutex> lock(_elock);
            return _error;
            
        }
    private:
        shared_ptr<TransactionId> _tid;
        shared_ptr<PageId> _pid;
        Permissions _perm;
        bool _acquired;
        string _error;
        mutex _alock;
        mutex _elock;
        thread::native_handle_type _handle;
    };

    class CreateHeapFile {
    protected:
        CreateHeapFile() {
            try
            {
                _emptyFile = File::createTempFile();
            }
            catch (const std::exception&)
            {
                throw runtime_error("createHeapFile failed");
            }
            _emptyFile->deleteOnExit();
        }

        void SetUp() {
            try
            {
                _empty = Utility::createEmptyHeapFile(_emptyFile->fileName(), 2);
            }
            catch (const std::exception&)
            {
                throw runtime_error("createHeapFile setUp failed");
            }

        }

        shared_ptr<HeapFile> _empty;
    private:
        shared_ptr<File> _emptyFile;
    };

    class CountDownLatch : public Noncopyable {
    public:
        CountDownLatch(int count) :_count(count) {}

        bool await() {
           unique_lock<mutex> lk(_mutex);          
           _cv.wait(lk, [this] {return _count == 0; });
           return true;
        }
        void countDown() {
            lock_guard<mutex> lk(_mutex);
            if (_count == 0)
                return;
            _count--;
            if (_count == 0) {
                _cv.notify_all();
            }
        }
    public:
        int _count;
        mutex _mutex;
        condition_variable _cv;
    };
    template<typename Callable,typename... Args>
    class CyclicBarrier : public Noncopyable {
    public:
        CyclicBarrier(int count, shared_ptr<Callable> pCallable)
            :_count(count), _curCount(count), _pCallable(pCallable) {}
        void await(Args... args) {
            unique_lock<mutex> lock(_mutex);
            _curCount--;
            if (_curCount == 0) {
                (*_pCallable)(args...);              
                _cv.notify_all();
            }
            else {
                _cv.wait(lock);
            }
            _curCount += 1;
            lock.unlock();
            while (_curCount != _count)continue;
            //_cv.wait(lock, [this]() { return _curCount == _count; });
            this_thread::sleep_for(chrono::milliseconds(100));
        }

    private:
        void awaitFinish() {
            unique_lock<mutex> lock(_mutex);
           
        }


        int _count;
        int _curCount;
        mutex _mutex;
        condition_variable _cv;
        shared_ptr<Callable> _pCallable;
    };
};