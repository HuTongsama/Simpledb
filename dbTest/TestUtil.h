#pragma once
#include"DbFile.h"
#include"IntField.h"
#include"StringField.h"
#include"OpIterator.h"
#include"TupleIterator.h"
#include"Utility.h"
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
                if (t._type->toString() == String_Type::STRING_TYPE->toString())
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
            if (!(t1.getField(i)->equals(*(t2.getField(i)))))
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

        Tuple& expectedTup = expected.next();
        Tuple& actualTup = actual.next();
        EXPECT_TRUE(compareTuples(expectedTup, actualTup));
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
            Tuple& expectedTup = expected->next();
            matched = false;
            actual->rewind();

            while (actual->hasNext()) {
                Tuple& next = actual->next();
                if (compareTuples(expectedTup, next)) {
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                throw runtime_error("expected tuple not found: " + expectedTup.toString());
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
            Tuple& t = it.next();
            cout << "Got unexpected tuple: " + t.toString() << endl;
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

        int numPages() {
            throw runtime_error("not implemented");
        }

        void writePage(shared_ptr<Page> p)override {
            throw runtime_error("not implemented");
        }

        list<shared_ptr<Page>> insertTuple(const TransactionId& tid,const Tuple& t)override {
            throw runtime_error("not implemented");
        }

        list<shared_ptr<Page>> deleteTuple(const TransactionId& tid,const Tuple& t)override {
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

        Tuple& next()override {
            if (_cur >= _high)
                throw runtime_error("no such element");
            shared_ptr<Tuple> _t = make_shared<Tuple>(getTupleDesc());
            for (int i = 0; i < _width; ++i)
                _t->setField(i, make_shared<IntField>(_cur));
            _cur++;
            return *_t;
        }
    protected:
        shared_ptr<Tuple> readNext() {
            if (_cur >= _high) 
                return nullptr;

            shared_ptr<Tuple> tup = make_shared<Tuple>(getTupleDesc());
            for (int i = 0; i < _width; ++i)
                tup->setField(i, make_shared<IntField>(_cur));
            _cur++;
            return tup;
        }
    private:
        int _cur;
        int _low;
        int _high;
        int _width;
        shared_ptr<Tuple> _t;
    };
};