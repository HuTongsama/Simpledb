#include"BTreeUtility.h"
#include"Utility.h"
#include"BTreePageId.h"
#include"Database.h"
#include"BTreeFileEncoder.h"
#include"boost/uuid/uuid.hpp"
#include"boost/uuid/uuid_io.hpp"
#include"boost/uuid/uuid_generators.hpp"
#include"boost/lexical_cast.hpp"
#include<random>
namespace Simpledb {
	vector<int> BTreeUtility::tupleToList(const Tuple& tuple)
	{
        vector<int> list;
        Tuple& tupleRef = const_cast<Tuple&>(tuple);
        for (int i = 0; i < tupleRef.getTupleDesc()->numFields(); ++i) {
            int value = (dynamic_pointer_cast<IntField>(tupleRef.getField(i)))->getValue();
            list.push_back(value);
        }
        return list;
	}
    shared_ptr<Tuple> BTreeUtility::getBTreeTuple(int n)
    {
        shared_ptr<Tuple> tup = make_shared<Tuple>(Utility::getTupleDesc(1));
        tup->setRecordId(make_shared<RecordId>(make_shared<BTreePageId>(1, 2, BTreePageId::LEAF), 3));
        tup->setField(0, make_shared<IntField>(n));
        return tup;
    }
    shared_ptr<Tuple> BTreeUtility::getBTreeTuple(const vector<int>& tupdata)
    {
        shared_ptr<Tuple> tup = make_shared<Tuple>(Utility::getTupleDesc(tupdata.size()));
        tup->setRecordId(make_shared<RecordId>(make_shared<BTreePageId>(1, 2, BTreePageId::LEAF), 3));
        for (int i = 0; i < tupdata.size(); ++i)
            tup->setField(i, make_shared<IntField>(tupdata[i]));
        return tup;
    }
    shared_ptr<Tuple> BTreeUtility::getBTreeTuple(int n, int width)
    {
        shared_ptr<Tuple> tup = make_shared<Tuple>(Utility::getTupleDesc(width));
        tup->setRecordId(make_shared<RecordId>(make_shared<BTreePageId>(1, 2, BTreePageId::LEAF), 3));
        for (int i = 0; i < width; ++i)
            tup->setField(i, make_shared<IntField>(n));
        return tup;
    }
    shared_ptr<BTreeEntry> BTreeUtility::getBTreeEntry(int n)
    {
        shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(1, n, BTreePageId::LEAF);
        shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(1, n + 1, BTreePageId::LEAF);
        shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(n), leftChild, rightChild);
        e->setRecordId(make_shared<RecordId>(make_shared<BTreePageId>(1, 2, BTreePageId::INTERNAL), 3));
        return e;
    }
    shared_ptr<BTreeEntry> BTreeUtility::getBTreeEntry(int n, int tableid)
    {
        shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(tableid, n, BTreePageId::LEAF);
        shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(tableid, n + 1, BTreePageId::LEAF);
        shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(n), leftChild, rightChild);
        e->setRecordId(make_shared<RecordId>(make_shared<BTreePageId>(tableid, 2, BTreePageId::INTERNAL), 3));
        return e;
    }
    shared_ptr<BTreeEntry> BTreeUtility::getBTreeEntry(int n, int key, int tableid)
    {
        shared_ptr<BTreePageId> leftChild = make_shared<BTreePageId>(tableid, n, BTreePageId::LEAF);
        shared_ptr<BTreePageId> rightChild = make_shared<BTreePageId>(tableid, n + 1, BTreePageId::LEAF);
        shared_ptr<BTreeEntry> e = make_shared<BTreeEntry>(make_shared<IntField>(key), leftChild, rightChild);
        e->setRecordId(make_shared<RecordId>(make_shared<BTreePageId>(tableid, 2, BTreePageId::INTERNAL), 3));
        return e;
    }
    shared_ptr<BTreeFile> BTreeUtility::createRandomBTreeFile(int columns, int rows, const map<int, int>& columnSpecification,
        vector<vector<int>>& tuples, int keyField)
    {
        return createRandomBTreeFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples, keyField);
    }
    shared_ptr<BTreeFile> BTreeUtility::createRandomBTreeFile(int columns, int rows, int maxValue,
        const map<int, int>& columnSpecification, vector<vector<int>>& tuples, int keyField)
    {
        if (!tuples.empty()) {
            tuples.clear();
        }
        tuples.resize(rows);
        generateRandomTuples(columns, rows, maxValue, columnSpecification, tuples);

        // Convert the tuples list to a B+ tree file
        shared_ptr<File> hFile = File::createTempFile();
        hFile->deleteOnExit();

        shared_ptr<File> bFile = File::createTempFile();
        bFile->deleteOnExit();
        vector<shared_ptr<Type>> typeAr(columns,Int_Type::INT_TYPE());
   
        return BTreeFileEncoder::convert(tuples, hFile, bFile, BufferPool::getPageSize(),
            columns, typeAr, ',', keyField);
    }
    void BTreeUtility::generateRandomTuples(int columns, int rows, int maxValue,
        const map<int, int>& columnSpecification, vector<vector<int>>& tuples)
    {
        generateRandomTuples(columns, rows, 0, maxValue, columnSpecification, tuples);
    }
    void BTreeUtility::generateRandomTuples(int columns, int rows, int minValue, int maxValue, 
        const map<int, int>& columnSpecification, vector<vector<int>>& tuples)
    {
        // Seed with a real random value, if available
        std::random_device r;

        std::default_random_engine e1(r());
        std::uniform_int_distribution<int> uniform_dist(minValue, maxValue);

        // Fill the tuples list with generated values
        for (int i = 0; i < rows; ++i) {
            vector<int> tuple;
            for (int j = 0; j < columns; ++j) {
                // Generate random values, or use the column specification
                int columnValue = 0;
                if (columnSpecification.find(j) != columnSpecification.end())columnValue = columnSpecification.at(j);
                else columnValue = uniform_dist(e1);

                tuple.push_back(columnValue);
            }
            tuples[i] = tuple;
        }
    }
    void BTreeUtility::generateRandomEntries(int numKeys, int minKey, int maxKey, int minChildPtr, vector<int>& childPointers, vector<int>& keys)
    {       
        // Seed with a real random value, if available
        std::random_device r;

        std::default_random_engine e1(r());
        std::uniform_int_distribution<int> uniform_dist(minKey, maxKey);

        // Fill the keys and childPointers lists with generated values
        int child = minChildPtr;
        int i = 0;
        for (i = 0; i < numKeys; ++i) {
            keys[i] = uniform_dist(e1);
            childPointers[i] = child;
            ++child;
        }

        // one extra child pointer
        childPointers[i] = child;
    }
    vector<shared_ptr<Tuple>> BTreeUtility::generateRandomTuples(int columns, int rows, int min, int max)
    {
        vector<vector<int>> tuples(rows);
        generateRandomTuples(columns, rows, min, max, map<int,int>(), tuples);
        vector<shared_ptr<Tuple>> tupleList;
        for (auto& tup : tuples) {
            tupleList.push_back(getBTreeTuple(tup));
        }
        return tupleList;
    }
    vector<shared_ptr<BTreeEntry>> BTreeUtility::generateRandomEntries(
        int numKeys, int tableid, int childPageCategory,
        int minKey, int maxKey, int minChildPtr)
    {
        vector<int> keys(numKeys);
        vector<int> childPointers(numKeys + 1);
        generateRandomEntries(numKeys, minKey, maxKey, minChildPtr, childPointers, keys);
        sort(keys.begin(), keys.end());
        vector<shared_ptr<BTreeEntry>> entryList;
        for (int i = 0; i < numKeys; ++i) {
            entryList.push_back(make_shared<BTreeEntry>(make_shared<IntField>(keys[i]),
                make_shared<BTreePageId>(tableid, childPointers[i], childPageCategory),
                make_shared<BTreePageId>(tableid, childPointers[i + 1], childPageCategory)));
        }
        return entryList;
    }
    int BTreeUtility::getNumTuplesPerPage(int columns)
    {

        int bytesPerTuple = Int_Type::INT_TYPE()->getLen() * columns * 8;
        return (BufferPool::getPageSize() * 8 - 3 * BTreeLeafPage::INDEX_SIZE * 8) / (bytesPerTuple + 1);
    }
    shared_ptr<BTreeLeafPage> BTreeUtility::createRandomLeafPage(shared_ptr<BTreePageId> pid, int columns, int keyField, int min, int max)
    {
        int tuplesPerPage = getNumTuplesPerPage(columns);
        return createRandomLeafPage(pid, columns, keyField, tuplesPerPage, min, max);
    }
    shared_ptr<BTreeLeafPage> BTreeUtility::createRandomLeafPage(shared_ptr<BTreePageId> pid, int columns, int keyField, int numTuples, int min, int max)
    {
        vector<shared_ptr<Type>> typeAr(columns,Int_Type::INT_TYPE());  
        auto tuples = BTreeUtility::generateRandomTuples(columns, numTuples, min, max);
        vector<unsigned char> data = BTreeFileEncoder::convertToLeafPage(
            tuples, BufferPool::getPageSize(), columns, typeAr, keyField);
        return make_shared<BTreeLeafPage>(pid, data, keyField);
    }
    int BTreeUtility::getNumEntriesPerPage()
    {
        int nentrybytes = Int_Type::INT_TYPE()->getLen() + BTreeInternalPage::INDEX_SIZE;
        // pointerbytes: one extra child pointer, parent pointer, child page category
        int internalpointerbytes = 2 * BTreeLeafPage::INDEX_SIZE + 1;
        return (BufferPool::getPageSize() * 8 - internalpointerbytes * 8 - 1) / (nentrybytes * 8 + 1);
    }
    shared_ptr<BTreeInternalPage> BTreeUtility::createRandomInternalPage(shared_ptr<BTreePageId> pid, int keyField, int childPageCategory, int minKey, int maxKey, int minChildPtr)
    {
        int entriesPerPage = getNumEntriesPerPage();
        return createRandomInternalPage(pid, keyField, childPageCategory, entriesPerPage, minKey, maxKey, minChildPtr);
    }
    shared_ptr<BTreeInternalPage> BTreeUtility::createRandomInternalPage(shared_ptr<BTreePageId> pid, int keyField, int childPageCategory, int numKeys, int minKey, int maxKey, int minChildPtr)
    {
        auto entries = BTreeUtility::generateRandomEntries(numKeys, (int)pid->getTableId(), childPageCategory, minKey, maxKey, minChildPtr);
        vector<unsigned char> data = BTreeFileEncoder::convertToInternalPage(entries ,
            BufferPool::getPageSize(), Int_Type::INT_TYPE(), childPageCategory);
        return make_shared<BTreeInternalPage>(pid, data, keyField);
    }
    shared_ptr<BTreeFile> BTreeUtility::createBTreeFile(int columns, int rows,
        const map<int, int>& columnSpecification, vector<vector<int>>& tuples, int keyField)
    {
        tuples.clear();

        // Fill the tuples list with generated values
        for (int i = 0; i < rows; ++i) {
            vector<int> tuple;
            for (int j = 0; j < columns; ++j) {
                // Generate values, or use the column specification
                int columnValue = 0;
                if (columnSpecification.find(j) != columnSpecification.end()) columnValue = columnSpecification.at(j);
                else columnValue = (i + 1) * (j + 1);
                tuple.push_back(columnValue);
            }
            tuples.push_back(tuple);
        }

        // Convert the tuples list to a B+ tree file
        shared_ptr<File> hFile = File::createTempFile();
        hFile->deleteOnExit();

        shared_ptr<File> bFile = File::createTempFile();
        bFile->deleteOnExit();

        vector<shared_ptr<Type>> typeAr(columns , Int_Type::INT_TYPE());
        return BTreeFileEncoder::convert(tuples, hFile, bFile, BufferPool::getPageSize(),
            columns, typeAr, ',', keyField);
    }
    shared_ptr<BTreeFile> BTreeUtility::openBTreeFile(int cols, shared_ptr<File> f, int keyField)
    {
        // create the BTreeFile and add it to the catalog
        shared_ptr<TupleDesc> td = Utility::getTupleDesc(cols);
        shared_ptr<BTreeFile> bf = make_shared<BTreeFile>(f, keyField, td);
        Database::getCatalog()->addTable(bf, 
            boost::lexical_cast<std::string>(boost::uuids::random_generator()()));
        return bf;
    }
    shared_ptr<BTreeFile> BTreeUtility::createEmptyBTreeFile(const string& path, int cols, int keyField)
    {
        shared_ptr<File> f = make_shared<File>(path);
        return openBTreeFile(cols, f, keyField);
    }
    shared_ptr<BTreeFile> BTreeUtility::createEmptyBTreeFile(const string& path, int cols, int keyField, int pages)
    {
        shared_ptr<File> f = make_shared<File>(path);
        vector<unsigned char> emptyRootPtrData = BTreeRootPtrPage::createEmptyPageData();
        vector<unsigned char> emptyPageData = BTreePage::createEmptyPageData();
        f->writeBytes(emptyRootPtrData.data(),emptyRootPtrData.size());
        for (int i = 0; i < pages; ++i) {
            f->writeBytes(emptyPageData.data(), emptyPageData.size());
        }

        return openBTreeFile(cols, f, keyField);
    }
    void BTreeUtility::BTreeWriter::runInner()
    {
        try {
            int c = 0;
            while (c < _count) {
                shared_ptr<Tuple> t = BTreeUtility::getBTreeTuple(_item, 2);
                Database::getBufferPool()->insertTuple(_tid, _bf->getId(), t);

                IndexPredicate ipred(Predicate::Op::EQUALS, t->getField(_bf->keyField()));
                shared_ptr<DbFileIterator> it = _bf->indexIterator(_tid, ipred);
                it->open();
                c = 0;
                while (it->hasNext()) {
                    it->next();
                    c++;
                }
                it->close();
            }
            lock_guard<mutex> lock(_slock);
            _success = true;
        }
        catch (const std::exception& e) {
            cout << "BTreeUtility::BTreeWriter::runInner: " << e.what() << endl;
            lock_guard<mutex> lock(_elock);
            _error = e.what();
            Database::getBufferPool()->transactionComplete(_tid, false);
        }
    }
    BTreeUtility::BTreeWriter::BTreeWriter(shared_ptr<TransactionId> tid, shared_ptr<BTreeFile> bf, int item, int count)
        : _tid(tid), _bf(bf), _item(item), _count(count)
    {
    }
    bool BTreeUtility::BTreeWriter::succeeded()
    {
        lock_guard<mutex> lock(_slock);
        return _success;
    }
    const string& BTreeUtility::BTreeWriter::getError()
    {
        lock_guard<mutex> lock(_elock);
        return _error;
    }
    void BTreeUtility::BTreeReader::runInner()
    {
        try {
            while (true) {
                IndexPredicate ipred(Predicate::Op::EQUALS, _f);
                shared_ptr<DbFileIterator> it = _bf->indexIterator(_tid, ipred);
                it->open();
                int c = 0;
                while (it->hasNext()) {
                    it->next();
                    c++;
                }
                it->close();
                if (c >= _count) {
                    lock_guard<mutex> lock(_slock);
                    _found = true;
                }
            }
        }
        catch (const std::exception& e) {
            cout << "BTreeUtility::BTreeReader::runInner: " << e.what() << endl;
            lock_guard<mutex> lock(_elock);
            _error = e.what();
            Database::getBufferPool()->transactionComplete(_tid, false);
        }
    }
    BTreeUtility::BTreeReader::BTreeReader(shared_ptr<TransactionId> tid, shared_ptr<BTreeFile> bf, shared_ptr<Field> f, int count)
        :_tid(tid), _bf(bf), _f(f), _count(count)
    {
    }
    bool BTreeUtility::BTreeReader::found()
    {
        lock_guard<mutex> lock(_slock);
        return _found;
    }
    const string& BTreeUtility::BTreeReader::getError()
    {
        lock_guard<mutex> lock(_elock);
        return _error;
    }
    void BTreeUtility::BTreeInserter::runInner()
    {

        try {
            shared_ptr<Tuple> t = BTreeUtility::getBTreeTuple(_tupdata);
            Database::getBufferPool()->insertTuple(_tid, _bf->getId(), t);
            Database::getBufferPool()->transactionComplete(_tid);
            vector<int> tuple = tupleToList(*t);
            _insertedTuples->push(tuple);
            lock_guard<mutex> lock(_slock);
            _success = true;
        }
        catch (const std::exception& e) {
            cout << "BTreeUtility::BTreeReader::BTreeInserter: " << e.what() << endl;
            lock_guard<mutex> lock(_elock);
            _error = e.what();
            Database::getBufferPool()->transactionComplete(_tid, false);
        }
    }
    BTreeUtility::BTreeInserter::BTreeInserter(shared_ptr<BTreeFile> bf, const vector<int>& tupdata,
        shared_ptr<BlockingQueue<vector<int>>> insertedTuples)
        :_bf(bf), _tupdata(tupdata), _insertedTuples(insertedTuples)
    {
        _tid = make_shared<TransactionId>();
    }
    bool BTreeUtility::BTreeInserter::succeeded()
    {
        lock_guard<mutex> lock(_slock);
        return _success;
    }
    const string& BTreeUtility::BTreeInserter::getError()
    {
        lock_guard<mutex> lock(_elock);
        return _error;
    }
    void BTreeUtility::BTreeDeleter::runInner()
    {
        try {
            _tuple = _insertedTuples->pop();
            if (_bf->getTupleDesc()->numFields() != _tuple.size()) {
                throw runtime_error("tuple desc mismatch");
            }
            shared_ptr<IntField> key = make_shared<IntField>(_tuple[_bf->keyField()]);
            IndexPredicate ipred(Predicate::Op::EQUALS, key);
            shared_ptr<DbFileIterator> it = _bf->indexIterator(_tid, ipred);
            it->open();
            while (it->hasNext()) {
                Tuple* t = it->next();
                vector<int> tmpList = tupleToList(*t);
                int sz = (int)tmpList.size();
                bool flag = true;
                if (sz == _tuple.size()) {
                    for (int i = 0; i < sz; ++i) {
                        if (tmpList[i] != _tuple[i]) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        Database::getBufferPool()->deleteTuple(_tid, *t);
                        break;
                    }
                }
            }
            it->close();
            Database::getBufferPool()->transactionComplete(_tid);
            lock_guard<mutex> lock(_slock);
            _success = true;
        }
        catch (const std::exception& e) {
            cout << "BTreeUtility::BTreeReader::BTreeInserter: " << e.what() << endl;
            {
                lock_guard<mutex> lock(_elock);
                _error = e.what();
            }
            try
            {
                _insertedTuples->push(_tuple);
                Database::getBufferPool()->transactionComplete(_tid, false);
            }
            catch (const std::exception& e1)
            {
                cout << "BTreeUtility::BTreeReader::BTreeInserter::_insertedTuples->push(_tuple): " << e1.what() << endl;
            }
           
        }
    }
    BTreeUtility::BTreeDeleter::BTreeDeleter(shared_ptr<BTreeFile> bf, shared_ptr<BlockingQueue<vector<int>>> insertedTuples)
        :_bf(bf), _insertedTuples(insertedTuples)
    {
        _tid = make_shared<TransactionId>();
    }
    bool BTreeUtility::BTreeDeleter::succeeded()
    {
        lock_guard<mutex> lock(_slock);
        return _success;
    }
    const string& BTreeUtility::BTreeDeleter::getError()
    {
        lock_guard<mutex> lock(_elock);
        return _error;
    }
}