#pragma once
#include"DbFile.h"
using namespace Simpledb;
class TestUtil {
public:

	class SkeletonFile :public DbFile {
	public:
        SkeletonFile(int tableId, shared_ptr<TupleDesc> td)
            :_tableId(tableId), _td(td)
        {};
        
        shared_ptr<Page> readPage(const PageId& id)override {
            throw runtime_error("not implemented");
        }

        int numPages() {
            throw runtime_error("not implemented");
        }

        bool writePage(const Page& p)override {
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

        DbFileIterator& iterator(const TransactionId& tid)override {
            throw runtime_error("not implemented");
        }

        shared_ptr<TupleDesc> getTupleDesc() {
            return _td;
        }
	private:
		int _tableId;
		shared_ptr<TupleDesc> _td;
	};
};