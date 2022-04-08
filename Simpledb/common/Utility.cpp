#include"Utility.h"
#include"HeapPage.h"
#include"IntField.h"
#include"Database.h"
#include"boost/uuid/uuid.hpp"
#include"boost/uuid/uuid_io.hpp"
#include"boost/uuid/uuid_generators.hpp"
#include"boost/lexical_cast.hpp"
namespace Simpledb {
	vector<shared_ptr<Type>> Utility::getTypes(int len)
	{
		vector<shared_ptr<Type>> result(len, Int_Type::INT_TYPE());
		return result;
	}
	vector<string> Utility::getStrings(int len, const string& val)
	{
		vector<string> result(len, val);
		for (int i = 0; i < len; ++i) {
			result[i] += to_string(i);
		}
		return result;
	}
	shared_ptr<TupleDesc> Utility::getTupleDesc(int n, const string& name)
	{
		return shared_ptr<TupleDesc>(
			new TupleDesc(getTypes(n), getStrings(n, name)));
	}
	shared_ptr<TupleDesc> Utility::getTupleDesc(int n)
	{
		return shared_ptr<TupleDesc>(new TupleDesc(getTypes(n)));
	}
	shared_ptr<Tuple> Utility::getHeapTuple(int n)
	{
		shared_ptr<Tuple> tup(new Tuple(getTupleDesc(1)));
		tup->setRecordId(shared_ptr<RecordId>
			(new RecordId(shared_ptr<PageId>(new HeapPageId(1, 2)),3)));
		tup->setField(0, shared_ptr<Field>(new IntField(n)));
		return tup;
	}
	shared_ptr<Tuple> Utility::getHeapTuple(const vector<int>& tupdata)
	{
		shared_ptr<Tuple> tup(new Tuple(getTupleDesc((int)tupdata.size())));
		tup->setRecordId(shared_ptr<RecordId>(
			new RecordId(shared_ptr<PageId>(new HeapPageId(1, 2)), 3)));
		for (int i = 0; i < tupdata.size(); ++i) {
			tup->setField(i, shared_ptr<Field>(new IntField(tupdata[i])));
		}
		return tup;
	}
	shared_ptr<Tuple> Utility::getHeapTuple(int n, int width)
	{
		shared_ptr<Tuple> tup(new Tuple(getTupleDesc(width)));
		tup->setRecordId(shared_ptr<RecordId>(
			new RecordId(shared_ptr<HeapPageId>(new HeapPageId(1, 2)), 3)));
		for (int i = 0; i < width; ++i) {
			tup->setField(i, shared_ptr<Field>(new IntField(n)));
		}
		return tup;
	}
	shared_ptr<Tuple> Utility::getTuple(const vector<int>& tupledata, int width)
	{
		if (tupledata.size() != width) {
			cout << "get Hash Tuple has the wrong length~" << endl;
			exit(1);
		}
		shared_ptr<Tuple> tup(new Tuple(getTupleDesc(width)));
		for (int i = 0; i < width; ++i) {
			tup->setField(i, shared_ptr<Field>(new IntField(tupledata[i])));
		}
		return tup;
	}
	shared_ptr<HeapFile> Utility::createEmptyHeapFile(const string& path, int cols)
	{
		shared_ptr<File> f = make_shared<File>(path);
		shared_ptr<HeapFile> hf = openHeapFile(cols, f);
		shared_ptr<HeapPageId> pid(new HeapPageId(hf->getId(), 0));

		shared_ptr<Page> page = nullptr;
		try
		{
			page.reset(new HeapPage(pid, HeapPage::createEmptyPageData()));
		}
		catch (const std::exception&)
		{
			throw runtime_error("failed to create empty page in HeapFile");
		}
		hf->writePage(page);
		return hf;
	}
	shared_ptr<HeapFile> Utility::openHeapFile(int cols, shared_ptr<File> f)
	{
		// create the HeapFile and add it to the catalog
		shared_ptr<TupleDesc> td = getTupleDesc(cols);
		shared_ptr<HeapFile> hf(new HeapFile(f, td));
		Database::getCatalog()->addTable(hf,
			boost::lexical_cast<std::string>(boost::uuids::random_generator()()));		
		return hf;
	}
	shared_ptr<HeapFile> Utility::openHeapFile(int cols, const string& colPrefix, shared_ptr<File> f, shared_ptr<TupleDesc> td)
	{
		// create the HeapFile and add it to the catalog
		shared_ptr<HeapFile> hf(new HeapFile(f, td));
		Database::getCatalog()->addTable(hf,
			boost::lexical_cast<std::string>(boost::uuids::random_generator()()));
		return hf;
	}
	shared_ptr<HeapFile> Utility::openHeapFile(int cols, const string& colPrefix, shared_ptr<File> f)
	{
		// create the HeapFile and add it to the catalog
		shared_ptr<TupleDesc> td = getTupleDesc(cols, colPrefix);
		return openHeapFile(cols, colPrefix, f, td);
	}
	string Utility::vectorToString(const vector<int>& list)
	{
		string s = "";
		for (auto i : list) {
			if (s.size() > 0) {
				s += "\t";
			}
			s += to_string(i);
		}
		return s;
	}
}