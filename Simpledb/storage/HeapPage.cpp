#include"HeapPage.h"
#include"Database.h"
namespace Simpledb {
	HeapPage::HeapPage(shared_ptr<HeapPageId> id, const vector<unsigned char>& data)
		:_pid(id)
	{
		_td = Database::getCatalog()->getTupleDesc(_pid->getTableId());
		_numSlots = getNumTuples();
		int headerSize = getHeaderSize();
		_header = vector<unsigned char>(data.begin(), data.begin() + headerSize);
		_tuples.resize(_numSlots);
		DataStream ds((const char *)data.data(), data.size());
		for (int i = 0; i < _numSlots; ++i) {
			_tuples[i] = readNextTuple(ds, i);
		}
		setBeforeImage();
	}
	shared_ptr<PageId> HeapPage::getId()const
	{
		return _pid;
	}
	shared_ptr<TransactionId> HeapPage::isDirty()const
	{
		return nullptr;
	}
	void HeapPage::markDirty(bool dirty, const TransactionId& tid)
	{
	}
	vector<unsigned char> HeapPage::getPageData()
	{
		return vector<unsigned char>();
	}
	shared_ptr<Page> HeapPage::getBeforeImage()
	{
		lock_guard<mutex> lock(_oldDataLock);
		return make_shared<HeapPage>(_pid, _oldData);
	}
	void HeapPage::setBeforeImage()
	{
		lock_guard<mutex> lock(_oldDataLock);
		_oldData = getPageData();
	}
	vector<unsigned char> HeapPage::createEmptyPageData()
	{
		return vector<unsigned char>();
	}
	void HeapPage::deleteTuple(const Tuple& t)
	{
	}
	void HeapPage::insertTuple(const Tuple& t)
	{
	}
	int HeapPage::getNumEmptySlots()
	{
		return 0;
	}
	bool HeapPage::isSlotUsed(int i)
	{
		return false;
	}
	shared_ptr<HeapPage::HeapPageIter> HeapPage::iterator()
	{
		return make_shared<HeapPageIter>(this);
	}
	int HeapPage::getNumTuples()
	{
		int tupleBytes = 0;
		auto tdIter = _td->iterator();
		while (tdIter->hasNext())
		{
			auto& item = tdIter->next();
			tupleBytes += item._fieldType->getLen();
		}
		int pageBytes = Database::getBufferPool()->getPageSize();
		int tupleCount = (pageBytes * 8) / (tupleBytes * 8 + 1);//floor comes for free
		return tupleCount;
	}
	int HeapPage::getHeaderSize()
	{	
		
		int headerBytes = _numSlots / 8;
		if (headerBytes * 8 < _numSlots) {
			headerBytes++;//ceiling
		}
		return headerBytes;
	}
	shared_ptr<Tuple> HeapPage::readNextTuple(DataStream& ds, int slotId)
	{
		// if associated bit is not set, read forward to the next tuple, and
		// return nullptr.
		if (!isSlotUsed(slotId)) {
			size_t sz = _td->getSize();
			for (size_t i = 0; i < sz; i++) {
				try {
					ds.readChar();
				}
				catch (const std::exception&) {
					throw runtime_error("error reading empty tuple");
				}

			}
			return nullptr;
		}

		// read fields in the tuple
		shared_ptr<Tuple> t = make_shared<Tuple>(_td);
		shared_ptr<RecordId> rid = make_shared<RecordId>(_pid, slotId);
		t->setRecordId(rid);
		try {
			int numFields = _td->numFields();
			for (int i = 0; i < numFields; i++) {
				shared_ptr<Field> f = _td->getFieldType(i)->parse(ds);
				t->setField(i, f);
			}
		}
		catch (const std::exception&) {
			throw runtime_error("parsing type error!");
		}

		return t;
	}
	void HeapPage::markSlotUsed(int i, bool value)
	{
	}
}