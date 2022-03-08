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
		DataStream ds((const char *)vector<unsigned char>(
			data.begin() + headerSize,data.end()).data(), data.size());
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
		vector<unsigned char> result;
		size_t tupleNum = _tuples.size();
		size_t tupleBytes = _td->getSize();
		size_t headerSz = _header.size();
		size_t totalTupleBytes = tupleNum * tupleBytes;
		size_t totalSz = headerSz + totalTupleBytes;
		if (totalSz < headerSz) {
			throw overflow_error("page data size overflow");
		}
		result.reserve(totalSz);
		for (auto& b : _header) {
			result.push_back(b);
		}
		int numFields = _td->numFields();
		//create the tuples
		for (int i = 0; i < tupleNum; i++) {
			//empty slot
			if (!isSlotUsed(i)) {
				for (int j = 0; j < tupleBytes; j++) {
					result.push_back(0);
				}
				continue;
			}
			//non-empty slot
			for (int j = 0; j < numFields; j++) {
				shared_ptr<Field> f = _tuples[i]->getField(j);
				vector<unsigned char> vec = f->serialize();
				result.insert(result.end(), vec.begin(), vec.end());
			}
		}
		size_t zeroLen = BufferPool::getPageSize() - (headerSz + totalTupleBytes);
		result.insert(result.end(), zeroLen, 0);
		return result;	
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
		size_t sz = BufferPool::getPageSize();
		return vector<unsigned char>(sz,0);
	}
	void HeapPage::deleteTuple(const Tuple& t)
	{
	}
	void HeapPage::insertTuple(const Tuple& t)
	{
	}
	size_t HeapPage::getNumEmptySlots()
	{
		size_t count = 0;
		for (auto& byte : _header) {
			if ((byte ^ 0xff) == 0) {
				count += 8;
			}
			else if (byte == 0) {
				break;
			}
			else {
				char c = byte;
				while (c > 0) {
					count++;
					c <<= 1;
				}
				break;
			}
		}
		size_t result = _numSlots - count;
		return result;
	}
	bool HeapPage::isSlotUsed(int i)
	{
		size_t bytePos = i / 8;
		size_t bitPos = i % 8;

		char b = _header[bytePos];
		bool result = (((b >> bitPos) & 0x1) > 0);
		return result;
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
	bool HeapPage::HeapPageIter::hasNext()
	{
		if (_position >= _page->_tuples.size()
			|| _page->_tuples[_position] == nullptr) {
			return false;
		}
		return true;
	}
	Tuple& HeapPage::HeapPageIter::next()
	{
		Tuple& t = *(_page->_tuples[_position]);
		_position++;
		return t;
	}
}