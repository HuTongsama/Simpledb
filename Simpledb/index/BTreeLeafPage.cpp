#include"BTreeLeafPage.h"
#include"IntField.h"
#include"BufferPool.h"
#include<assert.h>
namespace Simpledb {
	int BTreeLeafPage::getHeaderSize()
	{
		int tuplesPerPage = getMaxTuples();
		int hb = (tuplesPerPage / 8);
		if (hb * 8 < tuplesPerPage) hb++;

		return hb;
	}
	shared_ptr<Tuple> BTreeLeafPage::readNextTuple(DataStream& dis, int slotId)
	{
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!isSlotUsed(slotId)) {
			int sz = _td->getSize();
			for (int i = 0; i < sz; i++) {
				try
				{
					dis.readChar();
				}
				catch (const std::exception& e)
				{
					throw runtime_error("error reading empty tuple");
				}
			}
			return nullptr;
		}

		// read fields in the tuple
		shared_ptr<Tuple> t = make_shared<Tuple>(_td);
		shared_ptr<RecordId> rid = make_shared<RecordId>(_pid, slotId);
		t->setRecordId(rid);
		try
		{
			int fields = _td->numFields();
			for (int j = 0; j < fields; j++) {
				shared_ptr<Field> f = _td->getFieldType(j)->parse(dis);
				t->setField(j, f);
			}
		}
		catch (const std::exception&)
		{
			throw runtime_error("parsing error!");
		}

		return t;
	}
	void BTreeLeafPage::moveRecord(int from, int to)
	{
		if (!isSlotUsed(to) && isSlotUsed(from)) {
			markSlotUsed(to, true);
			shared_ptr<RecordId> rid = make_shared<RecordId>(_pid, to);
			_tuples[to] = _tuples[from];
			_tuples[to]->setRecordId(rid);
			markSlotUsed(from, false);
		}
	}
	void BTreeLeafPage::markSlotUsed(int i, bool value)
	{
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;

		if (value)
			_header[headerbyte] |= 1 << headerbit;
		else
			_header[headerbyte] &= (0xFF ^ (1 << headerbit));
	}
	BTreeLeafPage::BTreeLeafPage(shared_ptr<BTreePageId> id, const vector<unsigned char>& data, int key)
		:BTreePage(id, key)
	{
		_numSlots = getMaxTuples();
		DataStream dis((const char*)data.data(), data.size());

		// Read the parent and sibling pointers
		try
		{
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			_parent = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage");
		}

		try
		{
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			_leftSibling = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage");
		}

		try
		{
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			_rightSibling = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage");
		}

		// allocate and read the header slots of this page
		int hSz = getHeaderSize();
		_header.resize(hSz, 0);
		for (int i = 0; i < hSz; i++)
			_header[i] = dis.readChar();

		_tuples.resize(_numSlots, nullptr);
		try
		{
			// allocate and read the actual records of this page
			for (int i = 0; i < _numSlots; i++)
				_tuples[i] = readNextTuple(dis, i);
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage");
		}

		setBeforeImage();
	}
	void BTreeLeafPage::checkRep(int fieldid, shared_ptr<Field> lowerBound, shared_ptr<Field> upperBound, bool checkoccupancy, int depth)
	{
		shared_ptr<Field> prev = lowerBound;
		assert(dynamic_pointer_cast<BTreePageId>(getId())->pgcateg()
			== BTreePageId::LEAF);
	
		shared_ptr<Iterator<Tuple>> it = this->iterator();
		
		while (it->hasNext()) {
			Tuple* t = it->next();
			//if (nullptr != prev && !prev->compare(Predicate::Op::LESS_THAN_OR_EQ, *(t->getField(fieldid))))
			//{
			//	cout << "prev: " << prev->hashCode() << ", cmp: " << t->getField(fieldid)->hashCode() << endl;
			//}
			assert(nullptr == prev || prev->compare(Predicate::Op::LESS_THAN_OR_EQ, *(t->getField(fieldid))));
			prev = t->getField(fieldid);
			assert(t->getRecordId()->getPageId()->equals(*(this->getId())));
		}

		assert(nullptr == upperBound || nullptr == prev || (prev->compare(Predicate::Op::LESS_THAN_OR_EQ, *upperBound)));

		assert(!checkoccupancy || depth <= 0 || (getNumTuples() >= getMaxTuples() / 2));
	}
	int BTreeLeafPage::getMaxTuples()
	{
		int bitsPerTupleIncludingHeader = _td->getSize() * 8 + 1;
		// extraBits are: left sibling pointer, right sibling pointer, parent pointer
		int extraBits = 3 * INDEX_SIZE * 8;
		return (BufferPool::getPageSize() * 8 - extraBits) / bitsPerTupleIncludingHeader;
	}
	shared_ptr<Page> BTreeLeafPage::getBeforeImage()
	{
		try
		{
			lock_guard<mutex> lock(_oldDataLock);

			return make_shared<BTreeLeafPage>(_pid, _oldData, _keyField);
		}
		catch (const std::exception&)
		{
			//should never happen -- we parsed it OK before!
			exit(1);
		}
		return shared_ptr<Page>();
	}
	void BTreeLeafPage::setBeforeImage()
	{
		lock_guard<mutex> lock(_oldDataLock);
		_oldData = getPageData();
	}
	vector<unsigned char> BTreeLeafPage::getPageData()
	{
		int len = BufferPool::getPageSize();
		DataStream dos(len);
		// write out the parent and sibling pointers
		try
		{
			dos.writeInt(_parent);
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage::getPageData");
		}
		
		try
		{
			dos.writeInt(_leftSibling);
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage::getPageData");
		}
		
		try
		{
			dos.writeInt(_rightSibling);
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage::getPageData");
		}

		// create the header of the page
		for (auto b : _header) {
			try
			{
				dos.writeChar(b);
			}
			catch (const std::exception&)
			{
				throw runtime_error("BTreeLeafPage::getPageData");
			}
		}

		int tupleSz = _tuples.size();
		// create the tuples
		for (int i = 0; i < tupleSz; i++) {
			// empty slot
			if (!isSlotUsed(i)) {
				int tdSz = _td->getSize();
				for (int j = 0; j < tdSz; j++) {
					try
					{
						dos.writeChar(0);
					}
					catch (const std::exception&)
					{
						throw runtime_error("BTreeLeafPage::getPageData");
					}
				}
				continue;
			}

			// non-empty slot
			int fieldSz = _td->numFields();
			for (int j = 0; j < fieldSz; j++) {
				shared_ptr<Field> f = _tuples[i]->getField(j);
				try
				{
					auto fData = f->serialize();
					dos.write((char*)fData.data(), fData.size());
				}
				catch (const std::exception&)
				{
					throw runtime_error("BTreeLeafPage::getPageData");
				}
			}
		}

		// padding
		int zerolen = BufferPool::getPageSize() - (_header.size() + _td->getSize() * _tuples.size() + 3 * INDEX_SIZE); //- numSlots * td.getSize();
		
		vector<char> zeroes(zerolen, 0);
		try
		{
			dos.write(zeroes.data(), zerolen);
		}
		catch (const std::exception&)
		{
			throw runtime_error("BTreeLeafPage::getPageData");
		}
	
		return dos.toByteArray();
	}
	void BTreeLeafPage::deleteTuple(Tuple& t)
	{
		shared_ptr<RecordId> rid = t.getRecordId();
		if (rid == nullptr)
			throw runtime_error("tried to delete tuple with null rid");
		if ((rid->getPageId()->getPageNumber() != _pid->getPageNumber()) ||
			(rid->getPageId()->getTableId() != _pid->getTableId()))
			throw runtime_error("tried to delete tuple on invalid page or table");
		if (!isSlotUsed(rid->getTupleNumber()))
			throw runtime_error("tried to delete null tuple.");
		markSlotUsed(rid->getTupleNumber(), false);
		t.setRecordId(nullptr);
	}
	void BTreeLeafPage::insertTuple(shared_ptr<Tuple> t)
	{
		if (!t->getTupleDesc()->equals(*_td))
			throw runtime_error("type mismatch, in addTuple");

		// find the first empty slot 
		int emptySlot = -1;
		for (int i = 0; i < _numSlots; i++) {
			if (!isSlotUsed(i)) {
				emptySlot = i;
				break;
			}
		}

		if (emptySlot == -1) {
			cout << "page num:" << _pid->getPageNumber() << endl;
			throw runtime_error("called addTuple on page with no empty slots.");
		}
			

		// find the last key less than or equal to the key being inserted
		int lessOrEqKey = -1;
		shared_ptr<Field> key = t->getField(_keyField);
		for (int i = 0; i < _numSlots; i++) {
			if (isSlotUsed(i)) {
				if (_tuples[i]->getField(_keyField)->compare(Predicate::Op::LESS_THAN_OR_EQ, *key))
					lessOrEqKey = i;
				else
					break;
			}
		}

		// shift records back or forward to fill empty slot and make room for new record
		// while keeping records in sorted order
		int goodSlot = -1;
		if (emptySlot < lessOrEqKey) {
			for (int i = emptySlot; i < lessOrEqKey; i++) {
				moveRecord(i + 1, i);
			}
			goodSlot = lessOrEqKey;
		}
		else {
			for (int i = emptySlot; i > lessOrEqKey + 1; i--) {
				moveRecord(i - 1, i);
			}
			goodSlot = lessOrEqKey + 1;
		}

		// insert new record into the correct spot in sorted order
		markSlotUsed(goodSlot, true);
		shared_ptr<RecordId> rid = make_shared<RecordId>(_pid, goodSlot);
		t->setRecordId(rid);
		_tuples[goodSlot] = t;
	}
	shared_ptr<BTreePageId> BTreeLeafPage::getLeftSiblingId()
	{
		if (_leftSibling == 0) {
			return nullptr;
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _leftSibling, BTreePageId::LEAF);
	}
	shared_ptr<BTreePageId> BTreeLeafPage::getRightSiblingId()
	{
		if (_rightSibling == 0) {
			return nullptr;
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _rightSibling, BTreePageId::LEAF);
	}
	void BTreeLeafPage::setLeftSiblingId(shared_ptr<BTreePageId> id)
	{
		if (id == nullptr) {
			_leftSibling = 0;
		}
		else {
			if (id->getTableId() != _pid->getTableId()) {
				throw runtime_error("table id mismatch in setLeftSiblingId");
			}
			if (id->pgcateg() != BTreePageId::LEAF) {
				throw runtime_error("leftSibling must be a leaf node");
			}
			_leftSibling = id->getPageNumber();
		}
	}
	void BTreeLeafPage::setRightSiblingId(shared_ptr<BTreePageId> id)
	{
		if (id == nullptr) {
			_rightSibling = 0;
		}
		else {
			if (id->getTableId() != _pid->getTableId()) {
				throw runtime_error("table id mismatch in setRightSiblingId");
			}
			if (id->pgcateg() != BTreePageId::LEAF) {
				throw runtime_error("rightSibling must be a leaf node");
			}
			_rightSibling = id->getPageNumber();
		}
	}
	size_t BTreeLeafPage::getNumTuples()
	{
		return _numSlots - getNumEmptySlots();
	}
	size_t BTreeLeafPage::getNumEmptySlots()
	{
		int cnt = 0;
		for (int i = 0; i < _numSlots; i++)
			if (!isSlotUsed(i))
				cnt++;
		return cnt;
	}
	bool BTreeLeafPage::isSlotUsed(size_t i)
	{
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;
		return (_header[headerbyte] & (1 << headerbit)) != 0;
	}
	shared_ptr<Iterator<Tuple>> BTreeLeafPage::iterator()
	{
		return make_shared<BTreeLeafPageIterator>(this);
	}
	shared_ptr<Iterator<Tuple>> BTreeLeafPage::reverseIterator()
	{
		return make_shared<BTreeLeafPageReverseIterator>(this);
	}
	shared_ptr<Tuple> BTreeLeafPage::getTuple(int i)
	{
		if (i >= _tuples.size() || i < 0)
			return nullptr;

		try
		{
			if (!isSlotUsed(i)) {
				//printf("BTreeLeafPage.getTuple: slot %d in %I64u:%I64u is not used\n", i, _pid->getTableId(), _pid->getPageNumber());
				return nullptr;
			}

			//Debug.log(1, "BTreeLeafPage.getTuple: returning tuple %d", i);
			return _tuples[i];
		}
		catch (const std::exception&)
		{
			throw runtime_error("no such element");
		}

	}
	int BTreeLeafPage::getFirstTupleIndex()
	{
		size_t sz = getMaxTuples();
		for (int i = 0; i < sz; ++i) {
			if (isSlotUsed(i))return i;
		}
		return 0;
	}
	int BTreeLeafPage::getLastTupleIndex()
	{
		size_t sz = getMaxTuples() - 1;
		for (int i = sz; i >= 0; --i) {
			if (isSlotUsed(i))return i;
		}
		return 0;
	}
	BTreeLeafPageIterator::BTreeLeafPageIterator(BTreeLeafPage* p) :_p(p)
	{
		_position = _p->getFirstTupleIndex();
	}
	bool BTreeLeafPageIterator::hasNext()
	{
		if (_p->getTuple(_position) != nullptr)
			return true;
		else {
			auto sz = _p->getMaxTuples();
			do
			{
				_position++;
				if (_p->getTuple(_position) != nullptr)
					return true;

			} while (_position < sz);
		}
		return false;
		

	}
	Tuple* BTreeLeafPageIterator::next()
	{
		auto tuple = _p->getTuple(_position);
		_items.push_back(tuple);
		_position++;
		return tuple.get();
	}
	BTreeLeafPageReverseIterator::BTreeLeafPageReverseIterator(BTreeLeafPage* p) :_p(p)
	{
		_position = _p->getLastTupleIndex();
	}
	bool BTreeLeafPageReverseIterator::hasNext()
	{
		if (_p->getTuple(_position) != nullptr)
			return true;
		else {
			do
			{
				_position--;
				if (_p->getTuple(_position) != nullptr)
					return true;

			} while (_position >= 0);
		}
		return false;
	}
	Tuple* BTreeLeafPageReverseIterator::next()
	{
		auto tuple = _p->getTuple(_position);
		_items.push_back(tuple);
		_position--;
		return tuple.get();
	}
}
