#include"BTreeInternalPage.h"
#include"Operator.h"
#include"Predicate.h"
#include"Type.h"
#include"IntField.h"
#include"BufferPool.h"
#include<assert.h>
namespace Simpledb {
	void BTreeInternalPage::checkRep(shared_ptr<Field> lowerBound, shared_ptr<Field> upperBound, bool checkOccupancy, int depth)
	{
		shared_ptr<Field> prev = lowerBound;
		assert(dynamic_pointer_cast<BTreePageId>(this->getId())->pgcateg() == BTreePageId::INTERNAL);

		shared_ptr<Iterator<BTreeEntry>> it = iterator();
		while (it->hasNext()) {
			shared_ptr<Field> f = it->next()->getKey();
			assert(nullptr == prev || prev->compare(Predicate::Op::LESS_THAN_OR_EQ, *f));
			prev = f;
		}

		assert(nullptr == upperBound || nullptr == prev || (prev->compare(Predicate::Op::LESS_THAN_OR_EQ, *upperBound)));

		assert(!checkOccupancy || depth <= 0 || (getNumEntries() >= getMaxEntries() / 2));
	}
	BTreeInternalPage::BTreeInternalPage(shared_ptr<BTreePageId> id, const vector<unsigned char>& data, int key)
		:BTreePage(id, key)
	{
		_numSlots = getMaxEntries() + 1;
		DataStream dis((const char*)data.data(), data.size());
		// Read the parent pointer
		try {
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			_parent = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception& e) {
			throw e;
		}

		// read the child page category
		_childCategory = dis.readChar();

		// allocate and read the header slots of this page
		int headerSz = getHeaderSize();
		_header = vector<unsigned char>(headerSz, 0);
		for (int i = 0; i < headerSz; i++)
			_header[i] = dis.readChar();
		_keys = vector<shared_ptr<Field>>(_numSlots);
		try {
			// allocate and read the keys of this page
			// start from 1 because the first key slot is not used
			// since a node with m keys has m+1 pointers
			_keys[0] = nullptr;
			for (int i = 1; i < _numSlots; i++)
				_keys[i] = readNextKey(dis, i);
		}
		catch (const std::exception& e) {
			throw e;
		}

		_children = vector<int>(_numSlots, 0);
		try {
			// allocate and read the child pointers of this page
			for (int i = 0; i < _numSlots; i++)
				_children[i] = readNextChild(dis, i);
		}
		catch (const std::exception& e) {
			throw e;
		}

		setBeforeImage();
	}
	int BTreeInternalPage::getMaxEntries()
	{
		int keySize = _td->getFieldType(_keyField)->getLen();
		int bitsPerEntryIncludingHeader = keySize * 8 + INDEX_SIZE * 8 + 1;
		// extraBits are: one parent pointer, 1 byte for child page category, 
		// one extra child pointer (node with m entries has m+1 pointers to children), 1 bit for extra header
		int extraBits = 2 * INDEX_SIZE * 8 + 8 + 1;
		int ret = (BufferPool::getPageSize() * 8 - extraBits) / bitsPerEntryIncludingHeader;
		return ret;
	}
	shared_ptr<Page> BTreeInternalPage::getBeforeImage()
	{
		try {
			lock_guard<mutex> lock(_oldDataLock);
			
			return make_shared<BTreeInternalPage>(_pid, _oldData, _keyField);
		}
		catch (const std::exception& e) {
			throw e;
		}
	}
	void BTreeInternalPage::setBeforeImage()
	{
		lock_guard<mutex> lock(_oldDataLock);
		_oldData = getPageData();
	}
	vector<unsigned char> BTreeInternalPage::getPageData()
	{
		int len = BufferPool::getPageSize();
		DataStream dos(len);

		// write out the parent pointer
		try
		{
			dos.writeInt(_parent);
		}
		catch (const std::exception& e)
		{
			throw e;
		}

		// write out the child page category
		try
		{
			dos.writeChar((char)_childCategory);
		}
		catch (const std::exception& e)
		{
			throw e;
		}
		for (auto ch : _header) {
			try
			{
				dos.writeChar(ch);
			}
			catch (const std::exception& e)
			{
				throw e;
			}
		}


		// create the keys
		// start from 1 because the first key slot is not used
		// since a node with m keys has m+1 pointers
		int keyLen = _keys.size();
		for (int i = 1; i < keyLen; i++) {
			// empty slot
			if (!isSlotUsed(i)) {
				int len = _td->getFieldType(_keyField)->getLen();
				for (int j = 0; j < len; j++) {
					try
					{
						dos.writeChar(0);
					}
					catch (const std::exception& e)
					{
						throw e;
					}
				}
				continue;
			}

			// non-empty slot
			try
			{
				vector<unsigned char> serializeData = _keys[i]->serialize();
				dos.write((char*)serializeData.data(), serializeData.size());
			}
			catch (const std::exception& e)
			{
				throw e;
			}		
		}

		// create the child pointers
		int childrenSz = _children.size();
		for (int i = 0; i < childrenSz; i++) {
			// empty slot
			if (!isSlotUsed(i)) {
				for (int j = 0; j < INDEX_SIZE; j++) {
					try
					{
						dos.writeChar(0);
					}
					catch (const std::exception& e)
					{
						throw e;
					}
				}
				continue;
			}

			// non-empty slot
			try {
				dos.writeInt(_children[i]);

			}
			catch (const std::exception& e) {
				throw e;
			}
		}

		// padding
		int zerolen = BufferPool::getPageSize() - (INDEX_SIZE + 1 + _header.size() +
			_td->getFieldType(_keyField)->getLen() * (keyLen - 1) + INDEX_SIZE * childrenSz);
		vector<char> zeroes(zerolen, 0);
		try {
			dos.write(zeroes.data(), zerolen);
		}
		catch (const std::exception& e) {
			throw e;
		}

		return dos.toByteArray();
	}
	void BTreeInternalPage::deleteKeyAndRightChild(BTreeEntry* e)
	{
		deleteEntry(e, true);
	}
	void BTreeInternalPage::deleteKeyAndLeftChild(BTreeEntry* e)
	{
		deleteEntry(e, false);
	}
	void BTreeInternalPage::updateEntry(BTreeEntry* e)
	{
		shared_ptr<RecordId> rid = e->getRecordId();
		if (rid == nullptr)
			throw runtime_error("tried to update entry with null rid");
		if ((rid->getPageId()->getPageNumber() != _pid->getPageNumber()) ||
			(rid->getPageId()->getTableId() != _pid->getTableId()))
			throw runtime_error("tried to update entry on invalid page or table");
		if (!isSlotUsed(rid->getTupleNumber()))
			throw runtime_error("tried to update null entry.");

		int tCount = rid->getTupleNumber();
		for (int i = tCount + 1; i < _numSlots; i++) {
			if (isSlotUsed(i)) {
				if (_keys[i]->compare(Predicate::Op::LESS_THAN, *(e->getKey()))) {
					throw runtime_error("attempt to update entry with invalid key " + e->getKey()->toString() +
						" HINT: updated key must be less than or equal to keys on the right");
				}
				break;
			}
		}
		for (int i = tCount - 1; i >= 0; i--) {
			if (isSlotUsed(i)) {
				if (i > 0 && _keys[i]->compare(Predicate::Op::GREATER_THAN, *(e->getKey()))) {
					throw runtime_error("attempt to update entry with invalid key " + e->getKey()->toString() +
						" HINT: updated key must be greater than or equal to keys on the left");
				}
				_children[i] = e->getLeftChild()->getPageNumber();
				break;
			}
		}
		_children[tCount] = e->getRightChild()->getPageNumber();
		_keys[tCount] = e->getKey();
	}
	void BTreeInternalPage::insertEntry(BTreeEntry* e)
	{
		if (e->getKey()->getType() != _td->getFieldType(_keyField)->type())
			throw runtime_error("key field type mismatch, in insertEntry");

		if (e->getLeftChild()->getTableId() != _pid->getTableId() ||
			e->getRightChild()->getTableId() != _pid->getTableId())
			throw runtime_error("table id mismatch in insertEntry");

		if (_childCategory == 0) {
			if (e->getLeftChild()->pgcateg() != e->getRightChild()->pgcateg())
				throw runtime_error("child page category mismatch in insertEntry");
			_childCategory = e->getLeftChild()->pgcateg();
		}
		else if (e->getLeftChild()->pgcateg() != _childCategory || e->getRightChild()->pgcateg() != _childCategory)
			throw runtime_error("child page category mismatch in insertEntry");

		// if this is the first entry, add it and return
		if (getNumEmptySlots() == getMaxEntries()) {
			_children[0] = e->getLeftChild()->getPageNumber();
			_children[1] = e->getRightChild()->getPageNumber();
			_keys[1] = e->getKey();
			markSlotUsed(0, true);
			markSlotUsed(1, true);
			e->setRecordId(make_shared<RecordId>(_pid, 1));
			return;
		}

		// find the first empty slot, starting from 1
		int emptySlot = -1;
		for (int i = 1; i < _numSlots; i++) {
			if (!isSlotUsed(i)) {
				emptySlot = i;
				break;
			}
		}

		if (emptySlot == -1)
			throw runtime_error("called insertEntry on page with no empty slots.");

		// find the child pointer matching the left or right child in this entry
		int lessOrEqKey = -1;
		for (int i = 0; i < _numSlots; i++) {
			if (isSlotUsed(i)) {
				if (_children[i] == e->getLeftChild()->getPageNumber() || _children[i] == e->getRightChild()->getPageNumber()) {
					if (i > 0 && _keys[i]->compare(Predicate::Op::GREATER_THAN, *(e->getKey()))) {
						throw runtime_error("attempt to insert invalid entry with left child " +
							to_string(e->getLeftChild()->getPageNumber()) + ", right child " +
							to_string(e->getRightChild()->getPageNumber()) + " and key " + e->getKey()->toString() +
							" HINT: one of these children must match an existing child on the page" +
							" and this key must be correctly ordered in between that child's" +
							" left and right keys" + ", slot i:" + to_string(i));
					}
					lessOrEqKey = i;
					if (_children[i] == e->getRightChild()->getPageNumber()) {
						_children[i] = e->getLeftChild()->getPageNumber();
					}
				}
				else if (lessOrEqKey != -1) {
					// validate that the next key is greater than or equal to the one we are inserting
					if (_keys[i]->compare(Predicate::Op::LESS_THAN, *(e->getKey()))) {
						throw runtime_error("attempt to insert invalid entry with left child " +
							to_string(e->getLeftChild()->getPageNumber()) + ", right child " +
							to_string(e->getRightChild()->getPageNumber()) + " and key " + e->getKey()->toString() +
							" HINT: one of these children must match an existing child on the page" +
							" and this key must be correctly ordered in between that child's" +
							" left and right keys" + ", slot i:" + to_string(i));
					}
					break;
				}
			}
		}

		if (lessOrEqKey == -1) {
			throw runtime_error("attempt to insert invalid entry with left child " +
				to_string(e->getLeftChild()->getPageNumber()) + ", right child " +
				to_string(e->getRightChild()->getPageNumber()) + " and key " + e->getKey()->toString() +
				" HINT: one of these children must match an existing child on the page" +
				" and this key must be correctly ordered in between that child's" +
				" left and right keys");
		}

		// shift entries back or forward to fill empty slot and make room for new entry
		// while keeping entries in sorted order
		int goodSlot = -1;
		if (emptySlot < lessOrEqKey) {
			for (int i = emptySlot; i < lessOrEqKey; i++) {
				moveEntry(i + 1, i);
			}
			goodSlot = lessOrEqKey;
		}
		else {
			for (int i = emptySlot; i > lessOrEqKey + 1; i--) {
				moveEntry(i - 1, i);
			}
			goodSlot = lessOrEqKey + 1;
		}

		// insert new entry into the correct spot in sorted order
		markSlotUsed(goodSlot, true);
		//Debug.log(1, "BTreeLeafPage.insertEntry: new entry, tableId = %d pageId = %d slotId = %d", pid.getTableId(), pid.getPageNumber(), goodSlot);
		_keys[goodSlot] = e->getKey();
		_children[goodSlot] = e->getRightChild()->getPageNumber();
		e->setRecordId(make_shared<RecordId>(_pid, goodSlot));
	}
	int BTreeInternalPage::getNumEntries()
	{
		return _numSlots - getNumEmptySlots() - 1;
	}
	size_t BTreeInternalPage::getNumEmptySlots()
	{
		int cnt = 0;
		// start from 1 because the first key slot is not used
		// since a node with m keys has m+1 pointers
		for (int i = 1; i < _numSlots; i++)
			if (!isSlotUsed(i))
				cnt++;
		return cnt;
	}
	bool BTreeInternalPage::isSlotUsed(size_t i)
	{
		size_t headerbit = i % 8;
		size_t headerbyte = (i - headerbit) / 8;
		return (_header[headerbyte] & (1 << headerbit)) != 0;
	}
	shared_ptr<Iterator<BTreeEntry>> BTreeInternalPage::iterator()
	{
		return make_shared<BTreeInternalPageIterator>(this);
	}
	shared_ptr<Iterator<BTreeEntry>> BTreeInternalPage::reverseIterator()
	{
		return make_shared<BTreeInternalPageReverseIterator>(this);
	}
	shared_ptr<Field> BTreeInternalPage::getKey(int i)
	{
		// key at slot 0 is not used
		if (i <= 0 || i >= _keys.size())
			throw runtime_error("BTreeInternalPage::getKey no such element.");
		try {
			if (!isSlotUsed(i)) {
				//Debug.log(1, "BTreeInternalPage.getKey: slot %d in %d:%d is not used", i, pid.getTableId(), pid.getPageNumber());
				return nullptr;
			}
			//Debug.log(1, "BTreeInternalPage.getKey: returning key %d", i);
			return _keys[i];

		}
		catch (const std::exception& e) {
			throw runtime_error("BTreeInternalPage::getKey no such element.");
		}
	}
	shared_ptr<BTreePageId> BTreeInternalPage::getChildId(int i)
	{
		if (i < 0 || i >= _children.size())
			throw runtime_error("BTreeInternalPage::getChildId no such element.");

		try {
			if (!isSlotUsed(i)) {
				//Debug.log(1, "BTreeInternalPage.getChildId: slot %d in %d:%d is not used", i, pid.getTableId(), pid.getPageNumber());
				return nullptr;
			}

			//Debug.log(1, "BTreeInternalPage.getChildId: returning child id %d", i);
			return make_shared<BTreePageId>(_pid->getTableId(), _children[i], _childCategory);

		}
		catch (const std::exception& e) {
			throw runtime_error("BTreeInternalPage::getChildId no such element.");
		}
	}
	int BTreeInternalPage::getHeaderSize()
	{
		int slotsPerPage = getMaxEntries() + 1;
		int hb = (slotsPerPage / 8);
		if (hb * 8 < slotsPerPage) hb++;

		return hb;
	}
	shared_ptr<Field> BTreeInternalPage::readNextKey(DataStream& dis, int slotId)
	{
		// if associated bit is not set, read forward to the next key, and
		// return null.
		if (!isSlotUsed(slotId)) {
			int len = _td->getFieldType(_keyField)->getLen();
			for (int i = 0; i < _td->getFieldType(_keyField)->getLen(); i++) {
				try
				{
					dis.readChar();
				}
				catch (const std::exception& e)
				{
					throw runtime_error("error reading empty key");
				}
				
			}
			return nullptr;
		}

		// read the key field
		shared_ptr<Field> f = nullptr;
		try
		{
			f = _td->getFieldType(_keyField)->parse(dis);
		}
		catch (const std::exception& e)
		{
			throw runtime_error("parsing error");
		}

		return f;
	}
	int BTreeInternalPage::readNextChild(DataStream& dis, int slotId)
	{
		// if associated bit is not set, read forward to the next child pointer, and
		// return -1.
		if (!isSlotUsed(slotId)) {
			for (int i = 0; i < INDEX_SIZE; i++) {
				try
				{
					dis.readChar();
				}
				catch (const std::exception&)
				{
					throw runtime_error("error reading empty child pointer");
				}
			}
			return -1;
		}

		// read child pointer
		int child = -1;
		try
		{
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			child = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception& e)
		{
			throw runtime_error("parsing error!");
		}
		return child;
	}
	void BTreeInternalPage::deleteEntry(BTreeEntry* e, bool deleteRightChild)
	{
		shared_ptr<RecordId> rid = e->getRecordId();
		if (rid == nullptr)
			throw runtime_error("tried to delete entry with null rid");
		if ((rid->getPageId()->getPageNumber() != _pid->getPageNumber()) ||
			(rid->getPageId()->getTableId() != _pid->getTableId()))
			throw runtime_error("tried to delete entry on invalid page or table");
		if (!isSlotUsed(rid->getTupleNumber()))
			throw runtime_error("tried to delete null entry.");
		if (deleteRightChild) {
			markSlotUsed(rid->getTupleNumber(), false);
		}
		else {
			int tCount = rid->getTupleNumber();
			for (int i = tCount - 1; i >= 0; i--) {
				if (isSlotUsed(i)) {
					_children[i] = _children[tCount];
					markSlotUsed(tCount, false);
					break;
				}
			}
		}
		e->setRecordId(nullptr);
	}
	void BTreeInternalPage::moveEntry(int from, int to)
	{
		if (!isSlotUsed(to) && isSlotUsed(from)) {
			markSlotUsed(to, true);
			_keys[to] = _keys[from];
			_children[to] = _children[from];
			markSlotUsed(from, false);
		}
	}
	void BTreeInternalPage::markSlotUsed(int i, bool value)
	{
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;

		//Debug.log(1, "BTreeInternalPage.setSlot: setting slot %d to %b", i, value);
		if (value)
			_header[headerbyte] |= 1 << headerbit;
		else
			_header[headerbyte] &= (0xFF ^ (1 << headerbit));
	}
	BTreeInternalPageIterator::BTreeInternalPageIterator(BTreeInternalPage* p) :_p(p)
	{
	}
	bool BTreeInternalPageIterator::hasNext()
	{
		if (_nextToReturn != nullptr)
			return true;

		try {
			if (_prevChildId == nullptr) {
				_prevChildId = _p->getChildId(0);
				if (_prevChildId == nullptr) {
					return false;
				}
			}
			while (true) {
				int entry = _curEntry++;
				shared_ptr<Field> key = _p->getKey(entry);
				shared_ptr<BTreePageId> childId = _p->getChildId(entry);
				if (key != nullptr && childId != nullptr) {
					_nextToReturn = make_shared<BTreeEntry>(key, _prevChildId, childId);
					_items.push_back(_nextToReturn);
					_nextToReturn->setRecordId(make_shared<RecordId>(_p->getId(), entry));
					_prevChildId = childId;
					return true;
				}
			}
		}
		catch (const std::exception& e) {
			return false;
		}
	}
	BTreeEntry* BTreeInternalPageIterator::next()
	{
		BTreeEntry* p = nullptr;
		if (_nextToReturn == nullptr) {
			if (hasNext()) {
				p = _nextToReturn.get();
				_nextToReturn = nullptr;
				return p;
			}
			else
				throw runtime_error("BTreeInternalPageIterator::next no such element.");
		}
		else {
			p = _nextToReturn.get();
			_nextToReturn = nullptr;
			return p;
		}
	}
	BTreeInternalPageReverseIterator::BTreeInternalPageReverseIterator(BTreeInternalPage* p) :_p(p)
	{
		_curEntry = p->getMaxEntries();
		while (!p->isSlotUsed(_curEntry) && _curEntry > 0) {
			--_curEntry;
		}
	}
	bool BTreeInternalPageReverseIterator::hasNext()
	{
		if (_nextToReturn != nullptr)
			return true;

		try {
			if (_nextChildId == nullptr) {
				_nextChildId = _p->getChildId(_curEntry);
				if (_nextChildId == nullptr) {
					return false;
				}
			}
			while (true) {
				int entry = _curEntry--;
				shared_ptr<Field> key = _p->getKey(entry);
				shared_ptr<BTreePageId> childId = _p->getChildId(entry - 1);
				if (key != nullptr && childId != nullptr) {
					_nextToReturn = make_shared<BTreeEntry>(key, childId, _nextChildId);
					_items.push_back(_nextToReturn);
					_nextToReturn->setRecordId(make_shared<RecordId>(_p->getId(), entry));
					_nextChildId = childId;
					return true;
				}
			}
		}
		catch (const std::exception& e) {
			return false;
		}
	}
	BTreeEntry* BTreeInternalPageReverseIterator::next()
	{
		BTreeEntry* p = nullptr;
		if (_nextToReturn == nullptr) {
			if (hasNext()) {
				p = _nextToReturn.get();
				_nextToReturn = nullptr;
				return p;
			}
			else
				throw runtime_error("BTreeInternalPageReverseIterator::next no such element.");
		}
		else {
			p = _nextToReturn.get();
			_nextToReturn = nullptr;
			return p;
		}		
	}
}