#include"BTreeHeaderPage.h"
#include"DataStream.h"
#include"Type.h"
#include"IntField.h"
#include"BufferPool.h"
namespace Simpledb {
	const int BTreeHeaderPage::INDEX_SIZE = Int_Type::INT_TYPE()->getLen();

	BTreeHeaderPage::BTreeHeaderPage(shared_ptr<BTreePageId> id, const vector<unsigned char>& data) 
	{
		_pid = id;
		_numSlots = getNumSlots();
		DataStream dis((const char*)data.data(),data.size());

		// Read the next and prev pointers
		try {
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			_nextPage = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception& e) {
			throw e;
		}

		try {
			shared_ptr<Field> f = Int_Type::INT_TYPE()->parse(dis);
			_prevPage = dynamic_pointer_cast<IntField>(f)->getValue();
		}
		catch (const std::exception& e) {
			throw e;
		}

		// allocate and read the header slots of this page
		int headSz = getHeaderSize();
		_header = vector<unsigned char>(headSz,0);
		for (int i = 0; i < headSz; i++)
			_header[i] = dis.readChar();

		setBeforeImage();
	}

	void BTreeHeaderPage::init() 
	{
		std::fill(_header.begin(), _header.end(), 0xff);
	}

	int BTreeHeaderPage::getNumSlots() {
		return getHeaderSize() * 8;
	}

	shared_ptr<Page> BTreeHeaderPage::getBeforeImage() 
	{
		try
		{
			lock_guard<mutex> lock(_oldDataLock);
			return make_shared<BTreeHeaderPage>(_pid, _oldData);

		}
		catch (const std::exception& e)
		{
			throw e;
		}
	}

	void BTreeHeaderPage::setBeforeImage() 
	{
		lock_guard<mutex> lock(_oldDataLock);
		_oldData = getPageData();		
	}

	shared_ptr<PageId> BTreeHeaderPage::getId()const 
	{
		return _pid;
	}

	vector<unsigned char> BTreeHeaderPage::getPageData()
	{
		int len = BufferPool::getPageSize();
		unique_ptr<unsigned char[]> data(new unsigned char[len]());
		DataStream dos((const char*)data.get(), len);

		// write out the next and prev pointers
		try
		{
			dos.writeInt(_nextPage);
		}
		catch (const std::exception& e)
		{
			throw e;
		}
		
		try
		{
			dos.writeInt(_prevPage);
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


		return vector<unsigned char>(data.get(), data.get() + len);
	}

	vector<unsigned char> BTreeHeaderPage::createEmptyPageData() 
	{
		int len = BufferPool::getPageSize();
		return vector<unsigned char>(len, 0); //all 0
	}

	shared_ptr<BTreePageId> BTreeHeaderPage::getPrevPageId()
	{
		if (_prevPage == 0) {
			return nullptr;
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _prevPage, BTreePageId::HEADER);
	}

	shared_ptr<BTreePageId> BTreeHeaderPage::getNextPageId() 
	{
		if (_nextPage == 0) {
			return nullptr;
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _nextPage, BTreePageId::HEADER);
	}

	void BTreeHeaderPage::setPrevPageId(shared_ptr<BTreePageId> id) 
	{
		if (id == nullptr) {
			_prevPage = 0;
		}
		else {
			if (id->getTableId() != _pid->getTableId()) {
				throw runtime_error("table id mismatch in setPrevPageId");
			}
			if (id->pgcateg() != BTreePageId::HEADER) {
				throw runtime_error("prevPage must be a header page");
			}
			_prevPage = id->getPageNumber();
		}
	}

	void BTreeHeaderPage::setNextPageId(shared_ptr<BTreePageId> id)
	{
		if (id == nullptr) {
			_nextPage = 0;
		}
		else {
			if (id->getTableId() != _pid->getTableId()) {
				throw runtime_error("table id mismatch in setNextPageId");
			}
			if (id->pgcateg() != BTreePageId::HEADER) {
				throw runtime_error("nextPage must be a header page");
			}
			_nextPage = id->getPageNumber();
		}
	}

	void BTreeHeaderPage::markDirty(bool dirty, shared_ptr<TransactionId> tid)
	{
		_dirty = dirty;
		if (dirty)
			_dirtier = tid;
	}

	shared_ptr<TransactionId> BTreeHeaderPage::isDirty()const
	{
		if (_dirty)
			return _dirtier;
		else
			return nullptr;
	}

	bool BTreeHeaderPage::isSlotUsed(int i)
	{
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;
		return (_header[headerbyte] & (1 << headerbit)) != 0;
	}

	void BTreeHeaderPage::markSlotUsed(int i, bool value)
	{
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;

		if (value)
			_header[headerbyte] |= 1 << headerbit;
		else
			_header[headerbyte] &= (0xFF ^ (1 << headerbit));
	}

	int BTreeHeaderPage::getEmptySlot()
	{
		int sz = _header.size();
		for (int i = 0; i < sz; i++) {
			if ((int)_header[i] != 0xFF) {
				for (int j = 0; j < 8; j++) {
					if (!isSlotUsed(i * 8 + j)) {
						return i * 8 + j;
					}
				}
			}
		}
		return -1;
	}

	int BTreeHeaderPage::getHeaderSize()
	{
		// pointerBytes: nextPage and prevPage pointers
		int pointerBytes = 2 * INDEX_SIZE;
		return BufferPool::getPageSize() - pointerBytes;
	}
}