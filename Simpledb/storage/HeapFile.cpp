#include"HeapFile.h"
#include"Database.h"
#include<functional>
#pragma warning(disable:4996)
namespace Simpledb {
	HeapFile::HeapFile(shared_ptr<File> f, shared_ptr<TupleDesc> td)
		:_file(f), _td(td)
	{
	}
	shared_ptr<File> HeapFile::getFile()
	{
		return _file;
	}
	size_t HeapFile::getId()
	{
		hash<string> str_hash;
		string fileName = _file != nullptr ? _file->fileName() : "";
		return str_hash(fileName);
		

	}
	shared_ptr<TupleDesc> HeapFile::getTupleDesc()
	{
		return _td;
	}
	shared_ptr<Page> HeapFile::readPage(shared_ptr<PageId> pid)
	{
		if (_file == nullptr)
			return nullptr;
		size_t pageNo = pid->getPageNumber();
		size_t pageOffset = getPageOffset(pageNo);
		size_t pageSize = BufferPool::getPageSize();
		_file->seek(pageOffset);
		vector<unsigned char> pageData = _file->readBytes(pageSize);
		if (pageData.empty())
			return nullptr;
		shared_ptr<HeapPageId> hid = dynamic_pointer_cast<HeapPageId>(pid);
		shared_ptr<HeapPage> page = make_shared<HeapPage>(hid, pageData);
		return page;
	}
	void HeapFile::writePage(shared_ptr<Page> page)
	{
		if (_file == nullptr)
			return;
		size_t pageNo = page->getId()->getPageNumber();
		size_t pageOffset = getPageOffset(pageNo);
		size_t pageSize = BufferPool::getPageSize();
		_file->seek(pageOffset);
		_file->writeBytes(page->getPageData().data(), pageSize);
		return;
	}
	size_t HeapFile::numPages()
	{
		size_t fileSize = _file->length();
		size_t pageSize = BufferPool::getPageSize();
		size_t pageNum = pageSize == 0 ? 0 : fileSize / pageSize;
		return pageNum;
	}
	vector<shared_ptr<Page>> HeapFile::insertTuple(shared_ptr<TransactionId> tid, shared_ptr<Tuple> t)
	{	
		vector<shared_ptr<Page>> pages;
		if (nullptr == t)
			return pages;
		size_t num = 0;
		size_t tableId = getId();
		while (true) {
			shared_ptr<HeapPageId> pid = make_shared<HeapPageId>(tableId, num);
			shared_ptr<HeapPage> hp = dynamic_pointer_cast<HeapPage>(
				Database::getBufferPool()->getPage(tid, pid, Permissions::READ_WRITE));

			if (hp->getNumEmptySlots() > 0) {
				hp->insertTuple(t);
				pages.push_back(hp);
				break;
			}
			else {
				lock_guard<mutex> guard(_dbfileMutex);
				Database::getBufferPool()->unsafeReleasePage(tid, pid);
				num++;
				size_t curPageNum = numPages();
				if (num >= curPageNum) {
					size_t byteCount = BufferPool::getPageSize();
					vector<unsigned char> bytes(byteCount, 0);
					_file->writeBytes(bytes.data(), byteCount);
				}
			}
		}
	
		return pages;
	}
	vector<shared_ptr<Page>> HeapFile::deleteTuple(shared_ptr<TransactionId> tid, Tuple& t)
	{
		vector<shared_ptr<Page>> pages;
		size_t num = numPages();
		size_t tableId = getId();
		for (int i = 0; i < num; ++i) {
			shared_ptr<HeapPageId> pid = make_shared<HeapPageId>(tableId, i);
			shared_ptr<HeapPage> hp = dynamic_pointer_cast<HeapPage>(
				Database::getBufferPool()->getPage(tid, pid, Permissions::READ_WRITE));
			try
			{
				hp->deleteTuple(t);
				pages.push_back(hp);
				break;
			}
			catch (const std::exception&)
			{
				continue;
			}
		}
		return pages;
	}
	shared_ptr<DbFileIterator> HeapFile::iterator(shared_ptr<TransactionId> tid)
	{
		shared_ptr<HeapFileIterator> iter = make_shared<HeapFileIterator>(getId(), numPages(), tid);
		return iter;
	}
	size_t HeapFile::getPageOffset(size_t pageNo)
	{
		size_t pageSz = BufferPool::getPageSize();

		return pageNo * pageSz;
	}
	
	void HeapFile::HeapFileIterator::open()
	{
		_open = true;
	}
	
	void HeapFile::HeapFileIterator::rewind()
	{
		_pageNo = 0;
		_iter = nullptr;
	}
	
	void HeapFile::HeapFileIterator::close()
	{
		AbstractDbFileIterator::close();
		_open = false;
	}

	Tuple* HeapFile::HeapFileIterator::readNext()
	{
		if (!_open) {
			return nullptr;
		}
		if (_iter != nullptr && _iter->hasNext()) {
			return &(_iter->next());
		}
		else {
			if (_iter != nullptr) {
				_iter->close();
			}
			readNextPage();
			if (_iter == nullptr) {
				return nullptr;
			}
			return readNext();
		}
	}
	
	void HeapFile::HeapFileIterator::readNextPage()
	{
		if (_pageNo >= _pageCount) {
			_iter = nullptr;
			return;
		}
		shared_ptr<HeapPageId> pid = make_shared<HeapPageId>(_tableId, _pageNo);
		shared_ptr<HeapPage> _curPage = dynamic_pointer_cast<HeapPage>
			(Database::getBufferPool()->getPage(_tid, pid, Permissions::READ_ONLY));
		if (_curPage != nullptr) {
			_iter = _curPage->iterator();
			_iter->open();
			_pageNo++;
		}
		else {
			_iter = nullptr;
		}
		return;
	}

}