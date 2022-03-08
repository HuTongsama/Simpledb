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
	list<shared_ptr<Page>> HeapFile::insertTuple(const TransactionId& tid, const Tuple& t)
	{
		return list<shared_ptr<Page>>();
	}
	list<shared_ptr<Page>> HeapFile::deleteTuple(const TransactionId& tid, const Tuple& t)
	{
		return list<shared_ptr<Page>>();
	}
	shared_ptr<DbFileIterator> HeapFile::iterator(shared_ptr<TransactionId> tid)
	{
		shared_ptr<HeapFileIterator> iter = make_shared<HeapFileIterator>(getId(), tid);
		return iter;
	}
	size_t HeapFile::getPageOffset(size_t pageNo)
	{
		size_t pageSz = BufferPool::getPageSize();

		return pageNo * pageSz;
	}
	bool HeapFile::HeapFileIterator::hasNext()
	{
		if (_curPage == nullptr || _curIter == nullptr || !_isOpen) {
			return false;
		}
		if (_curIter->hasNext()) {
			return true;
		}
		else {
			readNextPage();
			return hasNext();
		}
		
	}
	
	Tuple& HeapFile::HeapFileIterator::next()
	{
		if (!_isOpen) {
			throw runtime_error("iter does not open");
		}
		return _curIter->next();
	}

	bool HeapFile::HeapFileIterator::open()
	{
		if (readNextPage()) {
			_isOpen = true;
		}
		return _isOpen;
	}
	
	bool HeapFile::HeapFileIterator::rewind()
	{
		_pageNo = 0;
		if (readNextPage()) {
			return true;
		}
		return false;
	}
	
	void HeapFile::HeapFileIterator::close()
	{
		_isOpen = false;
	}
	
	bool HeapFile::HeapFileIterator::readNextPage()
	{
		shared_ptr<HeapPageId> pid = make_shared<HeapPageId>(_tableId, _pageNo);
		_curPage = dynamic_pointer_cast<HeapPage>
			(Database::getBufferPool()->getPage(_tid, pid, Permissions::READ_WRITE));
		if (_curPage != nullptr) {
			_curIter = _curPage->iterator();
			_pageNo++;
			return true;
		}
		return false;
	}

}