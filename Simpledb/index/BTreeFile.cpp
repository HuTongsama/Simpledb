#include"BTreeFile.h"
#include"BTreeHeaderPage.h"
#include"BufferPool.h"
namespace Simpledb {
	BTreeFile::BTreeFile(shared_ptr<File> f, int key, shared_ptr<TupleDesc> td)
	{
		_f = f;
		hash<string> str_hash;
		_tableid = str_hash(f->fileName());
		_keyField = key;
		_td = td;
	}
	shared_ptr<File> BTreeFile::getFile()
	{
		return _f;
	}
	size_t BTreeFile::getId()
	{
		return _tableid;
	}
	shared_ptr<TupleDesc> BTreeFile::getTupleDesc()
	{
		return _td;
	}
	shared_ptr<Page> BTreeFile::readPage(shared_ptr<PageId> pid)
	{
        shared_ptr<BTreePageId> id = dynamic_pointer_cast<BTreePageId>(pid);
        if (id == nullptr) {
            throw runtime_error("BTreePageId is nullptr");
        }
        try
        {
            
            if (id->pgcateg() == BTreePageId::ROOT_PTR) {

                int pageSize = BTreeRootPtrPage::getPageSize();
                _f->seek(0);
                vector<unsigned char> pageData = _f->readBytes(pageSize);               
                if (pageData.empty()) {
                    throw runtime_error("Read past end of table");
                }
                if (pageData.size() < pageSize) {
                    throw runtime_error("Unable to read " + to_string(pageSize) + " bytes from BTreeFile");
                }
                return make_shared<BTreeRootPtrPage>(id, pageData);
            }
            else {
                int pageSize = BufferPool::getPageSize();
                size_t skipSz = BTreeRootPtrPage::getPageSize() + (id->getPageNumber() - 1) * pageSize;
                int retVal = _f->seek(skipSz);
                if (retVal != 0) {
                    throw runtime_error("Unable to seek to correct place in BTreeFile");
                }
                vector<unsigned char> pageData = _f->readBytes(pageSize);
                if (pageData.empty()) {
                    throw runtime_error("Read past end of table");
                }
                if (pageData.size() < pageSize) {
                    throw runtime_error("Unable to read " + to_string(pageSize) + " bytes from BTreeFile");
                }
                if (id->pgcateg() == BTreePageId::INTERNAL) {
                    return make_shared<BTreeInternalPage>(id, pageData, _keyField);
                }
                else if (id->pgcateg() == BTreePageId::LEAF) {
                    return make_shared<BTreeLeafPage>(id, pageData, _keyField);
                }
                else { // id.pgcateg() == BTreePageId.HEADER
                    return make_shared<BTreeHeaderPage>(id, pageData);
                }
            }
        }
        catch (const std::exception& e)
        {
            throw e;
        }

        // Close the file on success or error
        // Ignore failures closing the file
	}
}