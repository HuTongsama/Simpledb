#include"BTreeRootPtrPage.h"
#include"DataStream.h"
namespace Simpledb {
	shared_ptr<BTreePageId> BTreeRootPtrPage::getId(int tableid)
	{
		return make_shared<BTreePageId>(tableid, 0, BTreePageId::ROOT_PTR);
	}
	vector<unsigned char> BTreeRootPtrPage::createEmptyPageData()
	{
		return vector<unsigned char>(PAGE_SIZE, 0);
	}
	int BTreeRootPtrPage::getPageSize()
	{
		return PAGE_SIZE;
	}
	BTreeRootPtrPage::BTreeRootPtrPage(shared_ptr<BTreePageId> id, const vector<unsigned char>& data)
		:_pid(id)
	{
		DataStream dis((const char*)data.data(), data.size());
		// read in the root pointer
		_root = dis.readInt();
		_rootCategory = dis.readChar();
		// read in the header pointer
		_header = dis.readInt();
		setBeforeImage();
	}
	void BTreeRootPtrPage::setBeforeImage()
	{
		_oldData = getPageData();
	}
	shared_ptr<PageId> BTreeRootPtrPage::getId() const
	{
		return _pid;
	}
	vector<unsigned char> BTreeRootPtrPage::getPageData()
	{
		unique_ptr<unsigned char[]> data(new unsigned char[PAGE_SIZE]());
		DataStream dos((const char *)data.get(), PAGE_SIZE);

		// write out the root pointer (page number of the root page)
		try {
			dos.writeInt(_root);
		}
		catch (const std::exception& e) {
			throw e;
		}

		// write out the category of the root page (leaf or internal)
		try {
			dos.writeChar((char)_rootCategory);
		}
		catch (const std::exception& e) {
			throw e;
		}

		// write out the header pointer (page number of the first header page)
		try {
			dos.writeInt(_header);
		}
		catch (const std::exception& e) {
			throw e;
		}

		//maybe need flush;
		return vector<unsigned char>(data.get(), data.get() + PAGE_SIZE);
	}
	void BTreeRootPtrPage::markDirty(bool dirty, shared_ptr<TransactionId> tid)
	{
		_dirty = dirty;
		if (dirty)
			_dirtier = tid;
	}
	shared_ptr<TransactionId> BTreeRootPtrPage::isDirty() const
	{
		if (_dirty)
			return _dirtier;
		else
			return nullptr;
	}
	shared_ptr<Page> BTreeRootPtrPage::getBeforeImage()
	{
		try {
			return make_shared<BTreeRootPtrPage>(_pid, _oldData);
		}
		catch (const std::exception& e) {
			throw e;
		}
		return nullptr;
	}
	shared_ptr<BTreePageId> BTreeRootPtrPage::getRootId()
	{
		if (_root == 0) {
			return nullptr;
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _root, _rootCategory);
	}
	void BTreeRootPtrPage::setRootId(shared_ptr<BTreePageId> id)
	{
		if (id == nullptr) {
			_root = 0;
		}
		else {
			if (id->getTableId() != _pid->getTableId()) {
				throw new runtime_error("table id mismatch in setRootId");
			}
			if (id->pgcateg() != BTreePageId::INTERNAL && id->pgcateg() != BTreePageId::LEAF) {
				throw new runtime_error("root must be an internal node or leaf node");
			}
			_root = id->getPageNumber();
			_rootCategory = id->pgcateg();
		}
	}
	shared_ptr<BTreePageId> BTreeRootPtrPage::getHeaderId()
	{
		if (_header == 0) {
			return nullptr;
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _header, BTreePageId::HEADER);
	}
	void BTreeRootPtrPage::setHeaderId(shared_ptr<BTreePageId> id)
	{
		if (id == nullptr) {
			_header = 0;
		}
		else {
			if (id->getTableId() != _pid->getTableId()) {
				throw runtime_error("table id mismatch in setHeaderId");
			}
			if (id->pgcateg() != BTreePageId::HEADER) {
				throw runtime_error("header must be of type BTreePageId.HEADER");
			}
			_header = id->getPageNumber();
		}
	}
}