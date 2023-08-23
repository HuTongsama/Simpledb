#include"BTreePage.h"
#include"Database.h"
#include"Type.h"
#include"BTreeRootPtrPage.h"
namespace Simpledb {
	const int BTreePage::INDEX_SIZE = Int_Type::INT_TYPE()->getLen();

	BTreePage::BTreePage(shared_ptr<BTreePageId> id, int key)
		:_pid(id), _keyField(key)
	{
		_td = Database::getCatalog()->getTupleDesc(id->getTableId());
	}
	shared_ptr<PageId> BTreePage::getId() const
	{
		return _pid;
	}
	vector<unsigned char> BTreePage::createEmptyPageData()
	{
		int len = BufferPool::getPageSize();
		return vector<unsigned char>(len, 0);
	}
	shared_ptr<BTreePageId> BTreePage::getParentId()
	{
		if (_parent == 0) {
			return BTreeRootPtrPage::getId(_pid->getTableId());
		}
		return make_shared<BTreePageId>(_pid->getTableId(), _parent, BTreePageId::INTERNAL);
	}
	void BTreePage::setParentId(shared_ptr<BTreePageId> id)
	{
		if (id == nullptr) {
			throw runtime_error("parent id must not be null");
		}
		if (id->getTableId() != _pid->getTableId()) {
			throw new runtime_error("table id mismatch in setParentId");
		}
		if (id->pgcateg() != BTreePageId::INTERNAL && id->pgcateg() != BTreePageId::ROOT_PTR) {
			throw runtime_error("parent must be an internal node or root pointer");
		}
		if (id->pgcateg() == BTreePageId::ROOT_PTR) {
			_parent = 0;
		}
		else {
			_parent = static_cast<int>(id->getPageNumber());
		}
	}
	void BTreePage::markDirty(bool dirty, shared_ptr<TransactionId> tid)
	{
		_dirty = dirty;
		if (dirty) 
			_dirtier = tid;
	}
	shared_ptr<TransactionId> BTreePage::isDirty()const
	{
		if (_dirty)
			return _dirtier;
		else
			return nullptr;
	}
}