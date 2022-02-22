#include"HeapPage.h"
namespace Simpledb {
	HeapPage::HeapPage(shared_ptr<HeapPageId> id, const vector<unsigned char>& data)
		:_pid(id)
	{
	}
	shared_ptr<PageId> HeapPage::getId()const
	{
		return nullptr;
	}
	shared_ptr<TransactionId> HeapPage::isDirty()const
	{
		return nullptr;
	}
	void HeapPage::markDirty(bool dirty, const TransactionId& tid)
	{
	}
	vector<unsigned char> HeapPage::getPageData()const
	{
		return vector<unsigned char>();
	}
	shared_ptr<Page> HeapPage::getBeforeImage()const
	{
		return nullptr;
	}
	void HeapPage::setBeforeImage()
	{
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
	const Iterator<Tuple>* HeapPage::iterator()
	{
		return nullptr;
	}
	int HeapPage::getNumTuples()
	{
		return 0;
	}
	int HeapPage::getHeaderSize()
	{
		return 0;
	}
	unique_ptr<Tuple> HeapPage::readNextTuple(const Unique_File& inFile, int slotId)
	{
		return unique_ptr<Tuple>();
	}
	void HeapPage::markSlotUsed(int i, bool value)
	{
	}
}