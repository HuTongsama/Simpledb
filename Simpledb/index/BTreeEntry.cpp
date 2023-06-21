#include"BTreeEntry.h"
namespace Simpledb {
	BTreeEntry::BTreeEntry(shared_ptr<Field> key, shared_ptr<BTreePageId> leftChild, shared_ptr<BTreePageId> rightChild)
		:_key(key), _leftChild(leftChild), _rightChild(rightChild)
	{
	}
	shared_ptr<Field> BTreeEntry::getKey()
	{
		return _key;
	}
	shared_ptr<BTreePageId> BTreeEntry::getLeftChild()
	{
		return _leftChild;
	}
	shared_ptr<BTreePageId> BTreeEntry::getRightChild()
	{
		return _rightChild;
	}
	shared_ptr<RecordId> BTreeEntry::getRecordId()
	{
		return _rid;
	}
	void BTreeEntry::setKey(shared_ptr<Field> key)
	{
		_key = key;
	}
	void BTreeEntry::setLeftChild(shared_ptr<BTreePageId> leftChild)
	{
		_leftChild = leftChild;
	}
	void BTreeEntry::setRightChild(shared_ptr<BTreePageId> rightChild)
	{
		_rightChild = rightChild;
	}
	void BTreeEntry::setRecordId(shared_ptr<RecordId> rid)
	{
		_rid = rid;
	}
	string BTreeEntry::toString()
	{
		return "[" + to_string(_leftChild->getPageNumber())
			+ "|" + _key->toString() + "|" +
			to_string(_rightChild->getPageNumber()) + "]";
	}
}