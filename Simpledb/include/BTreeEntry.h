#pragma once
#include"BTreePageId.h"
#include"Field.h"
#include"RecordId.h"
namespace Simpledb {
	class BTreeEntry {
	public:
		/**
		 * Constructor to create a new BTreeEntry
		 * @param key - the key
		 * @param leftChild - page id of the left child
		 * @param rightChild - page id of the right child
		 */
		BTreeEntry(shared_ptr<Field> key, shared_ptr<BTreePageId> leftChild, shared_ptr<BTreePageId> rightChild);
		/**
		 * @return the key
		 */
		shared_ptr<Field> getKey();
		/**
		 * @return the left child page id
		 */
		shared_ptr<BTreePageId> getLeftChild();
		/**
		 * @return the right child page id
		 */
		shared_ptr<BTreePageId> getRightChild();
		/**
		 * @return the record id of this entry, representing the location of this entry
		 * in a BTreeFile. May be null if this entry is not stored on any page in the file
		 */
		shared_ptr<RecordId> getRecordId();
		/**
		 * Set the key for this entry. Note that updating a BTreeEntry does not
		 * actually change the data stored on the page identified by its recordId.  After
		 * calling this method, you must call BTreeInternalPage.updateEntry() in order for
		 * it to take effect.
		 * @param key - the new key
		 * @see BTreeInternalPage#updateEntry(BTreeEntry)
		 */
		void setKey(shared_ptr<Field> key);
		/**
		 * Set the left child id for this entry.  Note that updating a BTreeEntry does not
		 * actually change the data stored on the page identified by its recordId.  After
		 * calling this method, you must call BTreeInternalPage.updateEntry() in order for
		 * it to take effect.
		 * @param leftChild - the new left child
		 * @see BTreeInternalPage#updateEntry(BTreeEntry)
		 */
		void setLeftChild(shared_ptr<BTreePageId> leftChild);
		/**
		 * Set the right child id for this entry.  Note that updating a BTreeEntry does not
		 * actually change the data stored on the page identified by its recordId.  After
		 * calling this method, you must call BTreeInternalPage.updateEntry() in order for
		 * it to take effect.
		 * @param rightChild - the new right child
		 * @see BTreeInternalPage#updateEntry(BTreeEntry)
		 */
		void setRightChild(shared_ptr<BTreePageId> rightChild);
		/**
		 * set the record id for this entry
		 * @param rid - the new record id
		 */
		void setRecordId(shared_ptr<RecordId> rid);
		/**
		 * Prints a representation of this BTreeEntry
		 */
		string toString();
	private:
		static const long _serialVersionUID = 1L;
		/**
		 * The key of this entry
		 * */
		shared_ptr<Field> _key;
		/**
		 * The left child page id
		 * */
		shared_ptr<BTreePageId> _leftChild;
		/**
		 * The right child page id
		 * */
		shared_ptr<BTreePageId> _rightChild;
		/**
		 * The record id of this entry
		 * */
		shared_ptr<RecordId> _rid; // null if not stored on any page

	};
}