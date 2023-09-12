#include"BTreeFileEncoder.h"
#include"HeapFileEncoder.h"
#include"Database.h"
#include"Utility.h"
#include"BTreeUtility.h"
#include"Transaction.h"
namespace Simpledb {

	BTreeFileEncoder::TupleComparator::TupleComparator(int keyField)
		:_keyField(keyField)
	{
	}
	bool BTreeFileEncoder::TupleComparator::operator()(shared_ptr<Tuple> t1, shared_ptr<Tuple> t2) const
	{
		if (t1->getField(_keyField)->compare(Predicate::Op::LESS_THAN, *(t2->getField(_keyField)))) {
			return true;
		}
		return false;
	}


	bool BTreeFileEncoder::EntryComparator::operator()(shared_ptr<BTreeEntry> e1, shared_ptr<BTreeEntry> e2) const
	{
		if (e1->getKey()->compare(Predicate::Op::LESS_THAN, *(e2->getKey()))) {
			return true;
		}
		return false;
	}

	bool BTreeFileEncoder::ReverseEntryComparator::operator()(shared_ptr<BTreeEntry> e1, shared_ptr<BTreeEntry> e2) const
	{
		if (e1->getKey()->compare(Predicate::Op::GREATER_THAN, *(e2->getKey()))) {
			return true;
		}
		return false;
	}

	shared_ptr<BTreeFile> BTreeFileEncoder::convert(const vector<vector<int>>& tuples, shared_ptr<File> hFile,
		shared_ptr<File> bFile, int keyField, int numFields)
	{
		shared_ptr<File> tempInput = File::createTempFile();
		tempInput->deleteOnExit();
		for (auto& tuple : tuples) {
			int writtenFields = 0;
			for (int field : tuple) {
				writtenFields++;
				if (writtenFields > numFields) {
					string error("Tuple has more than " + to_string(numFields) + " fields: (" +
						Utility::vectorToString(tuple) + ")");
					throw runtime_error(error);
				}
				string str = to_string(field);
				tempInput->writeBytes((const unsigned char*)str.data(), str.size());
				if (writtenFields < numFields) {
					tempInput->writeChar(',');
				}
			}
			tempInput->writeChar('\n');
		}
		tempInput->seek(0);
		return convert(tempInput, hFile, bFile, keyField, numFields);
	}

	shared_ptr<BTreeFile> BTreeFileEncoder::convert(shared_ptr<File> inFile, shared_ptr<File> hFile,
		shared_ptr<File> bFile, int keyField, int numFields)
	{
		// convert the inFile to HeapFile first.
		HeapFileEncoder::convert(*inFile, *hFile, BufferPool::getPageSize(), numFields);
		shared_ptr<HeapFile> heapf = Utility::openHeapFile(numFields, hFile);

		// add the heap file to B+ tree file
		shared_ptr<BTreeFile> bf = BTreeUtility::openBTreeFile(numFields, bFile, keyField);

		try
		{
			shared_ptr<TransactionId> tid = make_shared<TransactionId>();
			shared_ptr<DbFileIterator> it = Database::getCatalog()->getDatabaseFile(heapf->getId())->iterator(tid);
			it->open();
			int count = 0;
			shared_ptr<Transaction> t = make_shared<Transaction>();
			while (it->hasNext()) {
				Tuple* tup = it->next();
				shared_ptr<Tuple> tup1 = make_shared<Tuple>(tup->getTupleDesc());
				tup->copyTo(*tup1);
				Database::getBufferPool()->insertTuple(t->getId(), bf->getId(), tup1);
				count++;
				if (count >= 40) {
					Database::getBufferPool()->flushAllPages();
					count = 0;
				}
				t->commit();
				t = make_shared<Transaction>();
			}
			it->close();
		}
		catch (const std::exception& e)
		{
			cout << "BTreeFileEncoder::convert: " << e.what() << endl;
			return bf;
		}
		try
		{
			Database::getBufferPool()->flushAllPages();
		}
		catch (const std::exception& e)
		{
			cout << "BTreeFileEncoder::convert: " << e.what() << endl;
		}
		return bf;
	}

	shared_ptr<BTreeFile> BTreeFileEncoder::convert(const vector<vector<int>>& tuples, shared_ptr<File> hFile,
		shared_ptr<File> bFile, int npagebytes, int numFields, const vector<shared_ptr<Type>>& typeAr, char fieldSeparator, int keyField)
	{
		shared_ptr<File> tempInput = File::createTempFile();
		tempInput->deleteOnExit();
		for (auto& tuple : tuples) {
			int writtenFields = 0;
			for (int field : tuple) {
				writtenFields++;
				if (writtenFields > numFields) {
					string error("Tuple has more than " + to_string(numFields) + " fields: (" +
						Utility::vectorToString(tuple) + ")");
					throw runtime_error(error);
				}
				string str = to_string(field);
				tempInput->writeBytes((const unsigned char*)str.data(), str.size());
				if (writtenFields < numFields) {
					tempInput->writeChar(',');
				}
			}
			tempInput->writeChar('\n');
		}
		tempInput->seek(0);
		return convert(tempInput, hFile, bFile, npagebytes,
			numFields, typeAr, fieldSeparator, keyField);
	}

	shared_ptr<BTreeFile> BTreeFileEncoder::convert(shared_ptr<File> inFile, shared_ptr<File> hFile, shared_ptr<File> bFile,
		int npagebytes, int numFields, const vector<shared_ptr<Type>>& typeAr, char fieldSeparator, int keyField)
	{
		// convert the inFile to HeapFile first.
		HeapFileEncoder::convert(*inFile, *hFile, BufferPool::getPageSize(), numFields);
		shared_ptr<HeapFile> heapf = Utility::openHeapFile(numFields, hFile);

		// read all the tuples from the heap file and sort them on the keyField
		vector<shared_ptr<Tuple>> tuples;
		shared_ptr<TransactionId> tid = make_shared<TransactionId>();
		shared_ptr<DbFileIterator> it = Database::getCatalog()->getDatabaseFile(heapf->getId())->iterator(tid);
		it->open();
		while (it->hasNext()) {
			Tuple* tup = it->next();
			shared_ptr<Tuple> tmp = make_shared<Tuple>(tup->getTupleDesc());
			tup->copyTo(*tmp);
			tuples.push_back(tmp);
		}
		it->close();
		TupleComparator tupleComparator(keyField);
		sort(tuples.begin(), tuples.end(), tupleComparator);

		// add the tuples to B+ tree file
		shared_ptr<BTreeFile> bf = BTreeUtility::openBTreeFile(numFields, bFile, keyField);
		shared_ptr<Type> keyType = typeAr[keyField];
		size_t tableid = bf->getId();

		int nrecbytes = 0;
		for (int i = 0; i < numFields; i++) {
			nrecbytes += typeAr[i]->getLen();
		}
		// pointerbytes: left sibling pointer, right sibling pointer, parent pointer
		int leafpointerbytes = 3 * BTreeLeafPage::INDEX_SIZE;
		int nrecords = (npagebytes * 8 - leafpointerbytes * 8) / (nrecbytes * 8 + 1);  //floor comes for free

		int nentrybytes = keyType->getLen() + BTreeInternalPage::INDEX_SIZE;
		// pointerbytes: one extra child pointer, parent pointer, child page category
		int internalpointerbytes = 2 * BTreeLeafPage::INDEX_SIZE + 1;
		int nentries = (npagebytes * 8 - internalpointerbytes * 8 - 1) / (nentrybytes * 8 + 1);  //floor comes for free

		vector<vector<shared_ptr<BTreeEntry>>> entries;

		// first add some bytes for the root pointer page
		bf->writePage(make_shared<BTreeRootPtrPage>(BTreeRootPtrPage::getId(tableid),
			BTreeRootPtrPage::createEmptyPageData()));

		// next iterate through all the tuples and write out leaf pages
		// and internal pages as they fill up.
		// We wait until we have two full pages of tuples before writing out the first page
		// so that we will not end up with any pages containing less than nrecords/2 tuples
		// (unless it's the only page)
		vector<shared_ptr<Tuple>> page1;
		vector<shared_ptr<Tuple>> page2;
		shared_ptr<BTreePageId> leftSiblingId;
		for (shared_ptr<Tuple> tup : tuples) {
			if (page1.size() < nrecords) {
				page1.push_back(tup);
			}
			else if (page2.size() < nrecords) {
				page2.push_back(tup);
			}
			else {
				// write out a page of records
				vector<unsigned char> leafPageBytes = convertToLeafPage(page1, npagebytes, numFields, typeAr, keyField);
				shared_ptr<BTreePageId> leafPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::LEAF);
				shared_ptr<BTreeLeafPage> leafPage = make_shared<BTreeLeafPage>(leafPid, leafPageBytes, keyField);
				leafPage->setLeftSiblingId(leftSiblingId);
				bf->writePage(leafPage);
				leftSiblingId = leafPid;

				// update the parent by "copying up" the next key
				shared_ptr<BTreeEntry> copyUpEntry = make_shared<BTreeEntry>(page2[0]->getField(keyField), leafPid, nullptr);
				updateEntries(entries, bf, copyUpEntry, 0, nentries, npagebytes,
					keyType, tableid, keyField);

				page1 = page2;
				page2.clear();
				page2.push_back(tup);
			}
		}

		// now we need to deal with the end cases. There are two options:
		// 1. We have less than or equal to a full page of records. Because of the way the code
		//    was written above, we know this must be the only page
		// 2. We have somewhere between one and two pages of records remaining.
		// For case (1), we write out the page 
		// For case (2), we divide the remaining records equally between the last two pages,
		// write them out, and update the parent's child pointers.
		shared_ptr<BTreePageId> lastPid;
		if (page2.size() == 0) {
			// write out a page of records - this is the root page
			vector<unsigned char> lastPageBytes = convertToLeafPage(page1, npagebytes, numFields, typeAr, keyField);
			lastPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::LEAF);
			shared_ptr<BTreeLeafPage> lastPage = make_shared<BTreeLeafPage>(lastPid, lastPageBytes, keyField);
			lastPage->setLeftSiblingId(leftSiblingId);
			bf->writePage(lastPage);
		}
		else {
			// split the remaining tuples in half
			int remainingTuples = page1.size() + page2.size();
			vector<shared_ptr<Tuple>> lastPg;
			vector<shared_ptr<Tuple>> secondToLastPg(page1.begin(), page1.begin() + remainingTuples / 2);
			lastPg.insert(lastPg.end(), page1.begin() + remainingTuples / 2, page1.begin() + page1.size());
			lastPg.insert(lastPg.end(), page2.begin(), page2.end());

			// write out the last two pages of records
			vector<unsigned char> secondToLastPageBytes = convertToLeafPage(secondToLastPg, npagebytes, numFields, typeAr, keyField);
			shared_ptr<BTreePageId> secondToLastPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::LEAF);
			shared_ptr<BTreeLeafPage> secondToLastPage = make_shared<BTreeLeafPage>(secondToLastPid, secondToLastPageBytes, keyField);
			secondToLastPage->setLeftSiblingId(leftSiblingId);
			bf->writePage(secondToLastPage);

			vector<unsigned char> lastPageBytes = convertToLeafPage(lastPg, npagebytes, numFields, typeAr, keyField);
			lastPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::LEAF);
			shared_ptr<BTreeLeafPage> lastPage = make_shared<BTreeLeafPage>(lastPid, lastPageBytes, keyField);
			lastPage->setLeftSiblingId(secondToLastPid);
			bf->writePage(lastPage);

			// update the parent by "copying up" the next key
			shared_ptr<BTreeEntry> copyUpEntry = make_shared<BTreeEntry>(lastPg[0]->getField(keyField), secondToLastPid, lastPid);
			updateEntries(entries, bf, copyUpEntry, 0, nentries, npagebytes,
				keyType, tableid, keyField);
		}

		// Write out the remaining internal pages
		cleanUpEntries(entries, bf, nentries, npagebytes, keyType, tableid, keyField);

		// update the root pointer to point to the last page of the file
		int root = bf->numPages();
		int rootCategory = (root > 1 ? BTreePageId::INTERNAL : BTreePageId::LEAF);
		vector<unsigned char> rootPtrBytes = convertToRootPtrPage(root, rootCategory, 0);
		bf->writePage(make_shared<BTreeRootPtrPage>(BTreeRootPtrPage::getId(tableid), rootPtrBytes));

		// set all the parent and sibling pointers
		setParents(bf, make_shared<BTreePageId>(tableid, root, rootCategory), BTreeRootPtrPage::getId(tableid));
		setRightSiblingPtrs(bf, lastPid, nullptr);

		Database::resetBufferPool(BufferPool::DEFAULT_PAGES);
		return bf;
	}

	vector<unsigned char> BTreeFileEncoder::convertToLeafPage(vector<shared_ptr<Tuple>>& tuples, int npagebytes, int numFields,
		const vector<shared_ptr<Type>>& typeAr, int keyField)
	{
		int nrecbytes = 0;
		for (int i = 0; i < numFields; i++) {
			nrecbytes += typeAr[i]->getLen();
		}
		// pointerbytes: left sibling pointer, right sibling pointer, parent pointer
		int pointerbytes = 3 * BTreeLeafPage::INDEX_SIZE;
		int nrecords = (npagebytes * 8 - pointerbytes * 8) / (nrecbytes * 8 + 1);  //floor comes for free

		//  per record, we need one bit; there are nrecords per page, so we need
		// nrecords bits, i.e., ((nrecords/32)+1) integers.
		int nheaderbytes = (nrecords / 8);
		if (nheaderbytes * 8 < nrecords)
			nheaderbytes++;  //ceiling
		int nheaderbits = nheaderbytes * 8;
		DataStream dos(npagebytes);


		// write out the pointers and the header of the page,
		// then sort the tuples on the keyField and write out the tuples.
		//
		// in the header, write a 1 for bits that correspond to records we've
		// written and 0 for empty slots.

		int recordcount = tuples.size();
		if (recordcount > nrecords)
			recordcount = nrecords;

		dos.writeInt(0); // parent pointer
		dos.writeInt(0); // left sibling pointer
		dos.writeInt(0); // right sibling pointer

		int i = 0;
		char headerbyte = 0;

		for (i = 0; i < nheaderbits; i++) {
			if (i < recordcount)
				headerbyte |= (1 << (i % 8));

			if (((i + 1) % 8) == 0) {
				dos.writeChar(headerbyte);
				headerbyte = 0;
			}
		}

		if (i % 8 > 0)
			dos.writeChar(headerbyte);
		TupleComparator tupleComparator(keyField);
		sort(tuples.begin(), tuples.end(), tupleComparator);
		for (int t = 0; t < recordcount; t++) {
			shared_ptr<TupleDesc> td = tuples[t]->getTupleDesc();
			for (int j = 0; j < td->numFields(); j++) {
				vector<unsigned char> data = tuples[t]->getField(j)->serialize();
				dos.write((char*)data.data(), data.size());
			}
		}

		// pad the rest of the page with zeroes
		for (i = 0; i < (npagebytes - (recordcount * nrecbytes + nheaderbytes + pointerbytes)); i++)
			dos.writeChar(0);

		return dos.toByteArray();
	}

	vector<unsigned char> BTreeFileEncoder::convertToInternalPage(vector<shared_ptr<BTreeEntry>>& entries,
		int npagebytes, shared_ptr<Type> keyType, int childPageCategory)
	{
		int nentrybytes = keyType->getLen() + BTreeInternalPage::INDEX_SIZE;
		// pointerbytes: one extra child pointer, parent pointer, child page category
		int pointerbytes = 2 * BTreeLeafPage::INDEX_SIZE + 1;
		int nentries = (npagebytes * 8 - pointerbytes * 8 - 1) / (nentrybytes * 8 + 1);  //floor comes for free

		//  per entry, we need one bit; there are nentries per page, so we need
		// nentries bits, plus 1 for the extra child pointer.
		int nheaderbytes = (nentries + 1) / 8;
		if (nheaderbytes * 8 < nentries + 1)
			nheaderbytes++;  //ceiling
		int nheaderbits = nheaderbytes * 8;

		DataStream dos(npagebytes);

		// write out the pointers and the header of the page,
		// then sort the entries and write them out.
		//
		// in the header, write a 1 for bits that correspond to entries we've
		// written and 0 for empty slots.
		int entrycount = entries.size();
		if (entrycount > nentries)
			entrycount = nentries;

		dos.writeInt(0); // parent pointer
		dos.writeChar((char)childPageCategory);

		int i = 0;
		char headerbyte = 0;

		for (i = 0; i < nheaderbits; i++) {
			if (i < entrycount + 1)
				headerbyte |= (1 << (i % 8));

			if (((i + 1) % 8) == 0) {
				dos.writeChar(headerbyte);
				headerbyte = 0;
			}
		}

		if (i % 8 > 0)
			dos.writeChar(headerbyte);
		EntryComparator entryComparator;
		sort(entries.begin(), entries.end(), entryComparator);
		for (int e = 0; e < entrycount; e++) {
			vector<unsigned char> data = entries[e]->getKey()->serialize();
			dos.write((char*)data.data(), data.size());
		}

		for (int e = entrycount; e < nentries; e++) {
			for (int j = 0; j < keyType->getLen(); j++) {
				dos.writeChar(0);
			}
		}

		dos.writeInt(static_cast<int>(entries[0]->getLeftChild()->getPageNumber()));
		for (int e = 0; e < entrycount; e++) {
			dos.writeInt(static_cast<int>(entries[e]->getRightChild()->getPageNumber()));
		}

		for (int e = entrycount; e < nentries; e++) {
			for (int j = 0; j < BTreeInternalPage::INDEX_SIZE; j++) {
				dos.writeChar(0);
			}
		}

		// pad the rest of the page with zeroes
		for (i = 0; i < (npagebytes - (nentries * nentrybytes + nheaderbytes + pointerbytes)); i++)
			dos.writeChar(0);

		return dos.toByteArray();

	}

	vector<unsigned char> BTreeFileEncoder::convertToRootPtrPage(int root, int rootCategory, int header)
	{
		DataStream dos(BTreeRootPtrPage::getPageSize());
		
		dos.writeInt(root); // root pointer
		dos.writeChar((char)rootCategory); // root page category

		dos.writeInt(header); // header pointer

		return dos.toByteArray();
	}

	void BTreeFileEncoder::setRightSiblingPtrs(shared_ptr<BTreeFile> bf, shared_ptr<BTreePageId> pid, shared_ptr<BTreePageId> rightSiblingId)
	{
		shared_ptr<BTreeLeafPage> page = dynamic_pointer_cast<BTreeLeafPage>(bf->readPage(pid));
		page->setRightSiblingId(rightSiblingId);
		shared_ptr<BTreePageId> leftSiblingId = page->getLeftSiblingId();
		bf->writePage(page);
		if (leftSiblingId != nullptr) {
			setRightSiblingPtrs(bf, leftSiblingId, dynamic_pointer_cast<BTreePageId>(page->getId()));
		}
	}

	void BTreeFileEncoder::setParents(shared_ptr<BTreeFile> bf, shared_ptr<BTreePageId> pid, shared_ptr<BTreePageId> parent)
	{
		if (pid->pgcateg() == BTreePageId::INTERNAL) {
			shared_ptr<BTreeInternalPage> page = dynamic_pointer_cast<BTreeInternalPage>(bf->readPage(pid));
			page->setParentId(parent);

			shared_ptr<Iterator<BTreeEntry>> it = page->iterator();
			BTreeEntry* e = nullptr;
			while (it->hasNext()) {
				e = it->next();
				setParents(bf, e->getLeftChild(), pid);
			}
			if (e != nullptr) {
				setParents(bf, e->getRightChild(), pid);
			}
			bf->writePage(page);
		}
		else { // pid.pgcateg() == BTreePageId.LEAF
			shared_ptr<BTreeLeafPage> page = dynamic_pointer_cast<BTreeLeafPage>(bf->readPage(pid));
			page->setParentId(parent);
			bf->writePage(page);
		}
	}

	void BTreeFileEncoder::cleanUpEntries(vector<vector<shared_ptr<BTreeEntry>>>& entries, shared_ptr<BTreeFile> bf,
		int nentries, int npagebytes, shared_ptr<Type> keyType, size_t tableid, int keyField)
	{
		// As with the leaf pages, there are two options:
		// 1. We have less than or equal to a full page of entries. Because of the way the code
		//    was written, we know this must be the root page
		// 2. We have somewhere between one and two pages of entries remaining.
		// For case (1), we write out the page 
		// For case (2), we divide the remaining entries equally between the last two pages,
		// write them out, and update the parent's child pointers.
		for (int i = 0; i < (int)entries.size(); i++) {
			int childPageCategory = (i == 0 ? BTreePageId::LEAF : BTreePageId::INTERNAL);
			int size = (int)entries[i].size();
			if (size <= nentries) {
				// write out a page of entries
				vector<unsigned char> internalPageBytes = convertToInternalPage(entries[i], npagebytes, keyType, childPageCategory);
				shared_ptr<BTreePageId> internalPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::INTERNAL);
				bf->writePage(make_shared<BTreeInternalPage>(internalPid, internalPageBytes, keyField));
			}
			else {
				// split the remaining entries in half
				vector<shared_ptr<BTreeEntry>> secondToLastPg(entries[i].begin(), entries[i].begin() + size / 2);
				vector<shared_ptr<BTreeEntry>> lastPg(entries[i].begin() + size / 2 + 1, entries[i].end());

				// write out the last two pages of entries
				vector<unsigned char> secondToLastPageBytes = convertToInternalPage(secondToLastPg, npagebytes, keyType, childPageCategory);
				shared_ptr<BTreePageId> secondToLastPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::INTERNAL);
				bf->writePage(make_shared<BTreeInternalPage>(secondToLastPid, secondToLastPageBytes, keyField));

				vector<unsigned char> lastPageBytes = convertToInternalPage(lastPg, npagebytes, keyType, childPageCategory);
				shared_ptr<BTreePageId> lastPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::INTERNAL);
				bf->writePage(make_shared<BTreeInternalPage>(lastPid, lastPageBytes, keyField));

				// update the parent by "pushing up" the next key
				shared_ptr<BTreeEntry> pushUpEntry = make_shared<BTreeEntry>(entries[i][size / 2]->getKey(), secondToLastPid, lastPid);
				updateEntries(entries, bf, pushUpEntry, i + 1, nentries, npagebytes,
					keyType, tableid, keyField);
			}

		}
	}

	void BTreeFileEncoder::updateEntries(vector<vector<shared_ptr<BTreeEntry>>>& entries, shared_ptr<BTreeFile> bf,
		shared_ptr<BTreeEntry> e, int level, int nentries, int npagebytes, shared_ptr<Type> keyType, size_t tableid, int keyField)
	{
		while (entries.size() <= level) {
			entries.push_back(vector<shared_ptr<BTreeEntry>>());
		}

		int childPageCategory = (level == 0 ? BTreePageId::LEAF : BTreePageId::INTERNAL);
		int size = entries[level].size();

		if (size > 0) {
			shared_ptr<BTreeEntry> prev = entries[level][size - 1];
			entries[level][size - 1] = make_shared<BTreeEntry>(prev->getKey(), prev->getLeftChild(), e->getLeftChild());
			if (size == nentries * 2 + 1) {
				// write out a page of entries
				vector<shared_ptr<BTreeEntry>> pageEntries(entries[level].begin(), entries[level].begin());
				vector<unsigned char> internalPageBytes = convertToInternalPage(pageEntries, npagebytes, keyType, childPageCategory);
				shared_ptr<BTreePageId> internalPid = make_shared<BTreePageId>(tableid, bf->numPages() + 1, BTreePageId::INTERNAL);
				bf->writePage(make_shared<BTreeInternalPage>(internalPid, internalPageBytes, keyField));

				// update the parent by "pushing up" the next key
				shared_ptr<BTreeEntry> pushUpEntry = make_shared<BTreeEntry>(entries[level][nentries]->getKey(), internalPid, nullptr);
				updateEntries(entries, bf, pushUpEntry, level + 1, nentries, npagebytes,
					keyType, tableid, keyField);
				vector<shared_ptr<BTreeEntry>> remainingEntries(entries[level].begin() + nentries + 1,entries[level].end());
				entries[level].clear();
				entries[level].insert(entries[level].end(),remainingEntries.begin(),remainingEntries.end());
			}
		}
		entries[level].push_back(e);
	}

}