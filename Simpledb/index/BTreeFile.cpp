#include"BTreeFile.h"
#include"BTreeHeaderPage.h"
#include"BTreeInternalPage.h"
#include"Database.h"

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
    void BTreeFile::writePage(shared_ptr<Page> p)
    {
        shared_ptr<BTreePageId> id = dynamic_pointer_cast<BTreePageId>(p->getId());

        vector<unsigned char> data = p->getPageData();
       
        if (id->pgcateg() == BTreePageId::ROOT_PTR) {
            _f->writeBytes(data.data(), data.size());
        }
        else {
            size_t pos = BTreeRootPtrPage::getPageSize() + (id->getPageNumber() - 1) * BufferPool::getPageSize();
            _f->seek(pos);            
            _f->writeBytes(data.data(), data.size());
        }
    }
    size_t BTreeFile::numPages()
    {
        // we only ever write full pages
        size_t pages = static_cast<size_t>(_f->length() - BTreeRootPtrPage::getPageSize()) / BufferPool::getPageSize();
        return pages;
    }
    int BTreeFile::keyField()
    {
        return _keyField;
    }
    shared_ptr<BTreeLeafPage> BTreeFile::splitLeafPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreeLeafPage> page, shared_ptr<Field> field)
    {
        return shared_ptr<BTreeLeafPage>();
    }
    shared_ptr<BTreeInternalPage> BTreeFile::splitInternalPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreeInternalPage> page, shared_ptr<Field> field)
    {
        return shared_ptr<BTreeInternalPage>();
    }
    vector<shared_ptr<Page>> BTreeFile::insertTuple(shared_ptr<TransactionId> tid, shared_ptr<Tuple> t)
    {
        map<shared_ptr<PageId>, shared_ptr<Page>> dirtypages;

        // get a read lock on the root pointer page and use it to locate the root page
        shared_ptr<BTreeRootPtrPage> rootPtr = getRootPtrPage(tid, dirtypages);
        shared_ptr<BTreePageId> rootId = rootPtr->getRootId();

        if (rootId == nullptr) { // the root has just been created, so set the root pointer to point to it		
            rootId = make_shared<BTreePageId>(_tableid, numPages(), BTreePageId::LEAF);
            rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
                (getPage(tid, dirtypages, BTreeRootPtrPage::getId(_tableid), Permissions::READ_WRITE));
            rootPtr->setRootId(rootId);
        }

        // find and lock the left-most leaf page corresponding to the key field,
        // and split the leaf page if there are no more slots available
        shared_ptr<BTreeLeafPage> leafPage = findLeafPage(tid, dirtypages, rootId, Permissions::READ_WRITE, t->getField(_keyField));
        if (leafPage->getNumEmptySlots() == 0) {
            leafPage = splitLeafPage(tid, dirtypages, leafPage, t->getField(_keyField));
        }

        // insert the tuple into the leaf page
        leafPage->insertTuple(t);
        vector<shared_ptr<Page>> result;
        for (auto& pagePair : dirtypages) {
            result.push_back(pagePair.second);
        }
        return result;
    }
    void BTreeFile::stealFromLeafPage(shared_ptr<BTreeLeafPage> page, shared_ptr<BTreeLeafPage> sibling, shared_ptr<BTreeInternalPage> parent, BTreeEntry* entry, bool isRightSibling)
    {
    }
    void BTreeFile::stealFromLeftInternalPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreeInternalPage> page, shared_ptr<BTreeInternalPage> leftSibling, shared_ptr<BTreeInternalPage> parent, BTreeEntry* parentEntry)
    {
    }
    void BTreeFile::stealFromRightInternalPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreeInternalPage> page, shared_ptr<BTreeInternalPage> rightSibling, shared_ptr<BTreeInternalPage> parent, BTreeEntry* parentEntry)
    {
    }
    void BTreeFile::mergeLeafPages(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreeLeafPage> leftPage, shared_ptr<BTreeLeafPage> rightPage, shared_ptr<BTreeInternalPage> parent, BTreeEntry* parentEntry)
    {
    }
    void BTreeFile::mergeInternalPages(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreeInternalPage> leftPage, shared_ptr<BTreeInternalPage> rightPage, shared_ptr<BTreeInternalPage> parent, BTreeEntry* parentEntry)
    {
    }
    shared_ptr<BTreeLeafPage> BTreeFile::findLeafPage(shared_ptr<TransactionId> tid, shared_ptr<BTreePageId> pid, shared_ptr<Field> f)
    {
        return shared_ptr<BTreeLeafPage>();
    }
    vector<shared_ptr<Page>> BTreeFile::deleteTuple(shared_ptr<TransactionId> tid, Tuple& t)
    {
        map<shared_ptr<PageId>, shared_ptr<Page>> dirtypages;

        shared_ptr<BTreePageId> pageId = make_shared<BTreePageId>(
            _tableid, t.getRecordId()->getPageId()->getPageNumber(), BTreePageId::LEAF);
        shared_ptr<BTreeLeafPage> page = dynamic_pointer_cast<BTreeLeafPage>
            (getPage(tid, dirtypages, pageId, Permissions::READ_WRITE));
        page->deleteTuple(t);

        // if the page is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        int maxEmptySlots = page->getMaxTuples() - page->getMaxTuples() / 2; // ceiling
        if (page->getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, page);
        }
        vector<shared_ptr<Page>> result;
        for (auto& pagePair : dirtypages) {
            result.push_back(pagePair.second);
        }
        return result;        
    }
    int BTreeFile::getEmptyPageNo(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages)
    {
        // get a read lock on the root pointer page and use it to locate the first header page
        shared_ptr<BTreeRootPtrPage> rootPtr = getRootPtrPage(tid, dirtypages);
        shared_ptr<BTreePageId> headerId = rootPtr->getHeaderId();
        int emptyPageNo = 0;

        if (headerId != nullptr) {
            shared_ptr<BTreeHeaderPage> headerPage = dynamic_pointer_cast<BTreeHeaderPage>
                (getPage(tid, dirtypages, headerId, Permissions::READ_ONLY));
            int headerPageCount = 0;
            // try to find a header page with an empty slot
            while (headerPage != nullptr && headerPage->getEmptySlot() == -1) {
                headerId = headerPage->getNextPageId();
                if (headerId != nullptr) {
                    headerPage = dynamic_pointer_cast<BTreeHeaderPage>
                        (getPage(tid, dirtypages, headerId, Permissions::READ_ONLY));
                    headerPageCount++;
                }
                else {
                    headerPage = nullptr;
                }
            }

            // if headerPage is not null, it must have an empty slot
            if (headerPage != nullptr) {
                headerPage = dynamic_pointer_cast<BTreeHeaderPage>
                    (getPage(tid, dirtypages, headerId, Permissions::READ_WRITE));
                int emptySlot = headerPage->getEmptySlot();
                headerPage->markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage::getNumSlots() + emptySlot;
            }
        }

        // at this point if headerId is null, either there are no header pages 
        // or there are no free slots
        if (headerId == nullptr) {
            lock_guard<mutex> lock(_dbfileMutex);
            // create the new page
            vector<unsigned char> emptyData = BTreeInternalPage::createEmptyPageData();
            _f->writeBytes(emptyData.data(), emptyData.size());
            emptyPageNo = static_cast<int>(numPages());
        }

        return emptyPageNo;
    }
    void BTreeFile::setEmptyPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, int emptyPageNo)
    {
        // if this is the last page in the file (and not the only page), just 
        // truncate the file
        // @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
        //		synchronized(this) {
        //			if(emptyPageNo == numPages()) {
        //				if(emptyPageNo <= 1) {
        //					// if this is the only page in the file, just return.
        //					// It just means we have an empty root page
        //					return;
        //				}
        //				long newSize = f.length() - BufferPool.getPageSize();
        //				FileOutputStream fos = new FileOutputStream(f, true);
        //				FileChannel fc = fos.getChannel();
        //				fc.truncate(newSize);
        //				fc.close();
        //				fos.close();
        //				return;
        //			}
        //		}

        // otherwise, get a read lock on the root pointer page and use it to locate 
        // the first header page
        shared_ptr<BTreeRootPtrPage> rootPtr = getRootPtrPage(tid, dirtypages);
        shared_ptr<BTreePageId> headerId = rootPtr->getHeaderId();
        shared_ptr<BTreePageId> prevId = nullptr;
        int headerPageCount = 0;

        // if there are no header pages, create the first header page and update
        // the header pointer in the BTreeRootPtrPage
        if (headerId == nullptr) {
            rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
                (getPage(tid, dirtypages, BTreeRootPtrPage::getId(_tableid), Permissions::READ_WRITE));

            shared_ptr<BTreeHeaderPage> headerPage = 
                dynamic_pointer_cast<BTreeHeaderPage>(getEmptyPage(tid, dirtypages, BTreePageId::HEADER));
            headerId = dynamic_pointer_cast<BTreePageId>(headerPage->getId());
            headerPage->init();
            rootPtr->setHeaderId(headerId);
        }

        // iterate through all the existing header pages to find the one containing the slot
        // corresponding to emptyPageNo
        while (headerId != nullptr && (headerPageCount + 1) * BTreeHeaderPage::getNumSlots() < emptyPageNo) {
            shared_ptr<BTreeHeaderPage> headerPage = 
                dynamic_pointer_cast<BTreeHeaderPage>(getPage(tid, dirtypages, headerId, Permissions::READ_ONLY));
            prevId = headerId;
            headerId = headerPage->getNextPageId();
            headerPageCount++;
        }

        // at this point headerId should either be null or set with 
        // the headerPage containing the slot corresponding to emptyPageNo.
        // Add header pages until we have one with a slot corresponding to emptyPageNo
        while ((headerPageCount + 1) * BTreeHeaderPage::getNumSlots() < emptyPageNo) {
            shared_ptr<BTreeHeaderPage> prevPage =
                dynamic_pointer_cast<BTreeHeaderPage>(getPage(tid, dirtypages, prevId, Permissions::READ_WRITE));

            shared_ptr<BTreeHeaderPage> headerPage = 
                dynamic_pointer_cast<BTreeHeaderPage>(getEmptyPage(tid, dirtypages, BTreePageId::HEADER));
            headerId = dynamic_pointer_cast<BTreePageId>(headerPage->getId());
            headerPage->init();
            headerPage->setPrevPageId(prevId);
            prevPage->setNextPageId(headerId);

            headerPageCount++;
            prevId = headerId;
        }

        // now headerId should be set with the headerPage containing the slot corresponding to 
        // emptyPageNo
        shared_ptr<BTreeHeaderPage> headerPage = 
            dynamic_pointer_cast<BTreeHeaderPage>(getPage(tid, dirtypages, headerId, Permissions::READ_WRITE));
        int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage::getNumSlots();
        headerPage->markSlotUsed(emptySlot, false);
    }
    shared_ptr<DbFileIterator> BTreeFile::indexIterator(shared_ptr<TransactionId> tid, IndexPredicate ipred)
    {
        return make_shared<BTreeSearchIterator>(this, tid, ipred);
    }
    shared_ptr<DbFileIterator> BTreeFile::iterator(shared_ptr<TransactionId> tid)
    {
        return make_shared<BTreeFileIterator>(this, tid);
    }
    shared_ptr<BTreeLeafPage> BTreeFile::findLeafPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreePageId> pid, Permissions perm, shared_ptr<Field> f)
    {
        return shared_ptr<BTreeLeafPage>();
    }
    shared_ptr<BTreeInternalPage> BTreeFile::getParentWithEmptySlots(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreePageId> parentId, shared_ptr<Field> field)
    {
        shared_ptr<BTreeInternalPage> parent = nullptr;

        // create a parent node if necessary
        // this will be the new root of the tree
        if (parentId->pgcateg() == BTreePageId::ROOT_PTR) {
            parent = dynamic_pointer_cast<BTreeInternalPage>(getEmptyPage(tid, dirtypages, BTreePageId::INTERNAL));

            // update the root pointer
            shared_ptr<BTreeRootPtrPage> rootPtr = 
                dynamic_pointer_cast<BTreeRootPtrPage>(getPage(tid, dirtypages,
                BTreeRootPtrPage::getId(_tableid), Permissions::READ_WRITE));
            shared_ptr<BTreePageId> prevRootId = rootPtr->getRootId(); //save prev id before overwriting.
            rootPtr->setRootId(dynamic_pointer_cast<BTreePageId>(parent->getId()));

            // update the previous root to now point to this new root.
            shared_ptr<BTreePage> prevRootPage = dynamic_pointer_cast<BTreePage>
                (getPage(tid, dirtypages, prevRootId, Permissions::READ_WRITE));
            prevRootPage->setParentId(dynamic_pointer_cast<BTreePageId>(parent->getId()));
        }
        else {
            // lock the parent page
            parent = dynamic_pointer_cast<BTreeInternalPage>
                (getPage(tid, dirtypages, parentId, Permissions::READ_WRITE));
        }

        // split the parent if needed
        if (parent->getNumEmptySlots() == 0) {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }

        return parent;
    }
    void BTreeFile::updateParentPointer(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>,
        shared_ptr<Page>>& dirtypages, shared_ptr<BTreePageId> pid, shared_ptr<BTreePageId> child)
    {
        shared_ptr<BTreePage> p = 
            dynamic_pointer_cast<BTreePage>(getPage(tid, dirtypages, child, Permissions::READ_ONLY));

        if (!p->getParentId()->equals(*pid)) {
            p = dynamic_pointer_cast<BTreePage>(getPage(tid, dirtypages, child, Permissions::READ_WRITE));
            p->setParentId(pid);
        }
    }
    void BTreeFile::updateParentPointers(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, 
        shared_ptr<Page>>& dirtypages, shared_ptr<BTreeInternalPage> page)
    {
        shared_ptr<Iterator<BTreeEntry>> it = page->iterator();
        shared_ptr<BTreePageId> pid = dynamic_pointer_cast<BTreePageId>(page->getId());
        BTreeEntry* e = nullptr;
        while (it->hasNext()) {
            e = it->next();
            updateParentPointer(tid, dirtypages, pid, e->getLeftChild());
        }
        if (e != nullptr) {
            updateParentPointer(tid, dirtypages, pid, e->getRightChild());
        }
    }
    shared_ptr<Page> BTreeFile::getPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages,
        shared_ptr<BTreePageId> pid, Permissions perm)
    {
        if (dirtypages.find(pid) != dirtypages.end()) {
            return dirtypages[pid];
        }
        else {
            shared_ptr<Page> p = Database::getBufferPool()->getPage(tid, pid, perm);
            if (perm == Permissions::READ_WRITE) {
                dirtypages[pid] = p;
            }
            return p;
        }
    }
    void BTreeFile::handleMinOccupancyPage(shared_ptr<TransactionId> tid,
        map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, shared_ptr<BTreePage> page)
    {
        shared_ptr<BTreePageId> parentId = page->getParentId();
        BTreeEntry* leftEntry = nullptr;
        BTreeEntry* rightEntry = nullptr;
        shared_ptr<BTreeInternalPage> parent = nullptr;

        // find the left and right siblings through the parent so we make sure they have
        // the same parent as the page. Find the entries in the parent corresponding to 
        // the page and siblings
        if (parentId->pgcateg() != BTreePageId::ROOT_PTR) {
            parent = dynamic_pointer_cast<BTreeInternalPage>
                (getPage(tid, dirtypages, parentId, Permissions::READ_WRITE));
            shared_ptr<Iterator<BTreeEntry>> ite = parent->iterator();
            while (ite->hasNext()) {
                BTreeEntry* e = ite->next();
                if (e->getLeftChild()->equals(*(page->getId()))) {
                    rightEntry = e;
                    break;
                }
                else if (e->getRightChild()->equals(*(page->getId()))) {
                    leftEntry = e;
                }
            }
        }

        if (dynamic_pointer_cast<BTreePageId>(page->getId())->pgcateg() == BTreePageId::LEAF) {
            handleMinOccupancyLeafPage(tid, dirtypages, dynamic_pointer_cast<BTreeLeafPage>(page), parent, leftEntry, rightEntry);
        }
        else { // BTreePageId.INTERNAL
            handleMinOccupancyInternalPage(tid, dirtypages, dynamic_pointer_cast<BTreeInternalPage>(page), parent, leftEntry, rightEntry);
        }
    }
    void BTreeFile::handleMinOccupancyLeafPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages,
        shared_ptr<BTreeLeafPage> page, shared_ptr<BTreeInternalPage> parent, BTreeEntry* leftEntry, BTreeEntry* rightEntry)
    {
        shared_ptr<BTreePageId> leftSiblingId = nullptr;
        shared_ptr<BTreePageId> rightSiblingId = nullptr;
        if (leftEntry != nullptr) leftSiblingId = leftEntry->getLeftChild();
        if (rightEntry != nullptr) rightSiblingId = rightEntry->getRightChild();

        int maxEmptySlots = page->getMaxTuples() - page->getMaxTuples() / 2; // ceiling
        if (leftSiblingId != nullptr) {
            shared_ptr<BTreeLeafPage> leftSibling = 
                dynamic_pointer_cast<BTreeLeafPage>(getPage(tid, dirtypages, leftSiblingId, Permissions::READ_WRITE));
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (leftSibling->getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }
            else {
                stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }
        }
        else if (rightSiblingId != nullptr) {
            shared_ptr<BTreeLeafPage> rightSibling = 
                dynamic_pointer_cast<BTreeLeafPage>(getPage(tid, dirtypages, rightSiblingId, Permissions::READ_WRITE));
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (rightSibling->getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
            else {
                stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }
    void BTreeFile::handleMinOccupancyInternalPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages,
        shared_ptr<BTreeInternalPage> page, shared_ptr<BTreeInternalPage> parent, BTreeEntry* leftEntry, BTreeEntry* rightEntry)
    {
        shared_ptr<BTreePageId> leftSiblingId = nullptr;
        shared_ptr<BTreePageId> rightSiblingId = nullptr;
        if (leftEntry != nullptr) leftSiblingId = leftEntry->getLeftChild();
        if (rightEntry != nullptr) rightSiblingId = rightEntry->getRightChild();

        int maxEmptySlots = page->getMaxEntries() - page->getMaxEntries() / 2; // ceiling
        if (leftSiblingId != nullptr) {
            shared_ptr<BTreeInternalPage> leftSibling = 
                dynamic_pointer_cast<BTreeInternalPage>(getPage(tid, dirtypages, leftSiblingId, Permissions::READ_WRITE));
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (leftSibling->getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }
            else {
                stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
            }
        }
        else if (rightSiblingId != nullptr) {
            shared_ptr<BTreeInternalPage> rightSibling = 
                dynamic_pointer_cast<BTreeInternalPage>(getPage(tid, dirtypages, rightSiblingId, Permissions::READ_WRITE));
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (rightSibling->getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
            else {
                stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
        }
    }
    void BTreeFile::deleteParentEntry(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages,
        shared_ptr<BTreePage> leftPage, shared_ptr<BTreeInternalPage> parent, shared_ptr<BTreeEntry> parentEntry)
    {
        // delete the entry in the parent.  If
        // the parent is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        parent->deleteKeyAndRightChild(parentEntry);
        int maxEmptySlots = parent->getMaxEntries() - parent->getMaxEntries() / 2; // ceiling
        if (parent->getNumEmptySlots() == parent->getMaxEntries()) {
            // This was the last entry in the parent.
            // In this case, the parent (root node) should be deleted, and the merged 
            // page will become the new root
            shared_ptr<BTreePageId> rootPtrId = parent->getParentId();
            if (rootPtrId->pgcateg() != BTreePageId::ROOT_PTR) {
                throw runtime_error("attempting to delete a non-root node");
            }
            shared_ptr<BTreeRootPtrPage> rootPtr = 
                dynamic_pointer_cast<BTreeRootPtrPage>(getPage(tid, dirtypages, rootPtrId, Permissions::READ_WRITE));
            leftPage->setParentId(rootPtrId);
            rootPtr->setRootId(dynamic_pointer_cast<BTreePageId>(leftPage->getId()));

            // release the parent page for reuse
            setEmptyPage(tid, dirtypages, static_cast<int>(parent->getId()->getPageNumber()));
        }
        else if (parent->getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, parent);
        }
    }
    shared_ptr<BTreeRootPtrPage> BTreeFile::getRootPtrPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages)
    {
        lock_guard<mutex> lock(_dbfileMutex);
        if (_f->length() == 0) {
            // create the root pointer page and the root page
            vector<unsigned char> emptyRootPtrData = BTreeRootPtrPage::createEmptyPageData();
            vector<unsigned char> emptyLeafData = BTreeLeafPage::createEmptyPageData();         
            _f->writeBytes(emptyRootPtrData.data(), emptyRootPtrData.size());
            _f->writeBytes(emptyLeafData.data(), emptyLeafData.size());
        }

        // get a read lock on the root pointer page
        return dynamic_pointer_cast<BTreeRootPtrPage>
            (getPage(tid, dirtypages, BTreeRootPtrPage::getId(_tableid), Permissions::READ_ONLY));
    }
    shared_ptr<Page> BTreeFile::getEmptyPage(shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, int pgcateg)
    {
        // create the new page
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        shared_ptr<BTreePageId> newPageId = make_shared<BTreePageId>(_tableid, emptyPageNo, pgcateg);

        // write empty page to disk
        
        _f->seek(BTreeRootPtrPage::getPageSize() + (long)(emptyPageNo - 1) * BufferPool::getPageSize());
        vector<unsigned char> emptyData = BTreePage::createEmptyPageData();
        _f->writeBytes(emptyData.data(), emptyData.size());

        // make sure the page is not in the buffer pool	or in the local cache		
        Database::getBufferPool()->discardPage(newPageId);
        dirtypages.erase(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions::READ_WRITE);
    }

    BTreeFileIterator::BTreeFileIterator(BTreeFile* f, shared_ptr<TransactionId> tid)
    {
        _f = f;
        _tid = tid;
    }

    void BTreeFileIterator::open()
    {
        shared_ptr<BTreeRootPtrPage> rootPtr =
            dynamic_pointer_cast<BTreeRootPtrPage>(Database::getBufferPool()->getPage(
                _tid, BTreeRootPtrPage::getId(_f->getId()), Permissions::READ_ONLY));
        shared_ptr<BTreePageId> root = rootPtr->getRootId();
        _curp = _f->findLeafPage(_tid, root, nullptr);
        _it = _curp->iterator();
    }

    void BTreeFileIterator::rewind()
    {
        close();
        open();
    }

    void BTreeFileIterator::close()
    {
        AbstractDbFileIterator::close();
        _it = nullptr;
        _curp = nullptr;
    }

    Tuple* BTreeFileIterator::readNext()
    {
        if (_it != nullptr && !_it->hasNext())
            _it = nullptr;

        while (_it == nullptr && _curp != nullptr) {
            shared_ptr<BTreePageId> nextp = _curp->getRightSiblingId();
            if (nextp == nullptr) {
                _curp = nullptr;
            }
            else {
                _curp = dynamic_pointer_cast<BTreeLeafPage>(
                    Database::getBufferPool()->getPage(_tid, nextp, Permissions::READ_ONLY));
                _it = _curp->iterator();
                if (!_it->hasNext())
                    _it = nullptr;
            }
        }

        if (_it == nullptr)
            return nullptr;
        return _it->next();
    }


    BTreeSearchIterator::BTreeSearchIterator(BTreeFile* f, shared_ptr<TransactionId> tid, IndexPredicate ipred)
        :_f(f), _tid(tid), _ipred(ipred)
    {
    
    }
    void BTreeSearchIterator::open()
    {
        shared_ptr<BTreeRootPtrPage> rootPtr = dynamic_pointer_cast<BTreeRootPtrPage>
            (Database::getBufferPool()->getPage(_tid, BTreeRootPtrPage::getId(_f->getId()), Permissions::READ_ONLY));
        shared_ptr<BTreePageId> root = rootPtr->getRootId();
        if (_ipred.getOp() == Predicate::Op::EQUALS ||
            _ipred.getOp() == Predicate::Op::GREATER_THAN ||
            _ipred.getOp() == Predicate::Op::GREATER_THAN_OR_EQ) {
            _curp = _f->findLeafPage(_tid, root, _ipred.getField());
        }
        else {
            _curp = _f->findLeafPage(_tid, root, nullptr);
        }
        _it = _curp->iterator();
    }
    void BTreeSearchIterator::rewind()
    {
        close();
        open();
    }
    void BTreeSearchIterator::close()
    {
        AbstractDbFileIterator::close();
        _it = nullptr;
    }
    Tuple* BTreeSearchIterator::readNext()
    {
        while (_it != nullptr) {

            while (_it->hasNext()) {
                Tuple* t = _it->next();
                if (t->getField(_f->keyField())->compare(_ipred.getOp(), *(_ipred.getField()))) {
                    return t;
                }
                else if (_ipred.getOp() == Predicate::Op::LESS_THAN || _ipred.getOp() == Predicate::Op::LESS_THAN_OR_EQ) {
                    // if the predicate was not satisfied and the operation is less than, we have
                    // hit the end
                    return nullptr;
                }
                else if (_ipred.getOp() == Predicate::Op::EQUALS &&
                    t->getField(_f->keyField())->compare(Predicate::Op::GREATER_THAN, *(_ipred.getField()))) {
                    // if the tuple is now greater than the field passed in and the operation
                    // is equals, we have reached the end
                    return nullptr;
                }
            }

            shared_ptr<BTreePageId> nextp = _curp->getRightSiblingId();
            // if there are no more pages to the right, end the iteration
            if (nextp == nullptr) {
                return nullptr;
            }
            else {
                _curp = dynamic_pointer_cast<BTreeLeafPage>
                    (Database::getBufferPool()->getPage(_tid,
                    nextp, Permissions::READ_ONLY));
                _it = _curp->iterator();
            }
        }

        return nullptr;
    }
}