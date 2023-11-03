#include "BTreeChecker.h"
#include <assert.h>
namespace Simpledb {
	BTreeChecker::SubtreeSummary::SubtreeSummary() : _depth(0)
	{
	}
	BTreeChecker::SubtreeSummary::SubtreeSummary(shared_ptr<BTreeLeafPage> base, int depth)
	{
		_depth = depth;

		_leftmostId = dynamic_pointer_cast<BTreePageId>(base->getId());
		_rightmostId = dynamic_pointer_cast<BTreePageId>(base->getId());

		_ptrLeft = dynamic_pointer_cast<BTreePageId>(base->getLeftSiblingId());
		_ptrRight = dynamic_pointer_cast<BTreePageId>(base->getRightSiblingId());
	}

	shared_ptr<BTreeChecker::SubtreeSummary> BTreeChecker::SubtreeSummary::checkAndMerge(
		shared_ptr<SubtreeSummary> accleft, shared_ptr<SubtreeSummary> right)
	{
		assert(accleft->_depth == right->_depth);
		assert(accleft->_ptrRight->equals(*right->_leftmostId));
		assert(accleft->_rightmostId->equals(*right->_ptrLeft));

		shared_ptr<SubtreeSummary> ans = make_shared<SubtreeSummary>();
		ans->_depth = accleft->_depth;

		ans->_ptrLeft = accleft->_ptrLeft;
		ans->_leftmostId = accleft->_leftmostId;

		ans->_ptrRight = right->_ptrRight;
		ans->_rightmostId = right->_rightmostId;
		return ans;
	}
	shared_ptr<BTreeChecker::SubtreeSummary> BTreeChecker::checkSubTree(BTreeFile* bt, shared_ptr<TransactionId> tid,
		map<BTreePageId, shared_ptr<Page>>& dirtypages, shared_ptr<BTreePageId> pageId, shared_ptr<Field> lowerBound,
		shared_ptr<Field> upperBound, shared_ptr<BTreePageId> parentId, bool checkOccupancy, int depth)
	{
		shared_ptr<BTreePage> page = dynamic_pointer_cast<BTreePage>
			(bt->getPage(tid, dirtypages, pageId, Permissions::READ_ONLY));
		assert(page->getParentId()->equals(*parentId));

		if (dynamic_pointer_cast<BTreePageId>(page->getId())->pgcateg() == BTreePageId::LEAF) {
			shared_ptr<BTreeLeafPage> bpage = dynamic_pointer_cast<BTreeLeafPage>(page);
			bpage->checkRep(bt->keyField(), lowerBound, upperBound, checkOccupancy, depth);
			return make_shared<SubtreeSummary>(bpage, depth);
		}
		else if (dynamic_pointer_cast<BTreePageId>(page->getId())->pgcateg() == BTreePageId::INTERNAL) {

			shared_ptr<BTreeInternalPage> ipage = dynamic_pointer_cast<BTreeInternalPage>(page);
			ipage->checkRep(lowerBound, upperBound, checkOccupancy, depth);

			shared_ptr<SubtreeSummary> acc;
			BTreeEntry* prev = nullptr;
			shared_ptr<Iterator<BTreeEntry>> it = ipage->iterator();

			prev = it->next();
			{ // init acc and prev.
				acc = checkSubTree(bt, tid, dirtypages, prev->getLeftChild(), lowerBound, prev->getKey(),
					dynamic_pointer_cast<BTreePageId>(ipage->getId()), checkOccupancy, depth + 1);
				lowerBound = prev->getKey();
			}

			assert(acc != nullptr);
			BTreeEntry* curr = prev; // for one entry case.
			while (it->hasNext()) {
				curr = it->next();
				shared_ptr<SubtreeSummary> currentSubTreeResult =
					checkSubTree(bt, tid, dirtypages, curr->getLeftChild(), lowerBound, curr->getKey(),
						dynamic_pointer_cast<BTreePageId>(ipage->getId()), checkOccupancy, depth + 1);
				acc = SubtreeSummary::checkAndMerge(acc, currentSubTreeResult);

				// need to move stuff for next iter:
				lowerBound = curr->getKey();
			}

			shared_ptr<SubtreeSummary> lastRight = checkSubTree(bt, tid, dirtypages,
				curr->getRightChild(), lowerBound, upperBound,
				dynamic_pointer_cast<BTreePageId>(ipage->getId()), checkOccupancy, depth + 1);
			acc = SubtreeSummary::checkAndMerge(acc, lastRight);

			return acc;
		}
		else {
			assert(false); // no other page types allowed inside the tree.
			return nullptr;
		}
	}
	void BTreeChecker::checkRep(BTreeFile* bt, shared_ptr<TransactionId> tid,
		map<BTreePageId, shared_ptr<Page>>& dirtypages, bool checkOccupancy)
	{
		shared_ptr<BTreeRootPtrPage> rtptr = dynamic_pointer_cast<BTreeRootPtrPage>
			(bt->getRootPtrPage(tid, dirtypages));

		if (rtptr->getRootId() == nullptr) { // non existent root is a legal state.
		}
		else {
			shared_ptr<SubtreeSummary> res = checkSubTree(bt, tid, dirtypages,
				rtptr->getRootId(), nullptr, nullptr, dynamic_pointer_cast<BTreePageId>(rtptr->getId()),
				checkOccupancy, 0);
			assert(res->_ptrLeft == nullptr);
			assert(res->_ptrRight == nullptr);
		}
	}
}