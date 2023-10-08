#pragma once
#include "BTreeLeafPage.h"
#include "BTreeFile.h"
#include <map>
namespace Simpledb {
	class BTreeChecker {
	private:
		class SubtreeSummary {
		public:
			int _depth;
			shared_ptr<BTreePageId> _ptrLeft;
			shared_ptr<BTreePageId> _leftmostId;
			shared_ptr<BTreePageId> _ptrRight;
			shared_ptr<BTreePageId> _rightmostId;

			SubtreeSummary();

			SubtreeSummary(shared_ptr<BTreeLeafPage> base, int depth);

			static shared_ptr<SubtreeSummary> checkAndMerge(
				shared_ptr<SubtreeSummary> accleft, shared_ptr<SubtreeSummary> right);

		};
		static shared_ptr<SubtreeSummary> checkSubTree(shared_ptr<BTreeFile> bt,
			shared_ptr<TransactionId> tid, map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages,
			shared_ptr<BTreePageId> pageId, shared_ptr<Field> lowerBound, shared_ptr<Field> upperBound,
			shared_ptr<BTreePageId> parentId, bool checkOccupancy, int depth);
	public:
		/**
		 * checks the integrity of the tree:
		 * 1) parent pointers.
		 * 2) sibling pointers.
		 * 3) range invariants.
		 * 4) record to page pointers.
		 * 5) occupancy invariants. (if enabled)
		 */
		static void checkRep(shared_ptr<BTreeFile> bt, shared_ptr<TransactionId> tid,
			map<shared_ptr<PageId>, shared_ptr<Page>>& dirtypages, bool checkOccupancy);

	};
}