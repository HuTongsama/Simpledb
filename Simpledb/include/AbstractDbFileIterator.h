#pragma once
#include"DbFileIterator.h"
namespace Simpledb {
	/** Helper for implementing DbFileIterators. Handles hasNext()/next() logic. */
	class AbstractDbFileIterator : public DbFileIterator {
	public:
		bool hasNext()override;
		Tuple* next()override;
		/** If subclasses override this, they should call AbstractDbFileIterator::close(). */
		void close()override;
	protected:
		/** Reads the next tuple from the underlying source.
		* @return the next Tuple in the iterator, null if the iteration is finished.
		*/
		virtual Tuple* readNext() = 0;
	private:
		Tuple* _next = nullptr;
	};
}