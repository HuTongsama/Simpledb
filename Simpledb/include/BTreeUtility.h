#pragma once
#include<vector>
#include"Tuple.h"
#include"IntField.h"
#include"BTreeEntry.h"
#include"BTreeFile.h"
#include"BlockingQueue.h"
#include"Common.h"
namespace Simpledb {

	class BTreeUtility {
	public:
		static const int MAX_RAND_VALUE = 1 << 16;
		static vector<int> tupleToList(const Tuple& tuple);
		/**
		 * @return a Tuple with a single IntField with value n and with
		 *   RecordId(BTreePageId(1,2, BTreePageId.LEAF), 3)
		 */
		static shared_ptr<Tuple> getBTreeTuple(int n);
		/**
		 * @return a Tuple with an IntField for every element of tupdata
		 *   and RecordId(BTreePageId(1, 2, BTreePageId.LEAF), 3)
		 */
		static shared_ptr<Tuple> getBTreeTuple(const vector<int>& tupdata);
		/**
		 * @return a Tuple with a 'width' IntFields each with value n and
		 *   with RecordId(BTreePageId(1, 2, BTreePageId.LEAF), 3)
		 */
		static shared_ptr<Tuple> getBTreeTuple(int n, int width);
		/**
		 * @return a BTreeEntry with an IntField with value n and with
		 *   RecordId(BTreePageId(1,2, BTreePageId.INTERNAL), 3)
		 */
		static shared_ptr<BTreeEntry> getBTreeEntry(int n);
		/**
		 * @return a BTreeEntry with an IntField with value n and with
		 *   RecordId(BTreePageId(tableid,2, BTreePageId.INTERNAL), 3)
		 */
		static shared_ptr<BTreeEntry> getBTreeEntry(int n, int tableid);
		/**
		 * @return a BTreeEntry with an IntField with value key and with
		 *   RecordId(BTreePageId(tableid,2, BTreePageId.INTERNAL), 3)
		 */
		static shared_ptr<BTreeEntry> getBTreeEntry(int n, int key, int tableid);
		/** @param columnSpecification Mapping between column index and value. */
		static shared_ptr<BTreeFile> createRandomBTreeFile(
			int columns, int rows, const map<int, int>& columnSpecification,
			vector<vector<int>>& tuples, int keyField);
		/**
		 * Generates a random B+ tree file for testing
		 * @param columns - number of columns
		 * @param rows - number of rows
		 * @param maxValue - the maximum random value in this B+ tree
		 * @param columnSpecification - optional column specification
		 * @param tuples - optional list of tuples to return
		 * @param keyField - the index of the key field
		 * @return a BTreeFile
		 */
		static shared_ptr<BTreeFile> createRandomBTreeFile(int columns, int rows,
			int maxValue, const map<int, int>& columnSpecification,
			vector<vector<int>>& tuples, int keyField);
		/**
		 * Generate a random set of tuples for testing
		 * @param columns - number of columns
		 * @param rows - number of rows
		 * @param maxValue - the maximum random value in this B+ tree
		 * @param columnSpecification - optional column specification
		 * @param tuples - list of tuples to return
		 */
		static void generateRandomTuples(int columns, int rows,
			int maxValue, const map<int, int>& columnSpecification,
			vector<vector<int>>& tuples);
		/**
		 * Generate a random set of tuples for testing
		 * @param columns - number of columns
		 * @param rows - number of rows
		 * @param minValue - the minimum random value in this B+ tree
		 * @param maxValue - the maximum random value in this B+ tree
		 * @param columnSpecification - optional column specification
		 * @param tuples - list of tuples to return
		 */
		static void generateRandomTuples(int columns, int rows,
			int minValue, int maxValue, const map<int, int>& columnSpecification,
			vector<vector<int>>& tuples);
		/**
		 * Generate a random set of entries for testing
		 * @param numKeys - number of keys
		 * @param minKey - the minimum key value
		 * @param maxKey - the maximum key value
		 * @param minChildPtr - the first child pointer
		 * @param childPointers - list of child pointers to return
		 * @param keys - list of keys to return
		 */
		static void generateRandomEntries(int numKeys, int minKey, int maxKey, int minChildPtr,
			vector<int>& childPointers, vector<int>& keys);
		/**
		 * Generate a random set of tuples for testing
		 * @param columns - number of columns
		 * @param rows - number of rows
		 * @param min - the minimum value
		 * @param max - the maximum value
		 * @return the list of tuples
		 */
		static vector<shared_ptr<Tuple>> generateRandomTuples(int columns, int rows, int min, int max);
		/**
		 * Generate a random set of entries for testing
		 * @param numKeys - the number of keys
		 * @param tableid - the tableid
		 * @param childPageCategory - the child page category (LEAF or INTERNAL)
		 * @param minKey - the minimum key value
		 * @param maxKey - the maximum key value
		 * @param minChildPtr - the first child pointer
		 * @return the list of entries
		 */
		static vector<shared_ptr<BTreeEntry>> generateRandomEntries(int numKeys, int tableid,
			int childPageCategory, int minKey, int maxKey, int minChildPtr);
		/**
		 * Get the number of tuples that can fit on a page with the specified number of integer fields
		 * @param columns - the number of columns
		 * @return the number of tuples per page
		 */
		static int getNumTuplesPerPage(int columns);
		/**
		 * Create a random leaf page for testing
		 * @param pid - the page id of the leaf page
		 * @param columns - the number of fields per tuple
		 * @param keyField - the index of the key field in each tuple
		 * @param min - the minimum value
		 * @param max - the maximum value
		 * @return the leaf page
		 */
		static shared_ptr<BTreeLeafPage> createRandomLeafPage(shared_ptr<BTreePageId> pid, int columns, int keyField, int min, int max);
		/**
		 * Create a random leaf page for testing
		 * @param pid - the page id of the leaf page
		 * @param columns - the number of fields per tuple
		 * @param keyField - the index of the key field in each tuple
		 * @param numTuples - the number of tuples to insert
		 * @param min - the minimum value
		 * @param max - the maximum value
		 * @return the leaf page
		 */
		static shared_ptr<BTreeLeafPage> createRandomLeafPage(shared_ptr<BTreePageId> pid, int columns, int keyField, int numTuples, int min, int max);
		/**
		 * The number of entries that can fit on a page with integer key fields
		 * @return the number of entries per page
		 */
		static int getNumEntriesPerPage();
		/**
		 * Create a random internal page for testing
		 * @param pid - the page id of the internal page
		 * @param keyField - the index of the key field in each tuple
		 * @param childPageCategory - the child page category (LEAF or INTERNAL)
		 * @param minKey - the minimum key value
		 * @param maxKey - the maximum key value
		 * @param minChildPtr - the first child pointer
		 * @return the internal page
		 */
		static shared_ptr<BTreeInternalPage> createRandomInternalPage(shared_ptr<BTreePageId> pid, int keyField,
			int childPageCategory, int minKey, int maxKey, int minChildPtr);
		/**
		 * Create a random internal page for testing
		 * @param pid - the page id of the internal page
		 * @param keyField - the index of the key field in each tuple
		 * @param childPageCategory - the child page category (LEAF or INTERNAL)
		 * @param numKeys - the number of keys to insert
		 * @param minKey - the minimum key value
		 * @param maxKey - the maximum key value
		 * @param minChildPtr - the first child pointer
		 * @return the internal page
		 */
		static shared_ptr<BTreeInternalPage> createRandomInternalPage(shared_ptr<BTreePageId> pid, int keyField,
			int childPageCategory, int numKeys, int minKey, int maxKey, int minChildPtr);
		/**
		 * creates a *non* random B+ tree file for testing
		 * @param columns - number of columns
		 * @param rows - number of rows
		 * @param columnSpecification - optional column specification
		 * @param tuples - optional list of tuples to return
		 * @param keyField - the index of the key field
		 * @return a BTreeFile
		 * @throws IOException
		 * @throws DbException
		 * @throws TransactionAbortedException
		 */
		static shared_ptr<BTreeFile> createBTreeFile(int columns, int rows,
			const map<int, int>& columnSpecification,
			vector<vector<int>>& tuples, int keyField);
		/** Opens a BTreeFile and adds it to the catalog.
		 *
		 * @param cols number of columns in the table.
		 * @param f location of the file storing the table.
		 * @param keyField the field the B+ tree is keyed on
		 * @return the opened table.
		 */
		static shared_ptr<BTreeFile> openBTreeFile(int cols, shared_ptr<File> f, int keyField);
		/**
		 * A utility method to create a new BTreeFile with no data,
		 * assuming the path does not already exist. If the path exists, the file
		 * will be overwritten. The new table will be added to the Catalog with
		 * the specified number of columns as IntFields indexed on the keyField.
		 */
		static shared_ptr<BTreeFile> createEmptyBTreeFile(const string& path, int cols, int keyField);
		/**
		 * A utility method to create a new BTreeFile with no data, with the specified
		 * number of pages, assuming the path does not already exist. If the path exists,
		 * the file will be overwritten. The new table will be added to the Catalog with
		 * the specified number of columns as IntFields indexed on the keyField.
		 */
		static shared_ptr<BTreeFile> createEmptyBTreeFile(const string& path, int cols, int keyField, int pages);

		class BTreeWriter : public Thread {
		private:
			shared_ptr<TransactionId> _tid;
			shared_ptr<BTreeFile> _bf;
			int _item = 0;
			int _count = 0;
			bool _success = false;
			mutex _slock;
			mutex _elock;

			void runInner()override;
		public:
			/**
			 * @param tid the transaction on whose behalf we want to insert the tuple
			 * @param bf the B+ tree file into which we want to insert the tuple
			 * @param item the key of the tuple to insert
			 * @param count the number of times to insert the tuple
			 */
			BTreeWriter(shared_ptr<TransactionId> tid, shared_ptr<BTreeFile> bf, int item, int count);
			/**
			 * @return true if we successfully inserted the tuple
			 */
			bool succeeded();
			/**
			 * @return an error string if one occurred while inserting the tuple;
			 */
			const string& getError();
		};

		class BTreeReader : public Thread {
		private:
			shared_ptr<TransactionId> _tid;
			shared_ptr<BTreeFile> _bf;
			shared_ptr<Field> _f;
			int _count = 0;
			bool _found = false;
			mutex _slock;
			mutex _elock;

			void runInner()override;
		public:
			/**
			 * @param tid the transaction on whose behalf we want to search for the tuple(s)
			 * @param bf the B+ tree file containing the tuple(s)
			 * @param f the field to search for
			 * @param count the number of tuples to search for
			 */
			BTreeReader(shared_ptr<TransactionId> tid, shared_ptr<BTreeFile> bf, shared_ptr<Field> f, int count);
			/**
			 * @return true if we successfully found the tuple(s)
			 */
			bool found();
			/**
			 * @return an error instance if one occurred while searching for the tuple(s);
			 */
			const string& getError();
		};

		class BTreeInserter : public Thread {
		private:
			shared_ptr<TransactionId> _tid;
			shared_ptr<BTreeFile> _bf;
			vector<int> _tupdata;
			shared_ptr<BlockingQueue<vector<int>>> _insertedTuples;
			bool _success = false;
			mutex _slock;
			mutex _elock;

			void runInner()override;
		public:
			/**
			 * @param bf the B+ tree file into which we want to insert the tuple
			 * @param tupdata the data of the tuple to insert
			 * @param insertedTuples the list of tuples that were successfully inserted
			 */
			BTreeInserter(shared_ptr<BTreeFile> bf, const vector<int>& tupdata, shared_ptr<BlockingQueue<vector<int>>> insertedTuples);
			/**
			 * @return true if we successfully inserted the tuple
			 */
			bool succeeded();
			/**
			 * @return an error instance if one occurred while inserting the tuple;
			 */
			const string& getError();
		};

		class BTreeDeleter : public Thread {
		private:
			shared_ptr<TransactionId> _tid;
			shared_ptr<BTreeFile> _bf;
			shared_ptr<BlockingQueue<vector<int>>> _insertedTuples;
			vector<int> _tuple;
			bool _success;
			mutex _slock;
			mutex _elock;

			void runInner()override;
		public:
			/**
			 * @param bf the B+ tree file from which we want to delete the tuple(s)
			 * @param insertedTuples the list of tuples to delete
			 */
			BTreeDeleter(shared_ptr<BTreeFile> bf, shared_ptr<BlockingQueue<vector<int>>> insertedTuples);
			/**
			 * @return true if we successfully deleted the tuple
			 */
			bool succeeded();
			/**
			 * @return an error instance if one occurred while deleting the tuple;
			 */
			const string& getError();
		};
	};
}