#pragma once
#include"BTreeFile.h"
#include"Tuple.h"
namespace Simpledb {

	class BTreeFileEncoder {
	public:
		struct TupleComparator
		{
			TupleComparator(int keyField);
			bool operator()(shared_ptr<Tuple> t1, shared_ptr<Tuple> t2) const;
		private:
			int _keyField;
		};
		struct EntryComparator {
			bool operator()(shared_ptr<BTreeEntry> e1, shared_ptr<BTreeEntry> e2)const;
		};

		struct ReverseEntryComparator
		{
			bool operator()(shared_ptr<BTreeEntry> e1, shared_ptr<BTreeEntry> e2)const;
		};

		/**
		* Encode the file using the BTreeFile's Insert method.
		*
		* @param tuples - list of tuples to add to the file
		* @param hFile - the file to temporarily store the data as a heap file on disk
		* @param bFile - the file on disk to back the resulting BTreeFile
		* @param keyField - the index of the key field for this B+ tree
		* @param numFields - the number of fields in each tuple
		* @return the BTreeFile
		*/
		static shared_ptr<BTreeFile> convert(const vector<vector<int>>& tuples, shared_ptr<File> hFile,
			shared_ptr<File> bFile, int keyField, int numFields);
		/**
		 * Encode the file using the BTreeFile's Insert method.
		 *
		 * @param inFile - the raw text file containing the tuples
		 * @param hFile - the file to temporarily store the data as a heap file on disk
		 * @param bFile - the file on disk to back the resulting BTreeFile
		 * @param keyField - the index of the key field for this B+ tree
		 * @param numFields - the number of fields in each tuple
		 * @return the BTreeFile
		 */
		static shared_ptr<BTreeFile> convert(shared_ptr<File> inFile,
			shared_ptr<File> hFile, shared_ptr<File> bFile,
			int keyField, int numFields);
		/**
		 * Faster method to encode the B+ tree file
		 *
		 * @param tuples - list of tuples to add to the file
		 * @param hFile - the file to temporarily store the data as a heap file on disk
		 * @param bFile - the file on disk to back the resulting BTreeFile
		 * @param npagebytes - number of bytes per page
		 * @param numFields - number of fields per tuple
		 * @param typeAr - array containing the types of the tuples
		 * @param fieldSeparator - character separating fields in the raw data file
		 * @param keyField - the field of the tuples the B+ tree will be keyed on
		 * @return the BTreeFile
		 */
		static shared_ptr<BTreeFile> convert(const vector<vector<int>>& tuples, shared_ptr<File> hFile,
			shared_ptr<File> bFile, int npagebytes, int numFields,
			const vector<shared_ptr<Type>>& typeAr, char fieldSeparator, int keyField);
		/**
		 * Faster method to encode the B+ tree file
		 *
		 * @param inFile - the file containing the raw data
		 * @param hFile - the data file for the HeapFile to be used as an intermediate conversion step
		 * @param bFile - the data file for the BTreeFile
		 * @param npagebytes - number of bytes per page
		 * @param numFields - number of fields per tuple
		 * @param typeAr - array containing the types of the tuples
		 * @param fieldSeparator - character separating fields in the raw data file
		 * @param keyField - the field of the tuples the B+ tree will be keyed on
		 * @return the B+ tree file
		 * @throws IOException
		 * @throws DbException
		 * @throws TransactionAbortedException
		 */
		static shared_ptr<BTreeFile> convert(shared_ptr<File> inFile, shared_ptr<File> hFile,
			shared_ptr<File> bFile, int npagebytes, int numFields,
			const vector<shared_ptr<Type>>& typeAr, char fieldSeparator, int keyField);
		/**
		 * Convert a set of tuples to a byte array in the format of a BTreeLeafPage
		 *
		 * @param tuples - the set of tuples
		 * @param npagebytes - number of bytes per page
		 * @param numFields - number of fields in each tuple
		 * @param typeAr - array containing the types of the tuples
		 * @param keyField - the field of the tuples the B+ tree will be keyed on
		 * @return a byte array which can be passed to the BTreeLeafPage constructor
		 * @throws IOException
		 */
		static vector<unsigned char> convertToLeafPage(vector<shared_ptr<Tuple>>& tuples, int npagebytes,
			int numFields, const vector<shared_ptr<Type>>& typeAr, int keyField);
		/**
		 * Convert a set of entries to a byte array in the format of a BTreeInternalPage
		 *
		 * @param entries - the set of entries
		 * @param npagebytes - number of bytes per page
		 * @param keyType - the type of the key field
		 * @param childPageCategory - the category of the child pages (either internal or leaf)
		 * @return a byte array which can be passed to the BTreeInternalPage constructor
		 * @throws IOException
		 */
		static vector<unsigned char> convertToInternalPage(vector<shared_ptr<BTreeEntry>>& entries, int npagebytes,
			shared_ptr<Type> keyType, int childPageCategory);
		/**
		 * Create a byte array in the format of a BTreeRootPtrPage
		 *
		 * @param root - the page number of the root page
		 * @param rootCategory - the category of the root page (leaf or internal)
		 * @param header - the page number of the first header page
		 * @return a byte array which can be passed to the BTreeRootPtrPage constructor
		 * @throws IOException
		 */
		static vector<unsigned char> convertToRootPtrPage(int root, int rootCategory, int header);
	private:
		/**
		 * Set all the right sibling pointers by following the left sibling pointers
		 *
		 * @param bf - the BTreeFile
		 * @param pid - the id of the page to update with the right sibling pointer
		 * @param rightSiblingId - the id of the page's right sibling
		 * @throws IOException
		 * @throws DbException
		 */
		static void setRightSiblingPtrs(shared_ptr<BTreeFile> bf, shared_ptr<BTreePageId> pid, shared_ptr<BTreePageId> rightSiblingId);
		/**
		 * Recursive function to set all the parent pointers
		 *
		 * @param bf - the BTreeFile
		 * @param pid - id of the page to update with the parent pointer
		 * @param parent - the id of the page's parent
		 * @throws IOException
		 * @throws DbException
		 */
		static void setParents(shared_ptr<BTreeFile> bf, shared_ptr<BTreePageId> pid, shared_ptr<BTreePageId> parent);
		/**
		 * Write out any remaining entries and update the parent pointers.
		 *
		 * @param entries - the list of remaining entries
		 * @param bf - the BTreeFile
		 * @param nentries - number of entries per page
		 * @param npagebytes - number of bytes per page
		 * @param keyType - the type of the key field
		 * @param tableid - the table id of this BTreeFile
		 * @param keyField - the index of the key field
		 * @throws IOException
		 */
		static void cleanUpEntries(vector<vector<shared_ptr<BTreeEntry>>>& entries,
			shared_ptr<BTreeFile> bf, int nentries, int npagebytes, shared_ptr<Type> keyType, size_t tableid,
			int keyField);
		/**
		 * Recursive function to update the entries by adding a new Entry at a particular level
		 *
		 * @param entries - the list of entries
		 * @param bf - the BTreefile
		 * @param e - the new entry
		 * @param level - the level of the new entry (0 is closest to the leaf pages)
		 * @param nentries - number of entries per page
		 * @param npagebytes - number of bytes per page
		 * @param keyType - the type of the key field
		 * @param tableid - the table id of this BTreeFile
		 * @param keyField - the index of the key field
		 * @throws IOException
		 */
		static void updateEntries(vector<vector<shared_ptr<BTreeEntry>>>& entries,
			shared_ptr<BTreeFile> bf, shared_ptr<BTreeEntry> e, int level, int nentries, int npagebytes,
			shared_ptr<Type> keyType, size_t tableid, int keyField);

	};
}