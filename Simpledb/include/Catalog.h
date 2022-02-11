#pragma once
#include"DbFile.h"
namespace Simpledb {
	/**
	* The Catalog keeps track of all available tables in the database and their
	* associated schemas.
	* For now, this is a stub catalog that must be populated with tables by a
	* user program before it can be used -- eventually, this should be converted
	* to a catalog that reads a catalog table from disk.
	*
	* @Threadsafe
	*/
	class Catalog {
	public:
		/**
		* Constructor.
		* Creates a new, empty catalog.
		*/
		Catalog() {}
		/**
		* Add a new table to the catalog.
		* This table's contents are stored in the specified DbFile.
		* @param file the contents of the table to add;  file.getId() is the identfier of
		*    this file/tupledesc param for the calls getTupleDesc and getFile
		* @param name the name of the table -- may be an empty string.  May not be null.  If a name
		* conflict exists, use the last table to be added as the table for a given name.
		* @param pkeyField the name of the primary key field
		*/
		void addTable(shared_ptr<DbFile> file, const string& name, const string& pkeyField);
		void addTable(shared_ptr<DbFile> file, const string& name);
		/**
		* Add a new table to the catalog.
		* This table has tuples formatted using the specified TupleDesc and its
		* contents are stored in the specified DbFile.
		* @param file the contents of the table to add;  file.getId() is the identfier of
		*    this file/tupledesc param for the calls getTupleDesc and getFile
		*/
		void addTable(shared_ptr<DbFile> file);
		/**
		* Return the id of the table with a specified name,
		* @throws -1 if the table doesn't exist
		*/
		int getTableId(const string& name);
		/**
		* Returns the tuple descriptor (schema) of the specified table
		* @param tableid The id of the table, as specified by the DbFile.getId()
		*     function passed to addTable
		* @returns nullptr if the table doesn't exist
		*/
		unique_ptr<TupleDesc> getTupleDesc(int tableid);
		/**
		* Returns the DbFile that can be used to read the contents of the
		* specified table.
		* @param tableid The id of the table, as specified by the DbFile.getId()
		*     function passed to addTable
		* @returns nullptr if the table doesn't exist
		*/
		unique_ptr<DbFile> getDatabaseFile(int tableid);
		string getPrimaryKey(int tableid);
		Iterator<int>* tableIdIterator();
		string getTableName(int id);
		void clear();
		/**
		* Reads the schema from a file and creates the appropriate tables in the database.
		* @param catalogFile
		*/
		bool loadSchema(const string& catalogFile);
	};
}