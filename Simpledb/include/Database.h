#pragma once
#include"Catalog.h"
#include"BufferPool.h"
#include"LogFile.h"
#include<string>
#include<atomic>
namespace Simpledb 
{	
	class Database 
	{
	public:
		~Database();
		/** Return the log file of the static Database instance */
		static LogFile* getLogFile() {
			return _instance->_pLogFile;
		}

		/** Return the buffer pool of the static Database instance */
		static BufferPool* getBufferPool() {
			return _instance->_pBufferPool;
		}

		/** Return the catalog of the static Database instance */
		static Catalog* getCatalog() {
			return _instance->_pCatalog;
		}
		/**
		* Method used for testing -- create a new instance of the buffer pool and
		* return it
		*/
		static BufferPool* resetBufferPool(int pages);
		// reset the database, used for unit tests only.
		static void reset();		
	private:
		Database();
		Database(const Database&) = delete;
		Database& operator=(const Database&) = delete;
		void setBufferPool(BufferPool* p) {
			_pBufferPool = p;
		}

		static Database *_instance;
		static string LOGFILENAME;
		LogFile* _pLogFile;
		Catalog* _pCatalog;
		BufferPool* _pBufferPool;				
	};
}