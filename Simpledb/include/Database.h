#pragma once
#include"Catalog.h"
#include"BufferPool.h"
#include"LogFile.h"
#include<string>
#include<atomic>
#include<memory>
namespace Simpledb 
{	
	class Database 
	{
	public:
		/** Return the log file of the static Database instance */
		static shared_ptr<LogFile> getLogFile() {
			return _instance->_pLogFile;
		}

		/** Return the buffer pool of the static Database instance */
		static shared_ptr<BufferPool> getBufferPool() {
			return _instance->_pBufferPool;
		}

		/** Return the catalog of the static Database instance */
		static shared_ptr<Catalog> getCatalog() {
			return _instance->_pCatalog;
		}
		/**
		* Method used for testing -- create a new instance of the buffer pool and
		* return it
		*/
		static shared_ptr<BufferPool> resetBufferPool(int pages);
		// reset the database, used for unit tests only.
		static void reset();		
	private:
		Database();
		Database(const Database&) = delete;
		Database& operator=(const Database&) = delete;
		void setBufferPool(shared_ptr<BufferPool> p) {
			_pBufferPool = p;
		}

		static Database* _instance;
		static string LOGFILENAME;
		shared_ptr<LogFile> _pLogFile;
		shared_ptr<Catalog> _pCatalog;
		shared_ptr<BufferPool> _pBufferPool;				
	};
}