#include"Database.h"
namespace Simpledb {
	Database* Database::_instance = new Database();
	string Database::LOGFILENAME = "log";
	BufferPool* Database::resetBufferPool(int pages)
	{
		BufferPool* p = _instance->_pBufferPool;
		if (p) {
			delete p;
		}
		_instance->_pBufferPool = new BufferPool(pages);
		return _instance->_pBufferPool;
	}
	void Database::reset()
	{
		if (_instance) {
			delete _instance;
		}

		_instance = new Database();
	}
	Database::Database()
	{
		_pLogFile = new LogFile(LOGFILENAME);
		_pCatalog = new Catalog();
		_pBufferPool = new BufferPool(BufferPool::DEFAULT_PAGES);	
	}
	Database::~Database()
	{
		if (_pLogFile) {
			delete _pLogFile;
		}
		if (_pCatalog) {
			delete _pCatalog;
		}
		if (_pBufferPool) {
			delete _pBufferPool;
		}
	}
}