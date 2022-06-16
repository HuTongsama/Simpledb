#include"Database.h"
namespace Simpledb {
	string Database::LOGFILENAME = "log";
	Database* Database::_instance = new Database();
	shared_ptr<BufferPool> Database::resetBufferPool(int pages)
	{
		_instance->_pBufferPool = make_shared<BufferPool>(pages);
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
		_pLogFile = make_shared<LogFile>(LOGFILENAME);
		_pCatalog = make_shared<Catalog>();
		_pBufferPool = make_shared<BufferPool>(BufferPool::DEFAULT_PAGES);	
	}
}