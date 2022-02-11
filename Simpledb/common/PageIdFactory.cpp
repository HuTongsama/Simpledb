#include"PageIdFactory.h"
#include"HeapPageId.h"
namespace Simpledb {
	shared_ptr<PageId> PageIdFactory::CreatePageId(const string& idClassStr, File& f)
	{
		int numIdArgs = f.readInt();
		if ( numIdArgs > 0) {
		
			vector<int> idArgs(numIdArgs, 0);
			for (int i = 0; i < numIdArgs; ++i) {
				idArgs[i] = f.readInt();
			}
			if (idClassStr == typeid(HeapPageId).name()) {
			
				return shared_ptr<PageId>(new HeapPageId(idArgs[0], idArgs[1]));
			}
			else {
				return nullptr;
			}
		}
		else {
			return nullptr;
		}
	}
}