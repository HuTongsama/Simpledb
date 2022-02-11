#include"PageFactory.h"
#include"HeapPage.h"
namespace Simpledb {
	shared_ptr<Page> PageFactory::CreatePage(const string& pageClassStr, shared_ptr<PageId> pid, File& f)
	{
		int readLen = f.readInt();
		if (readLen > 0) {
			vector<unsigned char> data = f.readBytes(readLen);
			if (!data.empty()) {
			
				if (pageClassStr == typeid(HeapPage).name()) {
					shared_ptr<HeapPageId> id = dynamic_pointer_cast<HeapPageId>(pid);
					return shared_ptr<Page>(new HeapPage(id, data));
				}
				else {
					return nullptr;
				}
			}
			else {
				return nullptr;
			}
		}
		else{
			return nullptr;
		}
	}
}