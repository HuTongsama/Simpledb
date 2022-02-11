#pragma once
#include"PageId.h"
#include"File.h"
namespace Simpledb {
	class PageIdFactory {
	public:
		static shared_ptr<PageId> CreatePageId(const string& idClassStr, File& f);
	private:
		PageIdFactory() = default;
		PageIdFactory(const PageIdFactory&) = delete;
		PageIdFactory& operator=(const PageIdFactory&) = delete;
	};
}