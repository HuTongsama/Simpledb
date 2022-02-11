#pragma once
#include"Page.h"
#include"Common.h"
#include"File.h"
namespace Simpledb {
	class PageFactory {
	public:
		static shared_ptr<Page> CreatePage(const string& pageClassStr, shared_ptr<PageId> pid, File& f);
	private:
		PageFactory() = default;
		PageFactory(const PageFactory&) = delete;
		PageFactory& operator=(const PageFactory&) = delete;
	};
}