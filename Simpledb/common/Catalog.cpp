#include"Catalog.h"
#include"Common.h"
#include"HeapFile.h"
#include<fstream>
#pragma warning(disable:4996)
namespace Simpledb {
	void Catalog::addTable(shared_ptr<DbFile> file, const string& name, const string& pkeyField)
	{
	}
	void Catalog::addTable(shared_ptr<DbFile> file, const string& name)
	{
	}
	void Catalog::addTable(shared_ptr<DbFile> file)
	{
	}
	int Catalog::getTableId(const string& name)
	{
		return 0;
	}
	unique_ptr<TupleDesc> Catalog::getTupleDesc(int tableid)
	{
		return unique_ptr<TupleDesc>();
	}
	unique_ptr<DbFile> Catalog::getDatabaseFile(int tableid)
	{
		return unique_ptr<DbFile>();
	}
	string Catalog::getPrimaryKey(int tableid)
	{
		return string();
	}
	Iterator<int>* Catalog::tableIdIterator()
	{
		return nullptr;
	}
	string Catalog::getTableName(int id)
	{
		return string();
	}
	void Catalog::clear()
	{
	}
	bool Catalog::loadSchema(const string& catalogFile)
	{
        
        size_t found = catalogFile.find_last_of("/\\");
        if (found == string::npos)
            return false;
        string baseFolder = catalogFile.substr(0, found);
        try
        {
            ifstream iFile(catalogFile);
            string line = "";
            while (getline(iFile, line)) {
                size_t lParentheses = line.find_first_of("(");
                size_t rParentheses = line.find_last_of(")");
                string name = trim(line.substr(0, lParentheses));
                string fields = trim(line.substr(lParentheses + 1, rParentheses - lParentheses - 1));
                vector<string> els = split(fields, ",");
                vector<string> names;
                vector<shared_ptr<Type>> types;
                string primaryKey = "";
                for (auto& e : els) {
                    vector<string> els2 = split(trim(e), " ");
                    names.push_back(trim(els2[0]));
                    if (equalsIgnoreCase(trim(els2[1]), "int")){
                        types.push_back(Int_Type::INT_TYPE);
                    }
                    else if (equalsIgnoreCase(trim(els2[1]), "string")) {
                        types.push_back(String_Type::STRING_TYPE);
                    }
                    else{
                        cout << "Unknown type " << els2[1] << endl;
                        return false;
                    }

                    if (els2.size() == 3) {
                        if (trim(els2[2]) == ("pk"))
                            primaryKey = trim(els2[0]);
                        else {
                            cout << "Unknown annotation " << els2[2] << endl;
                            return false;
                        }
                    }
                }

                shared_ptr<TupleDesc> t = make_shared<TupleDesc>(types, names);
                string fileName = baseFolder + "/" + name + ".dat";
                File inFile(fileName, "ab+");
                shared_ptr<DbFile> tabHf(new HeapFile(inFile, t));
                addTable(tabHf, name, primaryKey);
                cout << "Added table : " << name << " with schema " << t->toString() << endl;
            }

        }
        catch (const std::exception& e)
        {
            cout << e.what() << endl;
            return false;
        }       
	}
}