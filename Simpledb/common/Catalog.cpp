#include"Catalog.h"
#include"Common.h"
#include"HeapFile.h"
#include<fstream>
#pragma warning(disable:4996)
namespace Simpledb {
	void Catalog::addTable(shared_ptr<DbFile> file, const string& name, const string& pkeyField)
	{
        if (file == nullptr)
            return;
        Table* pOld = getTable(file->getId());
        if (pOld != nullptr) {
            _nameToTable.erase(pOld->_name);
        }
        Table table(file, name, pkeyField);
        _nameToTable[name] = table;
	}
	void Catalog::addTable(shared_ptr<DbFile> file, const string& name)
	{
        addTable(file, name, "");
	}
	void Catalog::addTable(shared_ptr<DbFile> file)
	{
        addTable(file, "", "");
	}
    void Catalog::removeTable(const string& name)
    {
        _nameToTable.erase(name);
    }
    void Catalog::removeTable(shared_ptr<DbFile> file)
    {
        if (file == nullptr)return;
        string name;
        for (auto& iter : _nameToTable) {
            if (iter.second._file->getId() == file->getId()) {
                name = iter.first;
                break;
            }
        }
        if (name != "")removeTable(name);
    }
    size_t Catalog::getTableId(const string& name)
	{
        Table* pTable = getTable(name);
        if (pTable != nullptr) {
            return pTable->_file->getId();
        }
        throw runtime_error("no such element");
	}
	shared_ptr<TupleDesc> Catalog::getTupleDesc(size_t tableid)
	{
        Table* pTable = getTable(tableid);
        if (pTable != nullptr) {
            return pTable->_file->getTupleDesc();
        }
        throw runtime_error("no such element");
	}
	shared_ptr<DbFile> Catalog::getDatabaseFile(size_t tableid)
	{
        Table* pTable = getTable(tableid);
        if (pTable != nullptr) {
            return pTable->_file;
        }
        throw runtime_error("no such element");
	}
	string Catalog::getPrimaryKey(size_t tableid)
	{
        Table* pTable = getTable(tableid);
        if (pTable != nullptr) {
            return pTable->_pkeyField;
        }
        throw runtime_error("no such element");
	}
    shared_ptr<Iterator<size_t>> Catalog::tableIdIterator()
	{
        vector<size_t> tableIds;
        for (auto& iter : _nameToTable) {
            tableIds.push_back(iter.second._file->getId());
        }
		return make_shared<Iterator<size_t>>(tableIds);
	}
	string Catalog::getTableName(size_t tableid)
	{
        Table* pTable = getTable(tableid);
        if (pTable) {
            return pTable->_name;
        }
		return string();
	}
	void Catalog::clear()
	{
        _nameToTable.clear();
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
                        types.push_back(Int_Type::INT_TYPE());
                    }
                    else if (equalsIgnoreCase(trim(els2[1]), "string")) {
                        types.push_back(String_Type::STRING_TYPE());
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
                shared_ptr<File> inFile = make_shared<File>(fileName);
                shared_ptr<DbFile> tabHf(new HeapFile(inFile, t));
                addTable(tabHf, name, primaryKey);
                cout << "Added table : " << name << " with schema " << t->toString() << endl;
            }
            return true;
        }
        catch (const std::exception& e)
        {
            cout << e.what() << endl;
            return false;
        }       
	}
    
    Catalog::Table* Catalog::getTable(const string& name)
    {
        if (_nameToTable.find(name) != _nameToTable.end()) {
            return &(_nameToTable[name]);
        }
        return nullptr;
    }

    Catalog::Table* Catalog::getTable(size_t tableid)
    {
        for (auto& iter : _nameToTable) {
            if (iter.second._file->getId() == tableid) {
                return &iter.second;
            }
        }
        return nullptr;
    }
}