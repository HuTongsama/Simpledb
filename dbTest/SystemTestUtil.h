#pragma once
#include<string>
#include"boost/uuid/uuid.hpp"
#include"boost/uuid/uuid_io.hpp"
#include"boost/uuid/uuid_generators.hpp"
#include"boost/lexical_cast.hpp"
using namespace std;
class SystemTestUtil {
public:
	/**
	* Generates a unique string each time it is called.
	* @return a new unique UUID as a string, using boost
	*/
	static string getUUID() {
		return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
	}

};