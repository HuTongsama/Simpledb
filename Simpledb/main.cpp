#include<iostream>
#include<memory>
#include"Common.h"
#include"boost\uuid\uuid.hpp"
#include"boost\uuid\uuid_generators.hpp"
using namespace std;
using namespace Simpledb;

int main() 
{
	auto id = boost::uuids::random_generator()();

	cout << "hello world" << endl;
}