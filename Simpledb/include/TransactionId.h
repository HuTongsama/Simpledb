#pragma once
#include<atomic>
using namespace std;

namespace Simpledb 
{
   /**
	* TransactionId is a class that contains the identifier of a transaction.
	*/
	class TransactionId
	{
	public:
		TransactionId()
		{
			_id = ++_counter;
			if (_counter == INT64_MAX)
			{
				_counter = 0;
			}
		}
		int64_t getId()const { return _id; }
		bool equals(const TransactionId& tid)
		{
			return tid._id == _id;
		}
		int64_t hashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + (int)(_id ^ ((uint64_t)_id >> 32));
			return result;
		}
	private:
		static atomic_int64_t _counter;		
		int64_t _id;
	};	
}