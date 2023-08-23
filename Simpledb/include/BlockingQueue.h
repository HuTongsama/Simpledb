#pragma once
#include<mutex>
#include<vector>
namespace Simpledb {
    template <typename T>
	class BlockingQueue {
        std::mutex              _mutex;
        std::condition_variable _not_full;
        std::condition_variable _not_empty;
        int                     _start;
        int                     _end;
        int                     _capacity;
        int                     _sz;
        std::vector<T>          _vt;

        BlockingQueue(const BlockingQueue<T>& other) = delete;
        BlockingQueue<T>& operator=(const BlockingQueue<T>& other) = delete;
    public:      
        BlockingQueue(int capacity) : _capacity(capacity), _sz(capacity + 1), _vt(capacity + 1), _start(0), _end(0) {}
        bool isEmpty() {
            return _end == _start;
        }

        bool isFull() {
            return (_end + 1) % _sz == _start;
        }

        void push(const T& e) {
            std::unique_lock<std::mutex> lock(_mutex);
            while (isFull()) {
                _not_full.wait(lock);
            }
            _vt[_end] = e;
            _end = (_end + 1) % _sz;
            _not_empty.notify_one();
        }

        T pop() {
            std::unique_lock<std::mutex> lock(_mutex);
            while (isEmpty()) {
                _not_empty.wait(lock);
            }
            T& res = _vt[_start];
            _start = (_start + 1) % _sz;
            _not_full.notify_one();
            return res;
        }
	};
}