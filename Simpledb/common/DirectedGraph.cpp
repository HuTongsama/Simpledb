#include"DirectedGraph.h"
#include<algorithm>
#include<stdexcept>
#include<iostream>
namespace Simpledb {
	void DirectedGraph::addEdge(int64_t from, int64_t to)
	{
		lock_guard<mutex> guard(_graphMutex);
		auto iter = findVertex(from);
		if (iter == _vertexs.end()) {
			throw runtime_error("invalid vertex");
		}

		list<int64_t>& listRef = (*iter)->_adjacencyList;
		auto listIter = find(listRef.begin(), listRef.end(), to);
		if (listIter == listRef.end()) {
			listRef.push_back(to);
			_indegreeMap[to] += 1;
		}
	}

	void DirectedGraph::deleteEdge(int64_t from, int64_t to)
	{
		lock_guard<mutex> guard(_graphMutex);
		auto iter = findVertex(from);
		if (iter != _vertexs.end()) {
			list<int64_t>& listRef = (*iter)->_adjacencyList;
			auto listIter = find(listRef.begin(), listRef.end(), to);
			if (listIter != listRef.end()) {
				listRef.erase(listIter);
				_indegreeMap[to] -= 1;
			}
		}
	}

	void DirectedGraph::addVertex(int64_t v)
	{
		addVertexInner(v);
	}

	void DirectedGraph::deleteVertex(int64_t v)
	{
		lock_guard<mutex> guard(_graphMutex);
		auto iter = findVertex(v);
		if (iter != _vertexs.end()) {
			for (auto vertex : _vertexs) {
				if (vertex->_id == v)
					continue;
				deleteEdge(vertex->_id, v);
			}
			_indegreeMap.erase(v);
			_vertexs.erase(iter);
		}
	}

	bool DirectedGraph::isAcyclic(bool log)
	{
		lock_guard<mutex> guard(_graphMutex);
		queue<shared_ptr<Vertex>> vertexQueue;
		map<int64_t, size_t> indegreeMap = _indegreeMap;
		updateVertexQueue(vertexQueue, indegreeMap);
		int count = 0;
		if (log) {
			for (auto iter : indegreeMap) {
				cout << "tid :" << iter.first << ", count: " << iter.second << endl;
			}
		}
		while (!vertexQueue.empty()) {
			auto v = vertexQueue.front();
			vertexQueue.pop();
			count++;
			list<int64_t>& adjacencyList = v->_adjacencyList;
			for (auto vertex : adjacencyList) {
				_indegreeMap[vertex] -= 1;
			}
			updateVertexQueue(vertexQueue, indegreeMap);
		}
		if (count == _vertexs.size()) {
			if (log)
				cout << "true" << endl;
			return true;
		}
		if (log)
			cout << "false" << endl;
		return false;
	}

	vector<shared_ptr<DirectedGraph::Vertex>>::iterator DirectedGraph::findVertex(int64_t v)
	{
		return find_if(_vertexs.begin(), _vertexs.end(),
			[v](shared_ptr<Vertex> vertex) {
				return vertex->_id == v; });
	}

	vector<shared_ptr<DirectedGraph::Vertex>>::iterator DirectedGraph::addVertexInner(int64_t v)
	{
		auto iter = findVertex(v);
		if (iter != _vertexs.end()) {
			return iter;
		}
		shared_ptr<Vertex> p = make_shared<Vertex>(v);
		iter = _vertexs.emplace(_vertexs.end(), p);	
		_indegreeMap[v] = 0;
		
		return iter;
	}

	void DirectedGraph::updateVertexQueue(queue<shared_ptr<Vertex>>& vertexQueue,map<int64_t, size_t>& indegreeMap)
	{
		for (auto iter = indegreeMap.begin(); iter != indegreeMap.end();) {
			if (iter->second == 0) {
				auto v = findVertex(iter->first);
				vertexQueue.push(*v);
				iter = indegreeMap.erase(iter);
			}
			else {
				iter++;
			}
		}
	}

}

