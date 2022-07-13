#include"DirectedGraph.h"
#include<algorithm>
#include<stdexcept>
namespace Simpledb {
	void DirectedGraph::addEdge(int64_t from, int64_t to)
	{
		auto iter = addVertex(to);
		iter = addVertex(from);
		if (iter == _vertexs.end()) {
			throw runtime_error("add edge failed");
		}
		list<int64_t>& listRef = (*iter)->_adjacencyList;
		auto listIter = find(listRef.begin(), listRef.end(), to);
		if (listIter == listRef.end()) {
			listRef.push_back(to);
			if (_indegreeMap.find(to) == _indegreeMap.end()) {
				_indegreeMap[to] = 0;
			}
			_indegreeMap[to] += 1;
		}
	}

	void DirectedGraph::deleteEdge(int64_t from, int64_t to)
	{
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

	void DirectedGraph::deleteVertex(int64_t v)
	{
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

	bool DirectedGraph::isAcyclic()
	{
		return false;
	}

	vector<shared_ptr<DirectedGraph::Vertex>>::iterator DirectedGraph::findVertex(int64_t v)
	{
		return find_if(_vertexs.begin(), _vertexs.end(),
			[v](shared_ptr<Vertex> vertex) {
				return vertex->_id == v; });
	}

	vector<shared_ptr<DirectedGraph::Vertex>>::iterator DirectedGraph::addVertex(int64_t v)
	{
		auto iter = findVertex(v);
		if (iter != _vertexs.end()) {
			return iter;
		}
		shared_ptr<Vertex> p = make_shared<Vertex>(v);
		iter = _vertexs.emplace(_vertexs.end(), p);
		return iter;
	}

}

