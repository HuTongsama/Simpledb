#pragma once
#include<vector>
#include<list>
#include<map>
#include<queue>
#include<memory>
#include<mutex>
using namespace std;
namespace Simpledb {
	class DirectedGraph {
	public:
		struct Vertex {
			int64_t _id;
			list<int64_t> _adjacencyList;

			Vertex(int64_t id) :_id(id) {}
		};
		void addEdge(int64_t from, int64_t to);
		void deleteEdge(int64_t from, int64_t to);
		void deleteVertex(int64_t v);
		bool isAcyclic();
	private:
		vector<shared_ptr<Vertex>>::iterator findVertex(int64_t v);
		vector<shared_ptr<Vertex>>::iterator addVertex(int64_t v);
		void updateVertexQueue(queue<shared_ptr<Vertex>>& vertexQueue,map<int64_t, size_t>& indegreeMap);


		vector<shared_ptr<Vertex>> _vertexs;
		map<int64_t, size_t> _indegreeMap;
		mutex _graphMutex;
	};
}