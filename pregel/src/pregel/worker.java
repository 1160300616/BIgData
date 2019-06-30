package pregel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import pregel.edge;
import pregel.vertex;

public class worker {
	
	// AF: vertices contains all the vertex in the graph
	// 		edges is the set of edge in the graph
	
	// RI: vertices in the edge must exit in vertices
	protected final Set<vertex> vertices = new HashSet<>();
	protected final Set<edge<vertex>> edges = new HashSet<>();
	
	
	public boolean addVertex(vertex v) {
	
			
			return vertices.add(v);
			
	}
	

	public boolean removeVertex(vertex v)
	{
			vertices.remove(v);
			Iterator<edge<vertex>> it = edges.iterator();
			while(it.hasNext()) 
			{
				if (it.next().containVertex(v))
				it.remove();
			}
			return true;
	}

	public Set<vertex> vertices() {
		Set<vertex> set = new HashSet<>();
		for (vertex vertex : vertices) {
			set.add(vertex);
		}
		return set;
	}


	public boolean addEdge(edge<vertex> edge) {
		edges.add(edge);
		return true;
		
	}
	

	public boolean removeEdge(edge<vertex> edge) {

		return	edges.remove(edge);

	}
	

	public Map<vertex, List<Integer>> sources(vertex target) {

		Map<vertex, List<Integer>> map = new HashMap<>();
		List<Integer> list;
		for (edge<vertex> edge : edges) {
			vertex targetVertex = edge.targetVertex();
			if (targetVertex.equals(target)) {
				vertex sourceVertex = edge.sourceVertex();
				if (map.keySet().contains(sourceVertex))	list = map.get(sourceVertex);
				else	list = new ArrayList<>();
				list.add(edge.getWeight());
				map.put(sourceVertex, list);
			}
		}
		return map;
	}
	

	public Map<vertex, List<Integer>> targets(vertex source) {

		Map<vertex, List<Integer>> map = new HashMap<>();
		List<Integer> list; 
		for (edge<vertex> edge : edges) 
		{
			vertex sourceVertex = edge.sourceVertex();
			if (sourceVertex.equals(source)) {
				vertex targetVertex = edge.targetVertex();
				if (map.containsKey(targetVertex))	list = map.get(targetVertex);
				else	list = new ArrayList<>();
				list.add(edge.getWeight());
				map.put(targetVertex, list);
			}
		}
		return map;
	}
	

	public Set<edge<vertex>> edges() {

		Set<edge<vertex>> set = new HashSet<>();
		for (edge<vertex> edge : edges) {
			set.add(edge);
		}

		return set;
	}
}
