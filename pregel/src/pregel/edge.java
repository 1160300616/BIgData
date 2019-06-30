package pregel;

import java.util.ArrayList;
import java.util.List;
import pregel.vertex;

@SuppressWarnings("hiding")
public  class edge<Vertex> {
	
	// AF: vertices is the set of vertex exit in the edge
	// RI��determined in the subclass
	
	protected final List<vertex> vertices = new ArrayList<>();
	protected String label;
	protected int weight;
	protected boolean directed;
	
	public edge(String label, int weight) {
		this.label = label;
		this.weight = weight;
	}
	
	
	public String getLabel() 
	{
		return this.label;
	}
	
	public void setLabel(String label) 
	{
		this.label = label;
	}
	
	public void setWeight(int weight) 
	{
		this.weight = weight;
	}
	
	public int getWeight() {
		return this.weight;
	}
	
	public boolean isDirected() {
		return directed;
	}

	public void setDirected(boolean directed) {
		this.directed = directed;
	}
	
	public void addVertices(List<vertex> vertices) {
		for (int i = 0; i < vertices.size(); i++) 
		{
			if (!this.vertices.contains(vertices.get(i)))
				this.vertices.add(vertices.get(i));
		}
	}
	
	public boolean containVertex(vertex v) {
		if (vertices.contains(v))	return true;
		else	return false;
	}
	
	public List<vertex> vertices() {
		List<vertex> list = new ArrayList<>();
		for (int i = 0; i < vertices.size(); i++) {
			list.add(vertices.get(i));
		}
		return list;
	}
	
	public vertex sourceVertex() 
	{
		return this.vertices().get(0);
	}
	
	public vertex targetVertex() 
	{
		return this.vertices().get(1);
	}
	
	@Override
	public String toString() {
		String string = new String();
		string = string + label + ": ";
		string = string + sourceVertex().toString() + " ";
		string = string + targetVertex().toString() + " ";
		string = string + String.valueOf(weight) + "\n";
		return string;
 	}
	
	@Override
	public boolean equals(Object obj) {
		@SuppressWarnings("unchecked")
		edge<Vertex> edge = (edge<Vertex>) obj;
		if (this.label.equals(edge.getLabel())) {
			return true;
		}
		return false;
	}


	
}
