package pregel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class vertex {
	private int value;
	private int id;
	private int state;  // 0为inactive 1为active
	private int Superstep;
	private double rank=0;
	public ArrayList<Integer> msg = new ArrayList<Integer>();
	public ArrayList<Double> ranks = new ArrayList<Double>();
	public vertex(int id)
	{
		this.id = id;
		this.state = 1;
		this.Superstep = 0;
		this.value = 999999;
		this.rank = 0.5;
	}
	
	public double getRank()
	{
		return this.rank;
	}
	public void setRank(double rank)
	{
		this.rank =rank;
	}
	public int getShortest(ArrayList<Integer> msgs)
	{
		int min =99999999;
		for(int i=0;i<msgs.size();i++)
		{
			if(msgs.get(i)<min)
			{
				min = msgs.get(i);
			}
		}
		return min;
	}
	public double getNewRank()
	{
		double sum = 0;
		for(int i=0;i<ranks.size();i++)
		{
			sum = sum + ranks.get(i);
		}
		sum = sum + (double)(1.0-pagerank.d)/pagerank.size;
		return sum;
	}
	public void Compute(int msgs)
	{
		double sum = 0;
		if(ranks.size()==0)
		{
			VoteToHalt();
			return;
		}
		sum = getNewRank();
		if(this.rank-sum<=0.00001&&this.rank-sum>=-0.00001)
		{
			VoteToHalt();
		}
		else
		{
			this.rank = sum;
			this.state =1;
		}
	}
	/*
	public  void Compute(int  msgs)
	{
		if(msg.size()==0)
		{
			VoteToHalt();
			return;
		}
		msgs = getShortest(msg);
		if(this.value>msgs)
		{
			this.value = msgs;
			this.state = 1;
			msg.clear();
			return;
		}
		else
		{
			msg.clear();
			VoteToHalt();
		}
	} */
	public int getState()
	{
		return this.state;
	}

	public int getId()
	{
		return this.id;
	}
	public int getValue()
	{
		return this.value;
	}
	public void setValue(int value)
	{
		this.value = value;
	}
	public Map<vertex, List<Double>>  GetOutEdgeIterator()
	{
		return null;
	}
	void addMessage(int msgs)
	{
		this.msg.add(msgs);
	}
	/*
	void SendMessageTo(Set<vertex> targets,int Message )
	{
		for(vertex v:targets)
		{
			v.addMessage(Message);
		}
	} */
	void SendMessageTo(Set<vertex> targets,double Message )
	{
		for(vertex v:targets)
		{
			v.ranks.add(Message);
		}
	} 
	void VoteToHalt()
	{
		this.state = 0;
	}
	void Wake()
	{
		this.state = 1;
	}
	@Override
	public boolean equals(Object o)
	{
		if(((vertex)o).getId()==this.id)
		{
			return true;
		}
		else return false;
	}
	
	@Override
	public int hashCode()
	{
		return this.id;
	}
}