package pregel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Svertex {
	private int value;
	private int id;
	private int state;  // 0为inactive 1为active
	private int Superstep;
	public ArrayList<Integer> msg = new ArrayList<Integer>();
	public Svertex(int id)
	{
		this.id = id;
		this.state = 1;
		this.Superstep = 0;
		this.value = 999999;
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
	} 
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
	public Map<Svertex, List<Double>>  GetOutEdgeIterator()
	{
		return null;
	}
	void addMessage(int msgs)
	{
		this.msg.add(msgs);
	}
	
	void SendMessageTo(Set<vertex> targets,int Message )
	{
		for(vertex v:targets)
		{
			v.addMessage(Message);
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
		if(((Svertex)o).getId()==this.id)
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