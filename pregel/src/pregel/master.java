package pregel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import pregel.worker;
public class master {
	public static ArrayList<worker> workers = new   ArrayList<worker>();
	/*public static Set<vertex> vertexs = new HashSet<vertex>();  */
	public static Map<vertex,vertex> vertexs = new HashMap<vertex,vertex>(); 
	public static Map<vertex,Set<vertex>> targets = new HashMap<vertex,Set<vertex>>();
	public static int workersize =4;
	public static Map<worker,Integer> workerVertex = new HashMap<worker,Integer>();
	public static Map<worker,Integer> workerEdge = new HashMap<worker,Integer>();
	public static ArrayList<Long> time= new ArrayList<Long>();
	public static void write(worker w)
	{
		String fileName = "/home/yuyu/Downloads/put.txt";
		try
		{
			BufferedWriter br = new BufferedWriter(new FileWriter(fileName));
			for(vertex v:w.vertices)
			{
				for(vertex target:targets.get(v))
				{
					br.write(v.getId()+" "+target.getId());
					br.flush();
				}
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void read(worker w)
	{
		String fileName = "/home/yuyu/Downloads/put.txt";
		int count = 0;
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			int in;
			int out;
			String line = "";
			while((line=br.readLine())!=null)
			{
				String strs[] = line.split("\\s+");
				in = Integer.parseInt(strs[0]);
				out = Integer.parseInt(strs[1]);
				vertex v1 = new vertex(in);
				vertex v2 = new vertex(out);
				w.vertices.add(v1);
				edge e = new edge("e"+count,1);
				ArrayList<vertex> vertextemp = new ArrayList<vertex>();
				vertextemp.add(v1);
				vertextemp.add(v2);
				e.addVertices(vertextemp);
				w.addEdge(e);
				count++;
			}
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void clearMap()
	{
		for(int i=0;i<workers.size();i++)
		{
			Map<vertex,ArrayList<Integer>> m = communication.sendMsg.get(workers.get(i));
			for(vertex v:m.keySet())
			{
				m.get(v).clear();
			}
		}
	}
	public static void main(String args[])
	{
		
		String fileName = "/home/yuyu/Downloads/web-Google.txt"; 
		/*String fileName = "/home/yuyu/Downloads/test.txt"; */
		try {
			BufferedReader br=new BufferedReader(new FileReader(fileName));
			String line ="";
			int in;
			int out;

			for(int i=0;i<workersize;i++)
			{
				worker work = new worker();
				workers.add(work);
				HashMap<vertex,ArrayList<Integer>> receive = new HashMap<vertex,ArrayList<Integer>>();
				HashMap<vertex,ArrayList<Integer>> send = new HashMap<vertex,ArrayList<Integer>>();
				communication.sendMsg.put(work,receive);
				communication.receiveMsg.put(work,send);
			}
			int count=0;
			while((line=br.readLine())!=null)
			{
				count++;
				String strs[] = line.split("\\s+");
				in = Integer.parseInt(strs[0]);
				out = Integer.parseInt(strs[1]);
				vertex v1 = new vertex(in);
				vertex v2 = new vertex(out);
				/*
				Active.add(v1);
				Active.add(v2);
				*/
				if(in==0)
				{
					v1.setValue(0);
				}
				if(out==0)
				{
					v2.setValue(0);
				}
				if(vertexs.containsKey(v1))
				{
					v1 = vertexs.get(v1);
				}
				else
				{
					vertexs.put(v1,v1);
				}
				if(vertexs.containsKey(v2))
				{
					v1 = vertexs.get(v2);
				}
				else
				{
					vertexs.put(v2,v2);
				}
				boolean f1 = true;
				boolean f2 = true;
				for(int i=0;i<workersize;i++)
				{
					if(workers.get(i).vertices.contains(v1))
					{
						f1 = false;
						break;
					}
				}
				if(f1)
				{
					workers.get(in%workersize).addVertex(v1);
				}
				for(int i=0;i<workersize;i++)
				{
					if(workers.get(i).vertices.contains(v2))
					{
						f2 = false;
						break;
					}
				}
				if(f2)
				{
					workers.get(out%workersize).addVertex(v2);
				}
				edge e = new edge("e"+count,1);
				ArrayList<vertex> vertextemp = new ArrayList<vertex>();
				vertextemp.add(v1);
				vertextemp.add(v2);
				if(targets.containsKey(v1))
				{
					Set<vertex> ver = targets.get(v1);
					ver.add(v2);
					targets.put(v1, ver);
				}
				else
				{
					Set<vertex> ver = new HashSet<vertex>();
					ver.add(v2);
					targets.put(v1, ver);
				}
				e.addVertices(vertextemp);
				workers.get(in%workersize).addEdge(e);
			}
			boolean f = true;
			int superstep=0;
			System.out.println("It's a graph with "+vertexs.size()+" vertexs"); 
			for(int i=0;i<workersize;i++)
			{
				workerVertex.put(workers.get(i),workers.get(i).vertices.size());
				workerEdge.put(workers.get(i), workers.get(i).edges.size());
				for(vertex v:workers.get(i).vertices)
				{
					ArrayList<Integer> a = new ArrayList<Integer>();
					communication.sendMsg.get(workers.get(i)).put(v,a);
				}
			}
			while(f)
			{
				long start = System.currentTimeMillis();
				f=false;
				System.out.println("Superstep:"+superstep);
				superstep++;
				communication.receiveMsg = communication.sendMsg;
				clearMap();
				for(vertex v:vertexs.keySet())
				{
					if(v.getState()==1)
					{
						f = true;
						if(targets.containsKey(v))
						{
							/*
							v.SendMessageTo(targets.get(v), v.getValue()+1);
							*/
							for(vertex target :targets.get(v))
							{
								for(int n=0;n<workersize;n++)
								{
									if(workers.get(n).vertices.contains(target))
									{
										communication.sendMsg.get(workers.get(n)).get(target).add(v.getValue()+1);
										
									}
								}
								target.addMessage(v.getValue()+1);
							}
						}
						else
						{
							v.VoteToHalt();
						}
					}
				}
				for(vertex v:vertexs.keySet())
				{
					v.Compute(0);
				}
				long end = System.currentTimeMillis();
				System.out.println("次轮的执行时间为："+(end-start));
				time.add(end-start);
			}
			
			ArrayList intemp = new ArrayList();
			intemp.add(0);
			vertex vt = new vertex(0);
			while(targets.containsKey(vt))
			{
				for(vertex v:targets.get(vt))
				{
					if(!intemp.contains(v.getId()))
					{
						System.out.print("0"+"->"+v.getId()+":");
						System.out.println(v.getValue());
						intemp.add(v.getId());
						vt = v;
						break;
					}
				}
			}
			br.close(); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
