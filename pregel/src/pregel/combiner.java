package pregel;

import java.util.ArrayList;

public class combiner {
	
	public static int getShortest(ArrayList<Integer> msgs)
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
	public static double getNewRank(ArrayList<Integer> ranks)
	{
		double sum = 0;
		for(int i=0;i<ranks.size();i++)
		{
			sum = sum + ranks.get(i);
		}
		sum = sum + (double)(1.0-pagerank.d)/pagerank.size;
		return sum;
	}
}
