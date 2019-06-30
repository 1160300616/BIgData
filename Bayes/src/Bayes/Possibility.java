package Bayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Possibility {

	public static Map<Integer,Map<Double, Double>> Tpossiblity = new HashMap<Integer,Map<Double, Double>>();
	public static Map<Integer,Map<Double, Double>> Fpossiblity = new HashMap<Integer,Map<Double, Double>>();
	public static Map<Integer,Map<Double, Integer>> Tcount = new HashMap<Integer,Map<Double, Integer>>();
	public static Map<Integer,Map<Double, Integer>> Fcount = new HashMap<Integer,Map<Double, Integer>>();

	public static int TrueCount = 0;
	public static int FalseCount = 0;
	
	public static int size = 8;
	
	/* public static String format = "%."+5+"f";  */
	
	public static void init() {
		for (int i = 1; i <= size; i++) {
			Tcount.put(i, new HashMap<Double, Integer>());
			Tpossiblity.put(i, new HashMap<Double, Double>());
			Fcount.put(i, new HashMap<Double, Integer>());
			Fpossiblity.put(i, new HashMap<Double, Double>());
		}
	}
	
	public static class AttrPosssiblityMap extends Mapper<Object, Text, DoubleWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] tokens = line.split(",");
			double result = Double.parseDouble(tokens[0]);
			if (result == 0)
			{
				FalseCount++;
			}
			else 
			{
				TrueCount++;
			}
			/*result = Double.parseDouble(String.format(format, result)); */
			context.write(new DoubleWritable(result), value);
		}
	}
	
	public static class AttrPossibleReduce extends Reducer<DoubleWritable, Text, DoubleWritable, DoubleWritable> {
		

		
		@Override
		protected void reduce(DoubleWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (arg0.get() == 0)    //类别为0的情况下
			{
				for (Text val: arg1) 
				{
					String line = val.toString();
					String[] tokens = line.split(",");
					for (int i = 1; i <= size; i++) 
					{
						double attrVal = Double.parseDouble(tokens[i]);
//						double attrVal = attrVal_all;
						/*double attrVal = Double.parseDouble(String.format(format, attrVal_all));  */
						Map<Double, Integer> attrMap = Fcount.get(i);
						if (attrMap.containsKey(attrVal)) 
						{
							int count = attrMap.get(attrVal);
							attrMap.put(attrVal, count+1);
						} 
						else 
						{
							attrMap.put(attrVal, 1);
						}
					}
					
				}
				for (int i = 1; i <= size; i++) 
				{

			        for (double att_val : Fcount.get(i).keySet())
			        {
			        	double possibility = Fcount.get(i).get(att_val)*1.0/FalseCount;
			        	Fpossiblity.get(i).put(att_val, possibility);
			        }
				}
			} 
			else 
			{
				for (Text val: arg1) 
				{
					String line = val.toString();
					String[] tokens = line.split(",");
					for (int i = 1; i <= size; i++) {
						double attrVal = Double.parseDouble(tokens[i]);
//						double attrVal = attrVal_all;
						/* double attrVal = Double.parseDouble(String.format(format, attrVal_all));  */
						Map<Double, Integer> attrMap = Tcount.get(i);
						if (attrMap.containsKey(attrVal)) 
						{
							int count = attrMap.get(attrVal);
							attrMap.put(attrVal, count+1);
						}
						else 
						{
							attrMap.put(attrVal, 1);
						}
					}
					
				}
				for (int i = 1; i <= size; i++) {
					
			        for (double att_val : Fcount.keySet()) 
			        {
			        	double possibility = Tcount.get(i).get(att_val)*1.0/TrueCount;
			        	Tpossiblity.get(i).put(att_val, possibility);
			        }
				}
			}
		}
	}
	
}
