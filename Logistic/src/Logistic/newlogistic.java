package Logistic;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;
import java.util.Map;

public class newlogistic 
{

	public static Map<Integer,Map<Double, Double>> Tpossiblity = new HashMap<Integer,Map<Double, Double>>();
	public static Map<Integer,Map<Double, Double>> Fpossiblity = new HashMap<Integer,Map<Double, Double>>();
	public static Map<Integer,Map<Double, Integer>> Tcount = new HashMap<Integer,Map<Double, Integer>>();
	public static Map<Integer,Map<Double, Integer>> Fcount = new HashMap<Integer,Map<Double, Integer>>();
	public static int TrueCount = 0;
	public static int FalseCount = 0;
	public static int size = 8;
	public static double[] w = new double[size+1];
	public static double[] middle = new double[size+1];
	public static double rate= 0.1;
	public static int num = 0;
	public static int number = 0;
	/* public static String format = "%."+5+"f";  */
	
	public static void init() {
		for (int i = 0; i <= size; i++) 
		{
			num = 0;
			w[i] = 0.0;
			middle[i] = 0.0;
		}
	}
	public static void updata()
	{
		for (int i = 0; i <= size; i++) 
		{
			w[i] = w[i]-1.0/number*middle[i];

		}
	}
	public static class AttrPosssiblityMap extends Mapper<Object, Text, DoubleWritable, Text> {

		public double sigmod(double[] data)
		{
			double sum = 0;
			for(int i=0;i<=size;i++)
			{
				sum = sum + data[i+1]*w[i];
			}
			double temp = 0.0;
			temp = 1.0 + Math.exp(-sum);
			return 1.0/(1+temp);
		}
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			num++;
			double h = 0;
			double error = 0;
			double[] data = new double[size+2];
			String[] strs = value.toString().split(",");
			for(int i=0;i<=size;i++)
			{
				data[i] = Double.parseDouble(strs[i]);
			}
			data[size+1]=1.0;
			h = sigmod(data);
			error = h - data[0];
			String temp = "";
			for(int i=0;i<=size;i++)
			{
				/*
				middle[i] = middle[i] + rate*data[i]*error;
				*/
				temp = temp+String.valueOf(rate*data[i]*error)+",";
			} 

			context.write(new DoubleWritable(0), new Text(temp));
		}
	}
	
	public static class AttrPossibleReduce extends Reducer<DoubleWritable, Text, DoubleWritable, DoubleWritable> {
		

		public double sigmod(double[] data)
		{
			double sum = 0;
			for(int i=0;i<=size;i++)
			{
				sum = sum + data[i]*w[i];
			}
			double temp = 0.0;
			temp = 1.0 + Math.exp(-sum);
			return 1.0/(1+temp);
		}
		@Override
		protected void reduce(DoubleWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
			if(number==0)
			{
				number = num;
				System.out.println(number);
			}
			for(Text val : arg1)
			{
		
				String[] g = val.toString().split(",");
				for(int i=0;i<=size;i++)
				{
					middle[i] = middle[i] + rate*Double.parseDouble(g[i]);
					
				}
			}
			
		}
	}
	
}
