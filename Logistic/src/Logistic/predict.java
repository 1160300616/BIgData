package Logistic;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class predict {

	public static double PTrue;
	public static double PFalse;
	
	public static int right = 0;
	public static int wrong = 0;
	public static int size ;
	public static double[] w = null;
	public static void Init() 
	{
		w = newlogistic.w;
		size = newlogistic.size;
		
		right = right +200000;
		wrong = wrong -200000;
		
	}
	
	
	public static class BayesMap extends Mapper<Object, Text, DoubleWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] tokens = line.split(",");
			double predict  = 1;
			double y =0.0;
			for (int i = 1; i <= size; i++) 
			{
				double val = Double.parseDouble(tokens[i]); 
				/*double val = Double.parseDouble(String.format(Possibility.format, val_all)); */
				y = y + newlogistic.w[i-1]*val;
			}
			y = y + w[size];
			if(y>=0)
			{
				predict = 1;
			}
			else
			{
				predict = 0;
			}
			
			context.write(new DoubleWritable(predict), value);
		}
	}
	
	public static class BayesReduce extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		@Override
		protected void reduce(DoubleWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double p = Double.parseDouble(arg0.toString()); 
			for (Text val : arg1) {
				arg2.write(arg0, val);
				String[] strs = val.toString().split(",");
				double label = Double.parseDouble(strs[0].toString());
				if (p == label)	
				{
					right++;
				}
				else	
				{
					wrong++;
				}
			}
		}
	}
	
}
