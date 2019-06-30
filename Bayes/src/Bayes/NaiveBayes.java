package Bayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class NaiveBayes {
	
	public static double PTrue;
	public static double PFalse;
	
	public static int right = 0;
	public static int wrong = 0;
	
	public static int size = 8;
	public static Map<Integer, Double> Tmeans = new HashMap<Integer, Double>();
	public static Map<Integer, Double> Tvars = new HashMap<Integer, Double>();
	
	public static Map<Integer, Double> Fmeans = new HashMap<Integer, Double>();
	public static Map<Integer, Double> Fvars = new HashMap<Integer, Double>();
	
	public static void Init() 
	{
		PTrue = Possibility.TrueCount*1.0 / (Possibility.TrueCount + Possibility.FalseCount);
		PFalse = Possibility.FalseCount*1.0 / (Possibility.TrueCount + Possibility.FalseCount);

	}
	public static void getMeanAndVar() 
	{
		for (int i = 1; i <= size; i++) {
			Map<Double, Double> possMap = Possibility.Fpossiblity.get(i);
			Set<Double> keys = possMap.keySet();
			double sum = 0;
			double var = 0;
			int n = keys.size();
			for (Double p : keys) {
				sum += p*possMap.get(p);
			}
			double mean = sum ;
			Fmeans.put(i, mean);
			for (Double p : keys) {
				var = var + (p-mean) * (p-mean)*possMap.get(p);
			}
			Fvars.put(i, var);
		}
		for (int i = 1; i <= size; i++) {
			Map<Double, Double> possMap = Possibility.Tpossiblity.get(i);
			Set<Double> keys = possMap.keySet();
			double sum = 0;
			double var = 0;
			int n = keys.size();
			for (Double p : keys) {
				sum += p*possMap.get(p);
			}
			double mean = sum ;
			Tmeans.put(i, mean);
			for (Double p : keys) {
				var = var + (p-mean) * (p-mean)*possMap.get(p);
			}

			Tvars.put(i, var);
		}
	}
	
	public static Double possibility(int classify, int index, double x) {
		if (classify == 1) 
		{
			double v1 = 1.0 / Math.sqrt(2*Math.PI*Tvars.get(index));
			double temp = (x-Fmeans.get(index)) * (x-Fmeans.get(index));
			double v2 = Math.exp(-temp / (2*Tvars.get(index)));
			return v1*v2;
		} 
		else 
		{
			double v1 = 1.0 / Math.sqrt(2*Math.PI*Fvars.get(index));
			double temp = (x-Fmeans.get(index)) * (x-Fmeans.get(index));
			double v2 = Math.exp(-temp / (2*Fvars.get(index)));
			return v1*v2;
		}
	}
	
	public static class BayesMap extends Mapper<Object, Text, DoubleWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] tokens = line.split(",");
			double result = Double.parseDouble(tokens[0]);
			double predict_true = 1;
			for (int i = 1; i <= size; i++) 
			{
				double val = Double.parseDouble(tokens[i]); 
				/*double val = Double.parseDouble(String.format(Possibility.format, val_all)); */
				predict_true = predict_true * NaiveBayes.possibility((int)result,i,val);

			}
			predict_true = PTrue*predict_true;
			
			double predict_false = 1;
			for (int i = 1; i <= size; i++) 
			{
				double val = Double.parseDouble(tokens[i]);
				/*
				double val = Double.parseDouble(String.format(Possibility.format, val_all)); */
				predict_true = predict_false * NaiveBayes.possibility((int)result,i,val);

			}
			predict_false = predict_false*PFalse;
			context.write(new DoubleWritable(result), new Text(predict_true + "," + predict_false));
		}
	}
	
	public static class BayesReduce extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		@Override
		protected void reduce(DoubleWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text val : arg1) {
				arg2.write(arg0, val);
				String predict = val.toString();
				String[] tokens = predict.split(",");
				Double pt = Double.parseDouble(tokens[0]);
				Double pf = Double.parseDouble(tokens[1]);
				int p;
				if (pt/pf > 1 ) 
				{
					p = 1;
				}
				else 
				{
					p = 0;
				}
				if (p == arg0.get())	
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
