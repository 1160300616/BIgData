package Bayes;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//MapReduce算法
public class split {
	public static void test()
	{
		NaiveBayes.right = 200000;
		NaiveBayes.wrong = -200000;
	}
  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{



    public IntWritable one= new IntWritable(1);
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	
		context.write(one,value);
    }
  }
  public static class IntSumReducer	extends Reducer<Text,Text,NullWritable,Text> {
	  Text out = new Text();
	  private MultipleOutputs<NullWritable, Text> mos;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}		
public void reduce(Text key, Iterable<Text> values, Context context  ) throws IOException, InterruptedException {

	int count = 0;
	for (Text val:values) {
		int k = count % 5;
		count++;
		if (k >= 1 && k <= 4) {
			mos.write("train", null, val);
		} 
		else if (k == 0)
		{
			mos.write("test", null, val);
		}
	}
	

	
}
}
 
  public static void main(String[] args) throws Exception {
	BasicConfigurator.configure();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "split");
    job.setJarByClass(split.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    MultipleOutputs.addNamedOutput(job, "train", TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, NullWritable.class, Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}