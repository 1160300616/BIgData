package Bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import Bayes.NaiveBayes.BayesMap;
import Bayes.NaiveBayes.BayesReduce;
import Bayes.Possibility.AttrPossibleReduce;
import Bayes.Possibility.AttrPosssiblityMap;



public class BayesDriver {

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Possibility.init();
		Configuration conf = new Configuration();
		Job jobAttrP = Job.getInstance(conf, "attribute possiblity");
		jobAttrP.setJarByClass(NaiveBayes.class);
		jobAttrP.setMapperClass(AttrPosssiblityMap.class);
		jobAttrP.setReducerClass(AttrPossibleReduce.class);
		jobAttrP.setMapOutputKeyClass(DoubleWritable.class);
		jobAttrP.setMapOutputValueClass(Text.class);
		jobAttrP.setOutputKeyClass(DoubleWritable.class);
		jobAttrP.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(jobAttrP, new Path("hdfs://localhost:9000/lab2/output/Data/train-r-00000"));
		Path outPath = new Path("hdfs://localhost:9000/lab2/output/Bayes/AttrPoss");
		FileSystem fs = outPath.getFileSystem(conf);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(jobAttrP, outPath);
		jobAttrP.waitForCompletion(true);
		
		
		NaiveBayes.Init();
		NaiveBayes.getMeanAndVar();
		Job jobPredict = Job.getInstance(conf, "predict");
		jobPredict.setJarByClass(NaiveBayes.class);
		jobPredict.setMapperClass(BayesMap.class);
		jobPredict.setReducerClass(BayesReduce.class);
		jobPredict.setMapOutputKeyClass(DoubleWritable.class);
		jobPredict.setMapOutputValueClass(Text.class);
		jobPredict.setOutputKeyClass(DoubleWritable.class);
		jobPredict.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobPredict, new Path("hdfs://localhost:9000/lab2/output/Data/test-r-00000"));
        Path outPath2 = new Path("hdfs://localhost:9000/lab2/output/Bayes/predict");
        FileSystem fs2 = outPath2.getFileSystem(conf);
        fs2.delete(outPath2, true);
        FileOutputFormat.setOutputPath(jobPredict, outPath2);
        jobPredict.waitForCompletion(true);
        
        System.err.println(NaiveBayes.right);
        System.err.println(NaiveBayes.wrong);
        System.err.println("正确率: " + NaiveBayes.right*1.0 / (NaiveBayes.right+NaiveBayes.wrong));
		
	}
	
}
