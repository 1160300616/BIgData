package kmeans;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.ArrayList;
 
 
/**
 * MapReduce开发WordCount应用程序
 * */
public class centers {
	
	static int k = 4;
    /**
     *
     * Map:读取输入的文件
     * */
	static ArrayList<ArrayList<Double>> center = new ArrayList<ArrayList<Double>>();
    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

    	
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                     
           context.write(new IntWritable(1), value);
        }
    }
    
   
    /**
     * Reduce
     * */
    public  static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
    	public double getDistance(ArrayList<Double> fileds)
    	   {
    		   double minDistance = 99999999;
    		   for(int i=0;i<k;i++){
    	           double currentDistance = 0;
    	           for(int j=1;j<fileds.size();j++)
    	           {
    	        	   
    	               double centerPoint = center.get(i).get(j);
    	               double filed = fileds.get(j);
    	               currentDistance += (centerPoint-filed)*(centerPoint-filed);

    	           }
    	           currentDistance = Math.sqrt(currentDistance);
    	           //循环找出距离该记录最接近的中心点的ID
    	           if(currentDistance<minDistance){
    	               minDistance = currentDistance;
    	             
    	           }
    	       }
    		   return minDistance;
    	   }
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	//用于累加的变量
        	if(center.size()==0)
        	{
        		for(Text val : values)
                {
            		context.write(null,val);
            		String temp = val.toString();
            		String strs[] = temp.split(",");
            		ArrayList<Double> t = new  ArrayList<Double>();
            		for(int i=0;i<strs.length;i++)
            		{
            			t.add(Double.parseDouble(strs[i]));
            		}
            		center.add(t);
            		context.write(null, val);
            		
            		break;
                }
        		return;
        	}
        	else 
        	{
        	Text out = null;
    		ArrayList<Double> r =new  ArrayList<Double>();
        	for(Text val : values)
            {
        		
        		String temp = val.toString();
        		String strs[] = temp.split(",");
        		ArrayList<Double> t = new  ArrayList<Double>();
        		double current = 9999999;
        		double now = 0;
        		for(int i=0;i<strs.length;i++)
        		{
        			t.add(Double.parseDouble(strs[i]));
        		}
        		now = getDistance(t);
        		if(now>current)
        		{
        			r = t;
        			current = now;
        			out  = val;
        		}
            }
        	center.add(r);
        	context.write(null, out);
        	}
        }
    }
 
    /**
     * 定义Driver类
     * */
 
    public static void main(String[] args) throws Exception {
    	//获取配置信息
    	/*BasicConfigurator.configure(); */
    	int count = 0;
    	while(count!=k) 
        {
    	count ++;
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS",  "hdfs://localhost:9000");
        String[] otherArgs = new GenericOptionsParser(configuration , args).getRemainingArgs();
        if(otherArgs.length != 2) {
        	System.err.println("Usage:wordcount <in> <out>");
        	System.exit(2);       	
        }
     
        //创建job并且命名
        Job job = Job.getInstance(configuration, "centers");
 
        //1.设置job运行的类
        job.setJarByClass(centers.class);
 
       // FileInputFormat.setInputPaths(job, new Path(args[0]));
 
        //2.设置Map和reduce类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        
        //3.设置输入输出的文件目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+count));
        
        //4.设置输出结果的key和value类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        
        //5.提交job等待运行结果，并在客户端显示运行信息
        job.waitForCompletion(true);
        //结束程序
        /*
        for(int i=0;i<center.size();i++)
        {
        	for(int j=0;j<center.get(i).size();j++)
        	{
        		System.out.print(center.get(i).get(j)+" ");
        	}
        	System.out.println();
        } */
        
        }
    	System.exit(0);
    }
}
