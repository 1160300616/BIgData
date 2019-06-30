package kmeans;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class MapReduce extends Configured implements Tool
{

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{


  
         public ArrayList<ArrayList<Double>> centers = null;
         public int k = 4;
         public void configure(JobConf job){
             String center = job.get("map.center.file");
             try {
                centers = Utils.getCentersFromHDFS(center,false);
                k = centers.size();
                System.out.println("centers point is: "+centers.toString());

            } catch (IOException e) {
                System.err.println("cannot find the map center file!");
                e.printStackTrace();
            }

         }

        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<IntWritable, Text> output, Reporter report)
                throws IOException {


            ArrayList<Double> fileds = new ArrayList<Double>();
            String[] temp = value.toString().replaceAll("\t", "").split(",");
            for(int i = 0; i<temp.length;i++){
                fileds.add(Double.parseDouble(temp[i]));
            }

            int sizeOfFileds = fileds.size();

            double minDistance = 99999999;
            int centerIndex = 0;

            
            for(int i=0;i<k;i++){
                double currentDistance = 0;
                for(int j=1;j<sizeOfFileds;j++)
                {

                    double centerPoint = centers.get(i).get(j);
                    double filed = fileds.get(j);
                    currentDistance += (centerPoint-filed)*(centerPoint-filed);

                }
                currentDistance = Math.sqrt(currentDistance);
                
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
           
            output.collect(new IntWritable(centerIndex+1), value);

        }

    }


     public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

        @Override
        public void reduce(IntWritable key, Iterator<Text> value,
                OutputCollector<Text, Text> output, Reporter report)
                throws IOException {
        		ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();
        		DecimalFormat df0 = new DecimalFormat("###.000000");

      
            System.out.println(key+":  "+value.toString());
            while(value.hasNext()){

                ArrayList<Double> tempList = new ArrayList<Double>();
                String[] temp0 = value.next().toString().replaceAll("\t", "").split(",");
                for(int i = 0; i< temp0.length; i++){
                    tempList.add(Double.parseDouble(df0.format(Double.parseDouble(temp0[i]))));
                }
                filedsList.add(tempList);
            }

            //计算新的中心
            int filedSize = filedsList.get(0).size();
            double[] avg = new double[filedSize];
            for(int i=0;i<filedSize;i++){
               
                double sum = 0;
                int size = filedsList.size();
                for(int j=0;j<size;j++){
                    sum += filedsList.get(j).get(i);
                }
                avg[i] = sum / size;
                avg[i] = Double.parseDouble(df0.format(avg[i]));
            }
            output.collect(new Text("") , new Text(Arrays.toString(avg).replace("[", "").replace("]", "").replaceAll("\t", "")));

        }


     }



    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), MapReduce.class);
         conf.setJobName("kmeans");

         conf.setMapperClass(Map.class);
         conf.setMapOutputKeyClass(IntWritable.class);
         conf.setMapOutputValueClass(Text.class);

         if(!"false".equals(args[3])||"true".equals(args[3]))
         {
             conf.setReducerClass(Reduce.class);
             conf.setOutputKeyClass(Text.class);
             conf.setOutputValueClass(Text.class);
         }

         FileInputFormat.setInputPaths(conf, new Path(args[0]));
         FileOutputFormat.setOutputPath(conf, new Path(args[1]));
         conf.set("map.center.file", args[2]);
         JobClient.runJob(conf);
         return 0;
    }
    public static void main(String[] args)throws Exception{
    	/*BasicConfigurator.configure(); */
        int count = 0;
        int res = 0;
        while(count<10)
        {
            res = ToolRunner.run(new Configuration(), new MapReduce(), args);
            count++;
            System.out.println(" 第 " + count + " 次迭代计算 ");
            if(Utils.compareCenters(args[2],args[1] )){
            	
                String lastarg[] = new String[args.length];
                for(int i=0; i < args.length-1; i++)
                {
                    lastarg[i] = args[i];
                    
                }
                lastarg[args.length-1] = "false";
                res = ToolRunner.run(new Configuration(), new MapReduce(), lastarg);
                break;
                }

         }
        System.exit(res);
    }


}
