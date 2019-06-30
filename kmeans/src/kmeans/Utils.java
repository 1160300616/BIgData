package kmeans;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Utils {

    //读取中心文件的数据
    public static ArrayList<ArrayList<Double>> getCentersFromHDFS(String centersPath,boolean isDirectory) throws IOException{

        ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();

        Path path = new Path(centersPath);

        Configuration conf = new Configuration();

        FileSystem fileSystem = path.getFileSystem(conf);

        if(isDirectory){    
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (int i = 0; i < listFile.length; i++) {
                result.addAll(getCentersFromHDFS(listFile[i].getPath().toString(),false));
            }
            return result;
        }

        FSDataInputStream fsis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsis, conf);

        Text line = new Text();

        while(lineReader.readLine(line) > 0){
            //ArrayList<Double> tempList = textToArray(line);
            ArrayList<Double> tempList = new ArrayList<Double>();
            String[] fields = line.toString().replaceAll("\t", "").split(",");
            for(int i=0;i<fields.length;i++){
                tempList.add(Double.parseDouble(fields[i]));
            }
            result.add(tempList);
        }
        lineReader.close();
        return result;
    }

    //删掉文件
    public static void deletePath(String pathStr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path ,true);
    }

    public static ArrayList<Double> textToArray(Text text){
        ArrayList<Double> list = new ArrayList<Double>();
        String[] fileds = text.toString().replaceAll("\t", "").split("/,");
        for(int i=0;i<fileds.length;i++){
            list.add(Double.parseDouble(fileds[i]));
        }
        return list;
    }

    public static boolean compareCenters(String centerPath,String newPath) throws IOException{
        System.out.println("比较两个中心点是否相等");
        List<ArrayList<Double>> oldCenters = Utils.getCentersFromHDFS(centerPath,false);
        List<ArrayList<Double>> newCenters = Utils.getCentersFromHDFS(newPath,true);
        int a = oldCenters.hashCode();
        int b = newCenters.hashCode();
        if(a==b)
        {
            //删掉新的中心文件以便最后依次归类输出
            Utils.deletePath(newPath);
            return true;
        }
        else
        {
            //先清空中心文件，将新的中心文件复制到中心文件中，再删掉中心文件

            Utils.deletePath(centerPath);
            Configuration conf = new Configuration();
            Path outPath = new Path(centerPath);
            FileSystem fileSystem = outPath.getFileSystem(conf);            
            FSDataOutputStream out = fileSystem.create(outPath);
            //out.
            //将newCenter的内容写到文件里面
            Path inPath = new Path(newPath);

            FileStatus[] listFiles = fileSystem.listStatus(inPath);
            for (int i = 0; i < listFiles.length; i++) {                

                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());

                int byteRead = 0;
                byte[] buffer = new byte[256];
                while ((byteRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, byteRead);
                }
                in.close();
            }
                out.close();

            //删掉新的中心文件以便第二次任务运行输出
            Utils.deletePath(newPath);
        }

        return false;
    }
}