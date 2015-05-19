package nju.iip;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 第一次矩阵相乘
 * @author mrpod2g
 *
 */
public class MatrixMultiply {

  public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
	  private Path[] localFiles;
	  Map<String, HashMap<String, Integer>> matrix = new  HashMap<String,HashMap<String,Integer>>();

    /**
     * 将第一步获得的邻接矩阵作为共享数据读入内存，用2层HashMap存储<x,<y,1>>
     */
    public void setup(Context context) throws IOException {
    	String line;
    	Configuration conf = context.getConfiguration();
        localFiles = DistributedCache.getLocalCacheFiles(conf); 
        for (int i = 0; i < localFiles.length; i++) {
        	 BufferedReader br =
                     new BufferedReader(new FileReader(localFiles[i].toString()));
             while ((line = br.readLine()) != null) {
             	String[] str = line.split("\t");
             	String key = str[0];
             	String[] value = str[1].split(",");
             	if(matrix.containsKey(key)) {
             		for(String y:value) {
             			matrix.get(key).put(y, 1);
                 	}
             	}
             	else {
             		HashMap<String, Integer> map = new HashMap<String, Integer>();
             		for(String y:value) {
             			map.put(y,1);
             		}
             		matrix.put(key, map);
             	}
             }
             br.close();
        }
       
    }

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    	
    	String[] str = value.toString().split("\t");
    	String x = str[0];
    	String[] ks = str[1].split(",");
    	for(String k:ks) {
    		HashMap<String, Integer> map = matrix.get(k);
    		if(map!=null) {
    			Set<String> ys = map.keySet();
    			for(String y:ys) {
    				context.write(new Text(x+","+y),new Text("1"));
    			}
    		}
    	}
    	
  
    }
  }

  public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
 
    @SuppressWarnings("unused")
	public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	int sum = 0;
    	for(Text value:values) {
    		sum++;
    	}
    	context.write(key, new Text(sum+""));
    }
  }

  /**
   * main函数
   * <p>
   * Usage:
   * 
   * <p>
   * <code>MatrixMultiply  inputPathM  outputPath</code>
   * 
   * 
   * @throws Exception
   */

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err
          .println("Usage: MatrixMultiply <inputPathM>  <outputPath>");
      System.exit(2);
    } 

    Configuration conf = new Configuration();
    
    DistributedCache.addCacheFile(new URI(
            "hdfs://master01:54310/user/2015st18/output1/part-r-00000"), conf);// 设置缓存文件
    
    Job job = new Job(conf, "Second step—>MatrixMultiply");
    job.setJarByClass(MatrixMultiply.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    System.out.println("================MatrixMultiply finish!==================");
  }
}
