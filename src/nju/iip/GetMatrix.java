package nju.iip;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 从原始文件构造初始矩阵，存储结构：邻接表
 * @author mrpod2g
 *
 */
public class GetMatrix {
  public static class GetMatrixMapper extends
      Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      
      String[] line = value.toString().split(" ");
      context.write(new Text(line[0]),new Text(line[1]));
      context.write(new Text(line[1]),new Text(line[0]));
    }
  }

 
  public static class GetMatrixReducer extends
      Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException 
    { 
    	HashSet<String> vertex = new HashSet<String>();
    	StringBuffer out = new StringBuffer();
    	for(Text value:values) {
    		if(!vertex.contains(value.toString())) {//防止重复
    			vertex.add(value.toString());
    			out.append(",");
    			out.append(value.toString());
    		}
    		
    	}
    	out.deleteCharAt(0);
      context.write(key, new Text(out.toString()));
    } 
  }

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "First step—>GetMatrix");
      job.setJarByClass(GetMatrix.class);
      job.setMapperClass(GetMatrixMapper.class);
      job.setReducerClass(GetMatrixReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.waitForCompletion(true);
      System.out.println("==============GetMatrix finish!!=================");
  }
}