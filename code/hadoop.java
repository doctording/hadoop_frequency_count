package com.hadoop_small;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class Data {

	/**
	 *  <数字ip , 数量1>
	 *
	 */
    static class TempMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
    	
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 打印样本: Before Mapper: 0, 某个数字
            //System.out.print("Before Mapper: " + key + "," + value);
            
            // 获取输入文件每一行的数据
            String line = value.toString();
            
        	context.write(new Text(line), new IntWritable(1)); 
        	// 为了显示效果而输出Mapper的输出键值对信息
            System.out.println("Mapper输出<" + line + "," + 1 + ">");
        }  
    }
    
    
    public static class  TempCombiner extends
		    Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(
		        Text key,
		        java.lang.Iterable<IntWritable> values,
		        org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context)
		        throws java.io.IOException, InterruptedException {
			
		    // 显示次数表示规约函数被调用了多少次，表示k2有多少个分组
		    System.out.println("Combiner输入分组<" + key.toString() + ",N(N>=1)>");
		    int count = 0;
		    for (IntWritable value : values) {
		        count += value.get();
		        // 显示次数表示输入的k2,v2的键值对数量
		       // System.out.println("Combiner输入键值对<" + key.toString() + ","
		       //         + value.get() + ">");
		    }
		    context.write(key, new IntWritable(count));
		    
		    // 显示次数表示输出的k2,v2的键值对数量
		    System.out.println("Combiner输出键值对<" + key.toString() + "," + count
		            + ">");
		};
    }
    
    /**
     *  <数字ip , 最终频数>
     *
     */
    static class TempReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

        	 System.out.println("Reducer输入分组<" + key.toString() + ",N(N>=1)>");
        	 
             int count = 0;
             for (IntWritable value : values) {
                 count += value.get();
             }
             context.write(key, new IntWritable(count));
             // 显示次数表示输入的k2,v2的键值对数量
             System.out.println("Reducer输入键值对<" + key.toString() + ","
                     + count + ">");
        }
    }

    public static void main(String[] args) throws Exception {

        // 输入路径
        String dst = "hdfs://localhost:9000/user/input/nums_small.txt";

        // 输出路径，必须是不存在的，空文件也不行。
        String dstOut = "hdfs://localhost:9000/user/output/numsout_small";

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Job job = new Job(hadoopConfig);

        // job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));

        // 指定自定义的Mapper和Reducer作为两个阶段的任务处理类, 加了个Combiner阶段
        job.setMapperClass(TempMapper.class);
        job.setCombinerClass(TempCombiner.class);
        job.setReducerClass(TempReducer.class);

        // 设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 执行job，直到完成
        job.waitForCompletion(true);
        System.out.println("Finished");
    }
}
