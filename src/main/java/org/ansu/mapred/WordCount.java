package org.ansu.mapred;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split(" ");
			for(String word:data){
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text word, Iterable<IntWritable> num,Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val:num){
				sum += val.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.cross-platform", "true");//设置提交到远程主机
		Path inPath = new Path("/test.txt");
		Path outPath = new Path("/output");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("wc");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true)?0:-1);
	}

}
