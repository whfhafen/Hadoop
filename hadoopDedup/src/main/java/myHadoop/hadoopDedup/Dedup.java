package myHadoop.hadoopDedup;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class Dedup {
	public static class Map extends Mapper<Object,Text,Text,IntWritable>{
		private static Text line ;
		private static IntWritable result = new IntWritable(1);
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			line = value;
				//直接输出value和
				context.write(line,result);
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			IntWritable result = new IntWritable(1);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJobName("debp");
		job.setJarByClass(Dedup.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	
	}
}
