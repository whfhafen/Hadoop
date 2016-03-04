package myHadoop.hadoopHot;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Runjob {
	public static class temMap extends Mapper<LongWritable, Text, keypair, Text>{
		public static SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] record = line.split("\t");
			Date date;
			try {
				date = sdf.parse(record[0]);
				Calendar c =Calendar.getInstance();
				c.setTime(date);
				int year = c.get(1);
				String hot = record[1].substring(0, record[1].indexOf("℃"));
				keypair kk = new  keypair();
				kk.setHot(Integer.parseInt(hot));
				kk.setYear(year);
				context.write(kk, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			
		}
	}
	public static class temReduce extends Reducer<keypair, Text, keypair, Text>{
		protected void reduce(keypair key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text v:values){
				context.write(key, v);
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length!=2){
			System.err.println("Usage");
			System.exit(0);
		}
		Job job = new Job(conf,"sorthot");
		job.setJarByClass(Runjob.class);
		job.setMapperClass(temMap.class);
		job.setReducerClass(temReduce.class);
		job.setSortComparatorClass(sortHot.class);
		job.setPartitionerClass(yearPartation.class);
		job.setGroupingComparatorClass(GroupHot.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(keypair.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
