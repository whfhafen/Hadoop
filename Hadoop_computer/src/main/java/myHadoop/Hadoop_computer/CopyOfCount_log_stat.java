package myHadoop.Hadoop_computer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CopyOfCount_log_stat {
	public static class  Count_Map extends Mapper<Object,Text,Text,IntWritable>{
		private IntWritable one = new IntWritable(1);
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String time_local = null;// 记录访问时间与时区
			String status;// 记录请求状态；成功是200
			 
			String a = "200";
			String b = "404";
			String c = "500";
			String[] sr = line.split(" ");
			if(sr.length>9){
				status = sr[7];
				SimpleDateFormat date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
				Date df1;
				try {
					df1 = date.parse(sr[1].substring(1));
					SimpleDateFormat df2 = new SimpleDateFormat("HH");
					time_local = df2.format(df1);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				Text retu = new Text(time_local+" "+status);	
				context.write(retu, one);
			}
		}
	}
	public static class Count_Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable d : values){
				sum+=d.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	 public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = new Job(conf);
	        job.setJobName("name");
	        job.setJarByClass(CopyOfCount_log_stat.class);
	        job.setMapperClass(Count_Map.class);
	        job.setCombinerClass(Count_Reduce.class);
	        job.setReducerClass(Count_Reduce.class);  
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        try {
				System.exit(job.waitForCompletion(true)?0:1);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
}

