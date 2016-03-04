package myHadoop.Hadoop_computer;

import java.io.IOException;
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

public class Count_log_stat {
	public static class  Count_Map extends Mapper<Object,Text,Text,IntWritable>{
		private IntWritable one = new IntWritable(1);
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			KPI kpi = KPI.split_sr(line);
			String a = "200";
			String b = "404";
			String c = "500";
			if(kpi.isValid()){
			if(kpi.getRequest_stat().equals(a)||kpi.getRequest_stat().equals(b)||kpi.getRequest_stat().equals(c)){
				Text retu = new Text(kpi.getRequest_time()+" "+kpi.getRequest_stat());	
				context.write(retu, one);
				}
			
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
	        String input = "hdfs://10.10.65.14:9000/whf/inputlog";
	        String output = "hdfs://10.10.65.14:9000/whf/outputlog1";
	        Configuration conf = new Configuration();
	        Job job = new Job();
	        job.setJobName("name");
	        conf.addResource("classpath:/hadoop/core-site.xml");
	        conf.addResource("classpath:/hadoop/hdfs-site.xml");
	        conf.addResource("classpath:/hadoop/mapred-site.xml");
	        conf.addResource("classpath:/hadoop/yarn-site.xml");
	        job.setJarByClass(Count_log_stat.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(IntWritable.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        job.setMapperClass(Count_Map.class);
	        job.setReducerClass(Count_Reduce.class);
	        FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
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
