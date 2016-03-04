package myHadoop.hadoopSort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Sort {
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		private static IntWritable data = new IntWritable ();
		public void map (Object key,Text value,Context context
				
				) throws IOException, InterruptedException{
			String line = value.toString();
			//将value转化为IntWritable类型
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}
		public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public static IntWritable linenum = new IntWritable(1);
		public void reduce (IntWritable key,Iterable<IntWritable> values,Context context){
			for(IntWritable val:values){
				try {
					context.write(linenum, key);
					linenum = new IntWritable(linenum.get()+1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		}
		public static class Partition extends Partitioner<IntWritable,IntWritable>{

			@Override
			public int getPartition(IntWritable key, IntWritable value,
					int numPartitions) {
				int Maxnumber = 65223;
				int bound = Maxnumber/numPartitions+1;
				int keynumber = key.get();
				for(int i=0;i<numPartitions;i++){
					if(keynumber<bound*(i+1)&&keynumber>=bound*i){
						return i;
					}
				}
				return -1;
			}
			
		}
		public static void main(String[] args) throws IOException{
			Configuration conf = new Configuration();
			Job job = new Job();
			job.setJarByClass(Sort.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setPartitionerClass(Partition.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path( args[1]));
		}
		
	
}
