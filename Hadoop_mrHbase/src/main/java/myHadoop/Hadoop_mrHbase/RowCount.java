package myHadoop.Hadoop_mrHbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class RowCount {
	static final String name = "RowCount";
	
	static class RowCountMap extends TableMapper<ImmutableBytesWritable,Result>{
		public static enum Counts{Rows};
		@Override
		protected void map(ImmutableBytesWritable key,Result value,Context context)
				throws IOException, InterruptedException {
			context.getCounter(Counts.Rows).increment(1);
		}
		public static Job createSubmittableJob(Configuration cof,String[] args){
			String tableName = args[0];
			String startKey = null;
			String endKey = null;
			StringBuilder sb = new StringBuilder();
			final String rangeSwitch = "--range=";
			for(int i = 1;i<args.length;i++){
				if(args[i].startsWith(rangeSwitch)){
					
				}
			}
			return null;
			
		}
	}
}
