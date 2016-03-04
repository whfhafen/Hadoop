package myHadoop.hadoop3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context){
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterable =values.iterator();
			while(iterable.hasNext()){
				sum+=iterable.next().get();
				count++;
			}
			int average = (int)sum/count;
			try {
				context.write(key, new IntWritable(average));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}
