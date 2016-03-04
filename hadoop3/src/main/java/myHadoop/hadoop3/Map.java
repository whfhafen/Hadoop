package myHadoop.hadoop3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

public class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context){
			String line = value.toString();
			System.out.println(line);
			StringTokenizer token = new StringTokenizer(line,"\n");
			while(token.hasMoreTokens()){
				StringTokenizer tokenizer = new StringTokenizer(token.nextToken());
				String strName = tokenizer.nextToken();
				String strScore = tokenizer.nextToken();
				Text name = null ;
				name.setData(strName);
				int scoreInt = Integer.parseInt(strScore);
				try {
					context.write(name, new IntWritable(scoreInt));
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
