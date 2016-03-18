package myHadoop.Hadoop_KNN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class knn {
	public static class knnmap extends Mapper<Text,Text,Text,ListWritable<IntWritable>>{
		private int k ;
		private IntWritable ont = new IntWritable(-1);
		//存放训练集
		private ArrayList<Martrix> trainSet;
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			//得到k值
			k = context.getConfiguration().getInt("k", 3);
			trainSet = new ArrayList<Martrix>();
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fsd = null;
			BufferedReader br = null;
			String[] lable = null;
			//读取一个文件夹的文件
			FileStatus[] statu = fs.listStatus(
					new Path(context.getConfiguration().get("clusterPath")));
					for(int j=0;j<statu.length;j++){
						System.out.println(statu[j].getPath().toString());
					fsd = fs.open(statu[j].getPath());
					Martrix ma = new Martrix(fsd);
					lable = statu[0].getPath().getName().toString().split("_");
					ma.setLable(Integer.parseInt(lable[0]));
					trainSet.add(ma);
					}
			}
		
		protected void map(Text key,Text value,Context context)
				throws IOException, InterruptedException {
			Path path = new Path(value.toString());
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fsd = null;
			fsd = fs.open(path);
			//System.out.println(key+"+"+value);
			String line = key.toString()+value.toString();
			 Counter countPrint = context.getCounter("Map输出传递Value", line);
			 countPrint.increment(1l);
			Martrix test = new  Martrix(fsd);
			//存放距离
			ArrayList<Double> distance = new ArrayList<Double>(k);
			//存放距离最近的k个的标签
			ArrayList<IntWritable> trainLable = new ArrayList<IntWritable>(k);
			ListWritable<IntWritable> lables = new ListWritable<IntWritable>(IntWritable.class);
			
			
			for(int i=0;i<k;i++){				
				distance.add(Double.MAX_VALUE);
				trainLable.add(ont);
			}
			for(int i=0;i<trainSet.size();i++){
				//计算欧氏距离
				double dis = 0;
				try {
					dis = Distance.EuclideanDistance(trainSet.get(i).getMa(), test.getMa());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				int index = indexofMax(distance);
				//distance值存放最近的k个值  判断这个值是不是比距离集合distance中最大的那个小
				if(dis<distance.get(index)){
					distance.remove(index);
					trainLable.remove(index);
					distance.add(dis);
					trainLable.add(new IntWritable(trainSet.get(i).getLable()));
				}
			}
			lables.setList(trainLable);
			context.write(key, lables);
		}
		private int indexofMax(ArrayList<Double> distance) {
			int index =-1;
			Double min = Double.MIN_VALUE;
			//
			for(int i =0;i<distance.size();i++){
				if(distance.get(i)>min){
				min = distance.get(i);
				index=i;
				}
			}
			return index;
		}
	}
	public static class KNNReduce extends Reducer<Text,ListWritable<IntWritable>,Text,IntWritable>{
		protected void reduce(
				Text key,Iterable<ListWritable> values,Context context)
				throws IOException, InterruptedException {
			IntWritable predictedLable = new IntWritable();
			String line = key.toString();
			Counter countPrint = context.getCounter("Map输出传递Value", line);
			 countPrint.increment(1l);
			for(ListWritable<IntWritable> val: values){
				try {
					predictedLable = valueOfMostFrequent(val);
					break;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println(key);
			context.write(key, predictedLable);
		}

		private IntWritable valueOfMostFrequent(
				ListWritable<IntWritable> list) throws Exception {
			if(list.isEmpty())
				throw new Exception("list is empty!");
			else{
				//HashMap存放标签 和标签的数量
				HashMap<IntWritable,Integer> tmp = new HashMap<IntWritable,Integer>();
				for(int i = 0 ;i < list.size();i++){
					if(tmp.containsKey(list.get(i))){
						Integer frequence = tmp.get(list.get(i)) + 1;
						tmp.remove(list.get(i));
						tmp.put(list.get(i), frequence);
					}else{
						tmp.put(list.get(i), new Integer(1));
					}
				}
				//find the value with the maximum frequence.
				IntWritable value = new IntWritable();
				Integer frequence = new Integer(Integer.MIN_VALUE);
				Iterator<Entry<IntWritable, Integer>> iter = tmp.entrySet().iterator();
				while (iter.hasNext()) {
				    Map.Entry<IntWritable,Integer> entry = (Map.Entry<IntWritable,Integer>) iter.next();
				    if(entry.getValue() > frequence){
				    	frequence = entry.getValue();
				    	value = entry.getKey();
				    }
				}
				return value;
			}
		}
	}
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job kNNJob = new Job(conf);
		kNNJob.setJobName("kNNJob");
		kNNJob.setJarByClass(knn.class);
		
		
		kNNJob.getConfiguration().set("clusterPath", args[2]+"/");
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));
		
		kNNJob.setMapperClass(knnmap.class);
		kNNJob.setMapOutputKeyClass(Text.class);
		kNNJob.setMapOutputValueClass(ListWritable.class);

		kNNJob.setReducerClass(KNNReduce.class);
		kNNJob.setOutputKeyClass(Text.class);
		kNNJob.setOutputValueClass(IntWritable.class);
		kNNJob.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
		
		kNNJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
