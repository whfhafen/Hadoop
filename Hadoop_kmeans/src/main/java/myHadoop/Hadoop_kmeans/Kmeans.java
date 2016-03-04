package myHadoop.Hadoop_kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Kmeans {
	public class  Kmap extends Mapper<LongWritable,Text,IntWritable,Cluster>{
		private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();
		//在setup中读入目前的簇信息
		public void setup(Context context) throws IOException, InterruptedException{
			//这一句完全是废话  父类的setup函数只是个空函数
			super.setup(context);
			
			//返回一个默认的文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FileStatus[] fileList = fs.listStatus(
					new Path(context.getConfiguration().get("clusterPath")));
			BufferedReader in = null;
			FSDataInputStream fsi = null;
			String line = null;
			for(int i = 0;i<fileList.length;i++){
				if(!fileList[i].isDirectory()){
					fsi = fs.open(fileList[i].getPath());
					in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
					while((line=in.readLine())!=null){
						//设置clusterID numofPoints ceneter
						Cluster cluster = new Cluster(line);
						cluster.setNumofPoints(0);
						//将中心簇信息添加到全局集合里便于迭代的时候调用
						kClusters.add(cluster);
				}
			}
		}
			in.close();
			fsi.close();
		}
		//读取一行然后找离该点最近的簇发射
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			//普通节点信息存放在instance中
			Instance instance = new Instance(value.toString());
			int id ;
			try {
				//离此节点最近的中心簇节点的ID
				id = getNearest(instance);
				if(id == -1)
					throw new InterruptedException("id == -1");
				else{
					Cluster cluster = new Cluster(id, instance);
					cluster.setNumofPoints(1);
					System.out.println("cluster that i emit is:" + cluster.toString());
					//返回这个instance所在的中心节点id和簇信息
					context.write(new IntWritable(id), cluster);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		private int getNearest(Instance instance) throws Exception {
			int id = -1;
			double distance = Double.MAX_VALUE;
			Distance<Double> distanceMeasure = new EuclideanDistance<Double>();
			double newDis = 0.0;
			for(Cluster cluster : kClusters){	
				//计算instance和cluster中各中心簇的距离选取其中最近的距离
				newDis = distanceMeasure.getDistance(cluster.getCneter().getvalue()
						, instance.getvalue());
				//newDis表示距离此节点最近的那个中心节点
				if(newDis < distance){
					id = cluster.getClusterID();
					distance = newDis;
				}
			}
			return id;
		}
		public Cluster getClusterByID(int id){
			for(Cluster cluster : kClusters){
				if(cluster.getClusterID() == id)
					return cluster;
			}
			return null;
		}
	} 
	public class kmeanCombiner extends Reducer<IntWritable,Cluster,IntWritable,Cluster>{
		@Override
		protected void reduce(IntWritable key, Iterable<Cluster> values,Context context)
				throws IOException, InterruptedException {
			Instance instance = new Instance();
			int numOfporint = 0;
			for(Cluster cluster:values){
				numOfporint += cluster.getNumofPoints();
				instance.add(cluster.getCneter().multiply(cluster.getNumofPoints()));
				
			}
			Cluster cluster = new Cluster(key.get(),instance.divide(numOfporint));
			cluster.setNumofPoints(numOfporint);
			context.write(key, cluster);
		}
	}
	public class kReduce extends Reducer<IntWritable,Cluster,IntWritable,Cluster>{
		protected void reduce(IntWritable key,Iterable<Cluster> values,Context context) throws IOException, InterruptedException{
			Instance instance = new Instance();
			int numOfPorint = 0;
			for(Cluster cluster:values){
				numOfPorint += cluster.getNumofPoints();
				instance.add(cluster.getCneter().multiply(cluster.getNumofPoints()));
			}
			//以求平均值均值的方法更新中心点信息
			Cluster cluster = new Cluster(key.get(),instance.divide(numOfPorint));
			cluster.setNumofPoints(numOfPorint);
			context.write(key, cluster);
			
		}
	}
}
