package myHadoop.Hadoop_kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kmeansRunJob {
	private int k;
	private int iterationNum;
	private String sourcePath;
	private String outputPath;
	private Configuration conf;
	public kmeansRunJob(int k,int iterationNum,String sourcePath,String outputPath,Configuration conf){
		this.k=k;
		this.iterationNum=iterationNum;
		this.sourcePath=sourcePath;
		this.outputPath=outputPath;
		this.conf=conf;
	}
	public void ClusterJob() throws IOException{
		for(int i = 0;i<iterationNum;i++){
			Job job = new Job(conf);
			
			job.setJobName("kmeansJob"+i);
			job.setJarByClass(Kmeans.class);
			job.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i +"/");
			
			job.setMapperClass(Kmeans.Kmap.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Cluster.class);
			
			job.setNumReduceTasks(0);
			
			job.setCombinerClass(Kmeans.kmeanCombiner.class);
			
			job.setReducerClass(Kmeans.kReduce.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Cluster.class);
			
			FileInputFormat.addInputPath(job, new Path(sourcePath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/cluster-" + (i + 1) +"/"));
			
			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	public void KMeansClusterJod() throws IOException{
	
			Job job = new Job(conf);
			
			job.setJobName("kmeansJob");
			job.setJarByClass(Kmeans.class);
			
			job.setMapperClass(Kmeans.Kmap.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Cluster.class);
			
			job.setNumReduceTasks(0);
			
			job.setCombinerClass(Kmeans.kmeanCombiner.class);
			
			job.setReducerClass(Kmeans.kReduce.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Cluster.class);
			
			FileInputFormat.addInputPath(job, new Path(sourcePath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
	}
	public void generateInitialCluster(){
		RandomClusterGenerator generator = new RandomClusterGenerator(conf, sourcePath, k);
		generator.generateInitialCluster(outputPath + "/");
	}
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		int k = Integer.parseInt(args[0]);
		int iterationNum = Integer.parseInt(args[1]);
		String sourcePath = args[2];
		String outputPath = args[3];
		kmeansRunJob run = new  kmeansRunJob(k,iterationNum,sourcePath,outputPath,conf);
		run.generateInitialCluster();
		try {
			run.KMeansClusterJod();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
