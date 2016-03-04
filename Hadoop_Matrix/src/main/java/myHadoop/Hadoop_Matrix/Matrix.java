package myHadoop.Hadoop_Matrix;

import java.io.IOException;












import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Matrix {
	public static int columnN;
	public static int rowM;
	public static int colunmM;
	public static class MatrixMap extends Mapper<Object,Text,Text,Text>{
		private Text map_key = new Text();
		private Text map_value = new Text();
		
		public static String filename;
		//执行map函数之前通过setup从输入文件名中取出部分信息 赋值给全局变量
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			columnN = Integer.parseInt(conf.get("columnN"));
			rowM = Integer.parseInt(conf.get("rowM"));
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filename = fileSplit.getPath().getName();
		}
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
		
		if(filename.contains("M")){
			String[] tuple = value.toString().split(",");
			int i = Integer.parseInt(tuple[0]);
			String[] tuples = tuple[1].split("\t");
			int j = Integer.parseInt(tuples[0]);
			int Mij = Integer.parseInt(tuples[1]);
			for(int k =1;k<columnN+1;k++){
				//矩阵M的行号为这一行数据计算后所在的行，M的每一行要与N的columnN行相乘
				map_key.set(i+","+k);
				map_value.set("M"+","+j+","+Mij);
				context.write(map_key, map_value);
			}
		}else if(filename.contains("N")){
			String[] tuple = value.toString().split(",");
			int j = Integer.parseInt(tuple[0]);
			String[] tuples = tuple[1].split("\t");
			int k = Integer.parseInt(tuples[0]);
			int Njk = Integer.parseInt(tuples[1]);
			for(int i =1;i<columnN+1;i++){
				//矩阵N的列号为这一行数据计算后所在的列号，N的每一列要与M的column行相乘
				map_key.set(i+","+k);
				map_value.set("N"+","+j+","+Njk);
				context.write(map_key, map_value);
			}
		}
		}
	}
	public static class MatrixReduce extends Reducer<Text,Text,Text,Text>{
		private int sum = 0;
		
		public	void setup(Context context){
			Configuration conf = context.getConfiguration();
			colunmM = Integer.parseInt(conf.get("colunmM"));
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
		int[] M = new int[colunmM+1];
		int[] N = new int[colunmM+1];
		
		for(Text val:values){
			String[] tuple = val.toString().split(",");
			if(tuple[0].equals("M")){
				M[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
			}else{
				N[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
			}
		}
		for(int j=1;j<colunmM+1;j++){
			sum+=M[j]*N[j];
		}
		context.write(key, new Text(Integer.toString(sum)));
		sum = 0;
		}
	}
	public static void main(String[] args) throws Exception {

	    if (args.length != 3) {
	      System.err
	          .println("Usage: MatrixMultiply <inputPathM> <inputPathN> <outputPath>");
	      System.exit(2);
	    } else {
	      String[] infoTupleM = args[0].split("_");
	      rowM = Integer.parseInt(infoTupleM[1]);
	      colunmM = Integer.parseInt(infoTupleM[2]);
	      String[] infoTupleN = args[1].split("_");
	      columnN = Integer.parseInt(infoTupleN[2]);
	    }

	    Configuration conf = new Configuration();
	    /** 设置三个全局共享变量 **/
	    conf.setInt("rowM", rowM);
	    conf.setInt("colunmM", colunmM);
	    conf.setInt("columnN", columnN);

	    Job job = new Job(conf);
	    job.setJarByClass(Matrix.class);
	    job.setMapperClass(MatrixMap.class);
	    job.setReducerClass(MatrixReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
 