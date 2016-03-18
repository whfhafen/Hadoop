package myHadoop.Hadoop_KNN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneReportReader extends RecordReader{
	//key里面存放文件的名字
	private Text key = null;
	//value里存放文件的路径
	private Text value = null;
	private FSDataInputStream fis = null;
	private FileSplit fs;
	private Configuration con;
	private Path fiel;
	
	public OneReportReader(FileSplit fileSplit,Configuration con){
		this.fs=fileSplit;
		this.con=con;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		 fs = (FileSplit)split;
		Configuration conf = context.getConfiguration();
		fiel = fs.getPath();
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key==null){
			key = new Text();
		}
		key.set(fiel.getName());
		if(value==null){
			value= new Text();
			System.out.println();
		}
		value.set(fiel.toString());
		System.out.println("RecordReader里面的路径"+fiel.toString());
		System.out.println(key+"+"+value);
		return false;
	}

	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		
	}

}
