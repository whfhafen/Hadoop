package result;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class resultFilter {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//hdfs是HDFS文件系统的实例 	local是本地文件系统的实例
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		Path inputDir,localFile;
		
		FileStatus[] inputFiles;
		//相当于建立一个读或写的通道，可以通过这个通道传输数据
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		Scanner scan;
		String str;
		byte[] buf;
		int singleFileLines;
		int numLines,numFiles,i;
		if(args.length!=4){
			System.err.println("输入格式错误");
			return;
		}
		//HDFS上的文件路径
		inputDir = new Path(args[0]);
		//是每个文件最大查询的行数
		singleFileLines = Integer.parseInt(args[3]);
		//HDFS文件列表
		inputFiles = hdfs.listStatus(inputDir);
		numLines = 0;
		numFiles = 1;
		localFile = new Path(args[1]);
		if(local.exists(localFile)){
			local.delete(localFile);
		}
		for(i = 0;i<inputFiles.length;i++){
			if(inputFiles[i].isDir() == true){
				continue;
			}
			System.out.println(inputFiles[i].getPath().getName());
			in = hdfs.open(inputFiles[i].getPath());
			scan = new Scanner(in);
			while(scan.hasNext()){
				str = scan.nextLine();
				//返回指定字符串第一次出现的索引
				if(str.indexOf(args[2])==-1)
					continue;
				System.out.println(str);
				numLines++;
				if(numLines==1)
				{
					localFile = new Path(args[1]+File.separator+numFiles);
					System.out.println(localFile);
					out = local.create(localFile);
					numFiles++;
				}
				buf = (str+"\n").getBytes();
				out.write(buf,0,buf.length);
				if(numLines == singleFileLines)
				{
					out.close();
					numLines = 0;
				}
			}
			scan.close();
			in.close();
		}
		if(out!=null)
			out.close();
	}
}
