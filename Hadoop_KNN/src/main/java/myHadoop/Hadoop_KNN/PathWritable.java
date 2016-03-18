package myHadoop.Hadoop_KNN;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PathWritable implements Writable{
	private Path path;
	private IntWritable Lable_name;
	
	public PathWritable(){
		this.path=path;
		this.Lable_name = Lable_name;
	}
	
	public void write(DataOutput out) throws IOException {
		Lable_name.write(out);
		out.writeUTF(path.toString());
		
	}

	public void readFields(DataInput in) throws IOException {
		Lable_name.readFields(in);
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public IntWritable getLable_name() {
		return Lable_name;
	}

	public void setLable_name(IntWritable lable_name) {
		Lable_name = lable_name;
	}

}
