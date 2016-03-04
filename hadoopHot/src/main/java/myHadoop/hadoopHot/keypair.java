package myHadoop.hadoopHot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class keypair implements WritableComparable<keypair>{
	private int year;
	private int hot;
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getHot() {
		return hot;
	}
	public void setHot(int hot) {
		this.hot = hot;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(hot);
	}
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.hot = in.readInt();
	}
	public int compareTo(keypair o) {
		int result = Integer.compare(year, o.getYear());
		if(result!=0){
			return result;
		}
		return Integer.compare(hot, o.getHot());
	}
}
