package myHadoop.Hadoop_kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
	//定义一个簇类
public class Cluster implements Writable{
	private int clusterID;					//簇ID
	private long numofPoints;				//属于改簇的点的个数
	private Instance ceneter;				//簇中心点的信息  	Instance也是一个自定义数据类型
	
	public Cluster(){
		this.setClusterID(-1);
		this.setNumofPoints(0);
		this.setCneter(new Instance());
	}
	
	public Cluster(int clusterID,Instance center){
		this.setClusterID(clusterID);
		this.setNumofPoints(0);
		this.setCneter(center);
	}
	
	public Cluster(String line){
		//分离后只取前三个
		String[] value = line.split(",",3);
		clusterID = Integer.parseInt(value[0]);
		numofPoints = Long.parseLong(value[1]);
		ceneter = new Instance(value[2]);
	}
	public String toString(){
		String  result = String.valueOf(clusterID) + "," 
				+ String.valueOf(numofPoints) + "," + ceneter.toString();
		return result;
	}
	
	public void observeInstance(Instance instance){
		try {
			Instance sum = ceneter.multiply(numofPoints).add(instance);
			numofPoints++;
			ceneter = sum.divide(numofPoints);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(clusterID);
		out.writeLong(numofPoints);
		ceneter.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		clusterID = in.readInt();
		numofPoints = in.readLong();
		ceneter.readFields(in);
	}

	public int getClusterID() {
		return clusterID;
	}

	public void setClusterID(int clusterID) {
		this.clusterID = clusterID;
	}

	public long getNumofPoints() {
		return numofPoints;
	}

	public void setNumofPoints(long numofPoints) {
		this.numofPoints = numofPoints;
	}

	public Instance getCneter() {
		return ceneter;
	}

	public void setCneter(Instance cneter) {
		this.ceneter = cneter;
	}

}
