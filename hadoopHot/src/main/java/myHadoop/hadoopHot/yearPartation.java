package myHadoop.hadoopHot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class yearPartation extends Partitioner<keypair, Text>{

	public int getPartition(keypair key, Text value, int numPartitions) {
		return (key.getYear()*127)% numPartitions;
	}

}
