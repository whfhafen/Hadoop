package myHadoop.hadoopHot;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupHot extends WritableComparator{
	public GroupHot(){
		super(keypair.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		keypair key1 = (keypair) a;
		keypair key2 = (keypair) b;
		//年份相同的就合并
		return Integer.compare(key1.getYear(), key2.getYear());
	}
}
