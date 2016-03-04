package myHadoop.hadoopHot;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class sortHot extends WritableComparator{
	public sortHot(){
		super(keypair.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		keypair key1 = (keypair) a;
		keypair key2 = (keypair) b;
		//升序排序
		int result = Integer.compare(key1.getYear(), key2.getYear());
		if(result!=0){
			return result;
		}
		//降序排序
		return -Integer.compare(key1.getHot(), key2.getHot());
	}
}
