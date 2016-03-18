package myHadoop.Hadoop_KNN;

import java.util.ArrayList;

public class Distance {
	public static double EuclideanDistance(ArrayList<Integer> a ,ArrayList<Integer> b) throws Exception{
		if(a.size()!=b.size())
			throw new Exception("size not compatible!");
		double sum =0.0;
		for(int i=0;i<a.size();i++){
			sum+=Math.pow(a.get(i)-b.get(i),2);
		}
		return Math.sqrt(sum);
	}

}
