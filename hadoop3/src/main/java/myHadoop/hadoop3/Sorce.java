package myHadoop.hadoop3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;



public class Sorce extends Configured implements Tool{
	public int run(String[] args)throws Exception{
		Job job = new Job((JobConf) getConf());
		job.setJobName("sorceNmae");
		
		return 0;
		
	}
}
