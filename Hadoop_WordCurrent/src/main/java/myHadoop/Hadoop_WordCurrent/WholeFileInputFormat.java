package myHadoop.Hadoop_WordCurrent;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

@SuppressWarnings({ "unused", "rawtypes" })
public class WholeFileInputFormat extends FileInputFormat {

	@Override
	public RecordReader createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new SingleFileNameReader((FileSplit)split,context.getConfiguration());
	}

	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
