package myHadoop.hadoop1;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class WordCount {

    public static class WordCountMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            //StringTokenizer(String str)。默认以” \t\n\r\f”（前有一个空格，引号不是）为分割符分割字符串中的单词
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                //集合手机单词并全部将value值赋值为1
                output.collect(word, one);
            }
        }
    }

    public static class WordCountReducer extends  MapReduceBase implements
    Reducer <Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += ((IntWritable) values.next()).get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	String input = "hdfs://10.10.65.14:9000/whf/lasion";
        String output = "hdfs://10.10.65.14:9000/whf/diyici";
        JobConf conf = new JobConf(WordCount.class);
        conf.addResource("classpath:/hadoop1/core-site.xml");
        conf.addResource("classpath:/hadoop1/hdfs-site.xml");
        conf.addResource("classpath:/hadoop1/mapred-site.xml");
       conf.addResource("classpath:/hadoop1/yarn-site.xml");
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReducer.class);
        conf.setReducerClass(WordCountReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }

} 
