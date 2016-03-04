package myHadoop.hadoop2;
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

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
    	//定义int型变量one并初始化为1
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        /*
         *Map类继承自MapReduceBase，并且它实现了Mapper接口，此接口是一个规范类型，
         *它有4种形式的参数，分别用来指定map的输入key值类型、输入value值类型、输出key值类型和输出value值类型。
         *在本例中，因为使用的是TextInputFormat，它的输出key值是LongWritable类型，输出value值是Text类型，
         *所以map的输入类型为<LongWritable,Text>。在本例中需要输出<word,1>这样的形式，
         *因此输出的key值类型是Text，输出的value值类型是IntWritable。
         */   
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
    /*
     * Reduce类也是继承自MapReduceBase的，需要实现Reducer接口。Reduce类以map的输出作为输入，
     * 因此Reduce的输入类型是<Text，Intwritable>。而Reduce的输出是单词和它的数目，因此，
     * 它的输出类型是<Text,IntWritable>。Reduce类也要实现reduce方法，在此方法中，
     * reduce函数将输入的key值作为输出的key值，然后将获得多个value值加起来，作为输出的值。
     */
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
    	/*Job的初始化过程。main函数调用Jobconf类来对MapReduce Job进行初始化，
    	 * 然后调用setJobName()方法命名这个Job。对Job进行合理的命名有助于更快地找到Job，
    	 * 以便在JobTracker和Tasktracker的页面中对其进行监视。
    	 * */
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");
        /*
         * 设置Job输出结果<key,value>的中key和value数据类型，因为结果是<单词,个数>，
         * 所以key设置为"Text"类型，相当于Java中String类型。Value设置为"IntWritable"，相当于Java中的int类型。
         */
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        /*
         * 然后设置Job处理的Map（拆分）、Combiner（中间结果合并）以及Reduce（合并）的相关处理类。
         * 这里用Reduce类来进行Map产生的中间结果合并，避免给网络数据传输产生压力。
         */
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        //接着就是调用setInputPath()和setOutputPath()设置输入输出路径
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        /*InputFormat()方法是用来生成可供map处理的<key,value>对
         * 其中TextInputFormat是Hadoop默认的输入方法，在TextInputFormat中，
         * 每个文件（或其一部分）都会单独地作为map的输入，而这个是继承自FileInputFormat的。
         * 之后，每行数据都会生成一条记录，每条记录则表示成<key,value>形式：
		   key值是每个数据的记录在数据分片中字节偏移量，数据类型是LongWritable；　　
		   value值是每行的内容，数据类型是Text。
         */
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        /*
         * 每一种输入格式都有一种输出格式与其对应。默认的输出格式是TextOutputFormat，这种输出方式与输入类似，
         * 会将每条记录以一行的形式存入文本文件。不过，它的键和值可以是任意形式的，因为程序内容会调用toString()方法将键和值转换为
         * String类型再输出。
         */
        JobClient.runJob(conf);
    }
}
