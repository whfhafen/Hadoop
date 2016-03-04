import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class Test1 {
	@Test
	public void test1() throws Exception, ZooKeeperConnectionException, IOException{
		Configuration conf = HBaseConfiguration.create();
		//连接Hbase 前面是配置文件中的配置 后面是属性
		conf.set("hbase.zookeeper.quorum", "fwq");
		String tableName = "t_name";
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			HTable table = new HTable(conf, tableName);
			String rowkey = "sjdihg";
	
			Put p = new Put(rowkey.getBytes());
			p.addColumn("name".getBytes(), "xiaoming".getBytes(),"34".getBytes());
			table.put(p);
			table.close();
		}else{
			//定义表
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			//定义列族
			HColumnDescriptor cf = new HColumnDescriptor("name".getBytes());
			tableDesc.addFamily(cf);
			admin.createTable(tableDesc);
		}
	}
	
	@Test
	public void test2() throws Exception, ZooKeeperConnectionException, IOException{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "fwq");
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable table = new HTable(conf,"t_name");
		String rowkey = "thrid";
		Put put = new Put(rowkey.getBytes());
		put.addColumn("name".getBytes(), "lisi".getBytes(), "156".getBytes());
		put.addColumn("name".getBytes(), "wangwu".getBytes(), "d45".getBytes());
		put.addColumn("name".getBytes(), "shiliu".getBytes(), "s44".getBytes());
		put.addColumn("name".getBytes(), "aiji".getBytes(), "e77".getBytes());
		put.addColumn("name".getBytes(), "banba".getBytes(), "d44".getBytes());
		table.put(put);
		table.close();
		
	}
	@Test
	public void test3() throws IOException{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "fwq");
		HTable table = new HTable(conf,"t_name");
		Delete deleteRow = new Delete("sjdihg".getBytes());
		table.delete(deleteRow);
	}
	@Test
	public void test4() throws IOException{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "fwq");
		HTable table = new HTable(conf,"t_name");
//		Delete delectColumn = new Delete("sjdihg".getBytes());
//		delectColumn.deleteColumns("name".getBytes(), "lisi".getBytes());
//		table.delete(delectColumn);
		
		
		//append是添加 put是替换
//		Append append = new Append("sjdihg".getBytes());
//		append.add("name".getBytes(), "xiaoming".getBytes(), "12345".getBytes());
//		table.append(append);
		
		Increment in = new Increment("sjdihg".getBytes());
		in.addColumn("name".getBytes(), "xiaoming".getBytes(),99);
		table.increment(in);
	}
	@Test
	public void test_read() throws IOException{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "fwq");
		HTable table = new HTable(conf,"t_name");
		Get get = new Get("sjdihg".getBytes());
		Result result = table.get(get);
		for(KeyValue kv : result.raw()){
			System.out.println("row:"+new String(kv.getRow())+" ");
			System.out.println("family:"+new String(kv.getFamily())+" ");
			System.out.println("qualifier:"+new String(kv.getQualifier())+" ");
			System.out.println("timestamp:"+kv.getTimestamp()+" ");
			System.out.println("value:"+new String(kv.getValue())+" ");
		}
		Scan scan = new Scan();
		ResultScanner rs = table.getScanner(scan);
		for(Result r:rs){
			System.out.println("scan:"+r);
		}
	}
	@Test
	public void test_scanByFilter() throws IOException{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "fwq");
		HTable table = new HTable(conf,"t_name");
		Scan scan = new Scan();
		//scan.addColumn("name".getBytes(), "xiaoming".getBytes());
//		Filter filter = new SingleColumnValueFilter("name".getBytes(),
//				"xiaoming".getBytes(), CompareOp.EQUAL,"3412345".getBytes());
		//Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
		SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("name"), 
				Bytes.toBytes("lisi"),CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("qq"));
		scvf.setFilterIfMissing(false);
		scvf.setLatestVersionOnly(true);
		// OK 如果突然发现一行中的列数超过设定的最大值时，整个扫描操作会停止  
		Filter ccf = new ColumnCountGetFilter(6);
		//筛选某个（值的条件满足的）特定的单元格
		Filter vf = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("77"));
		//筛选出前缀匹配的列  
		Filter of = new ColumnPrefixFilter(Bytes.toBytes("l"));
		//返回每一个rowkey的第一列
		Filter only = new FirstKeyOnlyFilter();
		//返回所有数据但值全为空
		Filter key = new KeyOnlyFilter();
		//返回rowkey前缀匹配的值  只能匹配前缀 中间的不行
		Filter row = new PrefixFilter("h".getBytes());
		scan.setFilter(row);
		ResultScanner re = table.getScanner(scan);
		for(Result r : re){
			System.out.println(r);
		}
		
	}
}
