package cmd.hbase.hadoop_hbase;



import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

public class hbaseTest {
	@Test
	public void test1() throws Exception, ZooKeeperConnectionException, IOException{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "fwq");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseAdmin admin = new HBaseAdmin(config);
		String table = "t_test";
		if(admin.tableExists(table)){
			System.out.println("wenjiancunzai");
		}else{
			HTableDescriptor t = new HTableDescriptor(table);
			HColumnDescriptor cf1 = new HColumnDescriptor("cf1".getBytes());
			t.addFamily(cf1);
			admin.createTable(t);
		}
		admin.close();
	}
	@Test
	public void test2() throws Exception{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "fwq");
		HTable table = new HTable(config, "t_test");
		String rowkey = "18837472186_"+System.currentTimeMillis();
		Put put =new Put(rowkey.getBytes());
		put.addColumn("cf1".getBytes(), "dest".getBytes(), "13103889556".getBytes());
		put.addColumn("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		put.addColumn("cf1".getBytes(), "time".getBytes(), "2016-2-17 16:17:49".getBytes());
		table.put(put);
		table.close();		
	}
	@Test
	public void test3() throws Exception{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "fwq");
		HTable table = new HTable(config, "t_test");
		Get get = new Get("18837472186_1455697346057".getBytes());
		Result re = table.get(get);
		Cell c1 = re.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
		System.out.println(new String(c1.getValue()));
		
		Scan scan = new Scan();
		scan.setStartRow("18837472186_1455697346000".getBytes());
		scan.setStopRow("18837472186_1455697346999".getBytes());
		ResultScanner res = table.getScanner(scan);
		for(Result r :res){
//			List<Cell> c2 = r.listCells();
//			System.out.println(new String(c2.getValue()));
		}
		table.close();
	}
}
