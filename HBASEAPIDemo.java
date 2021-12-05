package edu.lg.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBASEAPIDemo {
	
	/*
	 * 	测试客户端连接hbase，并创建学生表、
	 * 	如果hbase的服务没有正常运行
	 */
	private Configuration conf;
	private List<String> familyids = new ArrayList<String>();

	@Before
	public void init() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.56.133:2181");
		familyids.add("id");
		familyids.add("name");
		familyids.add("age");
		familyids.add("gender");
		familyids.add("email");
	}
	
	@Test
	public void testCreate() throws Exception {
		//获取HDFS配置
		Configuration conf = new Configuration();
		//通过conf来指定hbase所使用的zooke的地址
		conf.set("hbase.zookeeper.quorum", "192.168.56.133:2181");
		//获取hbase的连接
		HBaseAdmin admin = new HBaseAdmin(conf);
		//指定要创建的表名：student
		TableName tableName = TableName.valueOf("student");
		//描述表
		HTableDescriptor table = new HTableDescriptor(tableName);
		//指定表中的列族信息：basic_info、score_info
		HColumnDescriptor basic = new HColumnDescriptor("basic_info");
		HColumnDescriptor score = new HColumnDescriptor("score_info");
		//将列族添加到表中
		table.addFamily(basic);
		table.addFamily(score);
		//通过连接来创建表
		admin.createTable(table);
		//关闭连接
		admin.close();
	}
	
	/*
	 * 	添加数据
	 */
	@Test
	public void testPut() throws Exception {
		//获取表
		HTable table = new HTable(conf, "student");
		//创建行键：zhangsan_001
		//	java基础 —— "zhangsan_001".getBytes() new String(byte [])
		// HBase的工具类
		// Put put = new Put("zhangsan_001".getBytes());
		Put put = new Put(Bytes.toBytes("lisi_002"));
		//向指定
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("id"), Bytes.toBytes("002"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes("19"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("gender"), Bytes.toBytes("man"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("email"), Bytes.toBytes("lisi@163.com"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("math"), Bytes.toBytes("90"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("english"), Bytes.toBytes("90"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("chinese"), Bytes.toBytes("90"));
		//将put添加到表
		table.put(put);
		//关闭流
		table.close();
	}
	
	/*
	 * 	插入十万条数据，测试时间
	 */
	@Test
	public void testPutTime() throws Exception {
		//开始记录获取表的时间
		double time01 = System.currentTimeMillis();
		HTable table = new HTable(conf, "student");
		//结束记录获取表的时间
		double time02 = System.currentTimeMillis();
		System.out.println("获取表所用时间：" + (time02 - time01)/1000.0 + "秒");
		
		//开始记录写入十万条数据的时间
		double time_begin = System.currentTimeMillis();
			//创建十万条数据并添加到表格
		List<Put> putList = new ArrayList();
		for(int i=1;i<=100000;i++) {
			Put put = new Put(Bytes.toBytes("zs_"+i));
			put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("id"), Bytes.toBytes("id_" + i));
			putList.add(put);
		}
		table.put(putList);
		double time_end = System.currentTimeMillis();
		//结束记录十万条数据的时间
		System.out.println("插入十万条数据所用时间：" + (time_end - time_begin)/1000 + "秒");
			//删除添加的十万条数据
		List<Delete> deleteList = new ArrayList();
		for(int i=1;i<=100000;i++) {
			Delete delete = new Delete(Bytes.toBytes("zs_"+i));
			deleteList.add(delete);
		}
		table.delete(deleteList);
		table.close();
	}
	
	/*
	 * 	删除
	 */
	@Test
	public void testDel() throws Exception {
		HTable table = new HTable(conf, "student");
		Delete delete = new Delete(Bytes.toBytes("zhangsan_001"));
		table.delete(delete);
		table.close();
	}
	
	/*
	 * 	获取数据结果集
	 */
	@Test
	public void testScan() throws Exception {
		//获取表
		HTable table = new HTable(conf, "gsod");
		//执行scan:指定Scan在其中声明行键
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> itResult = scanner.iterator();
		//遍历迭代器
		while(itResult.hasNext()) {
			//取出其中的Result
			Result re = itResult.next();
			System.out.println(re);
			//byte[] value = re.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
			//System.out.println(Bytes.toString(value));
		}
		table.close();
	}
	
	/*
	 * 	获取数据
	 */
	@Test
	public void testGet() throws Exception {
		HTable table = new HTable(conf, "student");
		Get get = new Get(Bytes.toBytes("zhangsan_001"));
		Result result =  table.get(get);
		System.out.println(result);
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("basic_info"));
		System.out.println(Bytes.toString(familyMap.get(Bytes.toBytes("id"))));
		//遍历输出所有列
		for(String ids : familyids) {
			System.out.println(ids + " = " + Bytes.toString(familyMap.get(Bytes.toBytes(ids))));
		}
		
		//其他数据获取方式
		byte[] value = result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
		System.out.println(new String(value));
		System.out.println(Bytes.toString(value));
		table.close();
	}
	
	/*
	 * 	删除表格
	 */
	@Test
	public void testDrop() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName table = TableName.valueOf(Bytes.toBytes("test"), Bytes.toBytes("demo"));
		//admin.deleteTable(table);
		admin.disableTable(table);
		admin.deleteTable(table);
		admin.close();
	}
}
