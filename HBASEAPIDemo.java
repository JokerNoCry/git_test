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
	 * 	���Կͻ�������hbase��������ѧ����
	 * 	���hbase�ķ���û����������
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
		//��ȡHDFS����
		Configuration conf = new Configuration();
		//ͨ��conf��ָ��hbase��ʹ�õ�zooke�ĵ�ַ
		conf.set("hbase.zookeeper.quorum", "192.168.56.133:2181");
		//��ȡhbase������
		HBaseAdmin admin = new HBaseAdmin(conf);
		//ָ��Ҫ�����ı�����student
		TableName tableName = TableName.valueOf("student");
		//������
		HTableDescriptor table = new HTableDescriptor(tableName);
		//ָ�����е�������Ϣ��basic_info��score_info
		HColumnDescriptor basic = new HColumnDescriptor("basic_info");
		HColumnDescriptor score = new HColumnDescriptor("score_info");
		//��������ӵ�����
		table.addFamily(basic);
		table.addFamily(score);
		//ͨ��������������
		admin.createTable(table);
		//�ر�����
		admin.close();
	}
	
	/*
	 * 	�������
	 */
	@Test
	public void testPut() throws Exception {
		//��ȡ��
		HTable table = new HTable(conf, "student");
		//�����м���zhangsan_001
		//	java���� ���� "zhangsan_001".getBytes() new String(byte [])
		// HBase�Ĺ�����
		// Put put = new Put("zhangsan_001".getBytes());
		Put put = new Put(Bytes.toBytes("lisi_002"));
		//��ָ��
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("id"), Bytes.toBytes("002"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes("19"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("gender"), Bytes.toBytes("man"));
		put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("email"), Bytes.toBytes("lisi@163.com"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("math"), Bytes.toBytes("90"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("english"), Bytes.toBytes("90"));
		put.add(Bytes.toBytes("score_info"), Bytes.toBytes("chinese"), Bytes.toBytes("90"));
		//��put��ӵ���
		table.put(put);
		//�ر���
		table.close();
	}
	
	/*
	 * 	����ʮ�������ݣ�����ʱ��
	 */
	@Test
	public void testPutTime() throws Exception {
		//��ʼ��¼��ȡ���ʱ��
		double time01 = System.currentTimeMillis();
		HTable table = new HTable(conf, "student");
		//������¼��ȡ���ʱ��
		double time02 = System.currentTimeMillis();
		System.out.println("��ȡ������ʱ�䣺" + (time02 - time01)/1000.0 + "��");
		
		//��ʼ��¼д��ʮ�������ݵ�ʱ��
		double time_begin = System.currentTimeMillis();
			//����ʮ�������ݲ���ӵ����
		List<Put> putList = new ArrayList();
		for(int i=1;i<=100000;i++) {
			Put put = new Put(Bytes.toBytes("zs_"+i));
			put.add(Bytes.toBytes("basic_info"), Bytes.toBytes("id"), Bytes.toBytes("id_" + i));
			putList.add(put);
		}
		table.put(putList);
		double time_end = System.currentTimeMillis();
		//������¼ʮ�������ݵ�ʱ��
		System.out.println("����ʮ������������ʱ�䣺" + (time_end - time_begin)/1000 + "��");
			//ɾ����ӵ�ʮ��������
		List<Delete> deleteList = new ArrayList();
		for(int i=1;i<=100000;i++) {
			Delete delete = new Delete(Bytes.toBytes("zs_"+i));
			deleteList.add(delete);
		}
		table.delete(deleteList);
		table.close();
	}
	
	/*
	 * 	ɾ��
	 */
	@Test
	public void testDel() throws Exception {
		HTable table = new HTable(conf, "student");
		Delete delete = new Delete(Bytes.toBytes("zhangsan_001"));
		table.delete(delete);
		table.close();
	}
	
	/*
	 * 	��ȡ���ݽ����
	 */
	@Test
	public void testScan() throws Exception {
		//��ȡ��
		HTable table = new HTable(conf, "gsod");
		//ִ��scan:ָ��Scan�����������м�
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> itResult = scanner.iterator();
		//����������
		while(itResult.hasNext()) {
			//ȡ�����е�Result
			Result re = itResult.next();
			System.out.println(re);
			//byte[] value = re.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
			//System.out.println(Bytes.toString(value));
		}
		table.close();
	}
	
	/*
	 * 	��ȡ����
	 */
	@Test
	public void testGet() throws Exception {
		HTable table = new HTable(conf, "student");
		Get get = new Get(Bytes.toBytes("zhangsan_001"));
		Result result =  table.get(get);
		System.out.println(result);
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("basic_info"));
		System.out.println(Bytes.toString(familyMap.get(Bytes.toBytes("id"))));
		//�������������
		for(String ids : familyids) {
			System.out.println(ids + " = " + Bytes.toString(familyMap.get(Bytes.toBytes(ids))));
		}
		
		//�������ݻ�ȡ��ʽ
		byte[] value = result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
		System.out.println(new String(value));
		System.out.println(Bytes.toString(value));
		table.close();
	}
	
	/*
	 * 	ɾ�����
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
