package com.hcl.poc.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableScan {

	static Configuration configuration = HBaseConfiguration.create();
	private HBaseAdmin admin;

	public static void main(String[] args) {

		Table table = null;
		Connection conn = null;
		HBaseTableScan hBaseTableScan = null;
		TableName tableName = null;
		try {
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
			configuration.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
			configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

			System.out.println("HBase is running!");
			hBaseTableScan = new HBaseTableScan();
			tableName = TableName.valueOf("people");
			conn = ConnectionFactory.createConnection(configuration);
			table = conn.getTable(tableName);
			// creating a new table
			hBaseTableScan.createHBaseTable(tableName);
			// Inseting data
			hBaseTableScan.putDataInHBase(table);
			// Fetching data
			hBaseTableScan.fetchDataUsingGet(table);
			hBaseTableScan.fetchDataUsingScan(table);
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			try {
				table.close();
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			hBaseTableScan = null;
			tableName = null;
		}

	}

	private void fetchDataUsingScan(Table table) {
		System.out.println("Scan results....");

		ResultScanner scanner = null;
		Scan scan = null;
		try {
			scan = new Scan(Bytes.toBytes("doe-john-m-12345"));
			scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"));
			scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
			scan.setFilter(new PageFilter(25));
			scanner = table.getScanner(scan);
			for (Result result : scanner) {
				System.out.println(result);
			}
		} catch (Exception e) {

			e.printStackTrace();

		} finally {
			scanner.close();
			scan = null;
		}

	}

	private void fetchDataUsingGet(Table table) {
		try {
			Get get = new Get(Bytes.toBytes("doe-john-m-12345"));
			get.addFamily(Bytes.toBytes("personal"));
			get.setMaxVersions(3);
			Result result = table.get(get);
			System.out.println(result);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void putDataInHBase(Table table) {
		Put put = null;
		try {// HBase provides the HConnection class which provides
				// functionality similar to connection pool classes to share
				// connections

			put = new Put(Bytes.toBytes("doe-john-m-12345"));
			put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
			put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
			put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
			put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("john.m.doe@gmail.com"));
			table.put(put);

		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			put = null;
		}
	}

	@SuppressWarnings("deprecation")
	private void createHBaseTable(TableName tableName) throws IOException {
		admin = new HBaseAdmin(configuration);
		HTableDescriptor tableDescriptor = null;
		try {

			tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("personal"));
			tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
			tableDescriptor.addFamily(new HColumnDescriptor("creditcard"));
			admin.createTable(tableDescriptor);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			admin = null;
			tableDescriptor = null;
		}
		System.out.println("Table Created... ");
	}

}
