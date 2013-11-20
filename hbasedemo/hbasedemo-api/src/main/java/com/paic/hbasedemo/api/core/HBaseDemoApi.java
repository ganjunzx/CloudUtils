package com.paic.hbasedemo.api.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.grammar.v3.ANTLRParser.finallyClause_return;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hive.conf.HiveConf;

public class HBaseDemoApi {

	public void createTable(String tableName) {
		try {
			String hbaseTableName = HBaseUtils.getHBaseTableName(
					"test".toUpperCase(), tableName.toUpperCase());
			HBaseAdmin hbaseAdmin = HBaseUtils.getHBaseAdmin();

			if (hbaseAdmin.tableExists(hbaseTableName) == false
					&& hbaseAdmin.isTableAvailable(hbaseTableName) == false) {
				HTableDescriptor tableDesc = new HTableDescriptor(
						hbaseTableName);
				HColumnDescriptor columnDesc = new HColumnDescriptor(
						HBaseUtils.HBASE_COLUMN_FAMILY);

				columnDesc.setCompressionType(HBaseUtils
						.getHBaseCompressionType());
				columnDesc.setCompactionCompressionType(HBaseUtils
						.getHBaseCompressionType());

				int version = 1;
				if (HBaseUtils.isHBaseKeyUnique() == false)
					version = Integer.MAX_VALUE;

				columnDesc.setMaxVersions(version);
				tableDesc.addFamily(columnDesc);
				tableDesc.setDeferredLogFlush(HBaseUtils
						.isHBaseDeferredLogFlush());

				hbaseAdmin.createTable(tableDesc);

			} else {
				System.out.println("HBase table \'" + tableName
						+ "\' already exists!");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void dropTable(String tableName) {
		try {
			String hbaseTableName = HBaseUtils.getHBaseTableName(
					"test".toUpperCase(), tableName.toUpperCase());
			HBaseAdmin hbaseAdmin = HBaseUtils.getHBaseAdmin();
			if (hbaseAdmin.tableExists(hbaseTableName) == false) {
				System.out.println("Unknown table " + tableName);
			}
			if (hbaseAdmin.isTableEnabled(hbaseTableName)) {
				hbaseAdmin.disableTable(hbaseTableName);
			}

			if (hbaseAdmin.isTableAvailable(hbaseTableName)) {
				hbaseAdmin.deleteTable(hbaseTableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertRow(String tableName, Row row) {
		HTableInterface hbTable = null;
		try {
			String hbaseTableName = HBaseUtils.getHBaseTableName(
					"test".toUpperCase(), tableName.toUpperCase());
			if (!HBaseUtils.getHBaseAdmin().tableExists(hbaseTableName)) {
				System.out.println("Unknown table " + tableName);
			}
			byte[] key = row.getKey();
			byte[] value = row.getValue();
			Put put = new Put(Arrays.copyOf(key, key.length));
			put.add(HBaseUtils.HBASE_COLUMN_FAMILY,
					HBaseUtils.HBASE_COLUMN_QUALIFIER,
					Arrays.copyOf(value, value.length));
			hbTable = HBaseUtils.getHBaseTableFromPool(hbaseTableName);
			hbTable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (hbTable != null) {
				try {
					hbTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				hbTable = null;
			}
		}
	}

	public void insert(String tableName, String strPath) {
		try {
			if (HDFSUtils.exists(strPath)) {
				MultiThreadTableInserter multiInsert = new MultiThreadTableInserter(
						HBaseUtils.getConf(), "test".toUpperCase(),
						tableName.toUpperCase(), 10, 100);
				DataProc dataProc = new DataProc(HBaseUtils.getConf());
				if (HDFSUtils.isDirectory(strPath)) {
					Map<String, Long> mapPaths = HDFSUtils.lsdir(strPath);
					Set<String> listPaths = mapPaths.keySet();

					for (String strFile : listPaths) {
						System.out.println(new Path(strPath, strFile)
								.toString());
						if (HDFSUtils.isFile(new Path(strPath, strFile)
								.toString())) {
							BufferedReader reader = new BufferedReader(
									new InputStreamReader(HDFSUtils
											.openReadHdfsFile(new Path(strPath,
													strFile).toString()),
											"UTF-8"));
							String line;
							while ((line = reader.readLine()) != null) {
								Row row = dataProc.proc(line);
								multiInsert.insertRow(row);
							}
							reader.close();
						} else {
							System.out.println(new Path(strPath, strFile)
									.toString() + " is a directory!");
						}
					}
				} else {
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(
									HDFSUtils.openReadHdfsFile(strPath),
									"UTF-8"));
					String line;
					while ((line = reader.readLine()) != null) {
						Row row = dataProc.proc(line);
						multiInsert.insertRow(row);
					}
					reader.close();
				}
				multiInsert.close();
			} else {
				System.out.println("Dir " + strPath + " not exist!");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteRow(String tableName, Row row) {
		HTableInterface hbTable = null;
		try {
			String hbaseTableName = HBaseUtils.getHBaseTableName(
					"test".toUpperCase(), tableName.toUpperCase());
			if (!HBaseUtils.getHBaseAdmin().tableExists(hbaseTableName)) {
				System.out.println("Unknown table " + tableName);
			}
			hbTable = HBaseUtils.getHBaseTableFromPool(hbaseTableName);
			Delete delete = new Delete(row.getKey());
			hbTable.delete(delete);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (hbTable != null) {
				try {
					hbTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				hbTable = null;
			}
		}
	}

	public void queryResult(String tableName, String startKey, String stopKey) {
		HBaseScanner hBaseScanner = null;
		try {
			String hbaseTableName = HBaseUtils.getHBaseTableName(
					"test".toUpperCase(), tableName.toUpperCase());
			if (!HBaseUtils.getHBaseAdmin().tableExists(hbaseTableName)) {
				System.out.println("Unknown table " + tableName);
			}
			hBaseScanner = new HBaseScanner(hbaseTableName,
					startKey.getBytes(), stopKey.getBytes());
			Object result;
			long count = 0;
			while ((result = hBaseScanner.next()) != null) {
				if (result instanceof Result) {
					count++;
					Set<byte[]> families = ((Result) result).getMap().keySet();
					assert families.size() == 1;
					byte[] family = families.toArray(new byte[0][])[0];
					byte[] key = ((Result) result).getRow();
					byte[] value = ((Result) result).getValue(family,
							HBaseUtils.HBASE_COLUMN_QUALIFIER);
					System.out.println("Row " + count + ": [key]"
							+ Bytes.toString(key) + "\t\t[value]"
							+ Bytes.toString(value));
				} else {
					System.out.println("inner error!");
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (hBaseScanner != null) {
				try {
					hBaseScanner.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void scanResult(String tableName, long limit) {
		HBaseScanner hBaseScanner = null;
		try {
			String hbaseTableName = HBaseUtils.getHBaseTableName(
					"test".toUpperCase(), tableName.toUpperCase());
			if (!HBaseUtils.getHBaseAdmin().tableExists(hbaseTableName)) {
				System.out.println("Unknown table " + tableName);
			}

			hBaseScanner = new HBaseScanner(hbaseTableName, null, null, limit);
			Object result;
			long count = 0;
			while ((result = hBaseScanner.next()) != null) {
				if (result instanceof Result) {
					count++;
					Set<byte[]> families = ((Result) result).getMap().keySet();
					assert families.size() == 1;
					byte[] key = ((Result) result).getRow();
					byte[] family = families.toArray(new byte[0][])[0];
					byte[] value = ((Result) result).getValue(family,
							HBaseUtils.HBASE_COLUMN_QUALIFIER);
					System.out.println("Row " + count + ": [key]"
							+ Bytes.toString(key) + "\t\t[value]"
							+ Bytes.toString(value));
				} else {
					System.out.println("inner error!");
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (hBaseScanner != null) {
				try {
					hBaseScanner.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		/*String tableName = "hbasedemoapi";
		HBaseDemoApi hBaseDemoApi = new HBaseDemoApi();
		hBaseDemoApi.createTable(tableName);
		hBaseDemoApi.createTable("gdr");
		Row row1 = new Row("tom1".getBytes(), "tom1,1,91".getBytes());
		Row row2 = new Row("tom2".getBytes(), "tom2,2,92".getBytes());
		Row row3 = new Row("tom3".getBytes(), "tom3,3,93".getBytes());
		Row row4 = new Row("tom4".getBytes(), "tom4,4,94".getBytes());
		Row row5 = new Row("tom5".getBytes(), "tom5,5,95".getBytes());
		hBaseDemoApi.insertRow(tableName, row1);
		hBaseDemoApi.insertRow(tableName, row2);
		hBaseDemoApi.insertRow(tableName, row3);
		hBaseDemoApi.insertRow(tableName, row4);
		hBaseDemoApi.insertRow(tableName, row5);

		System.out
				.println("-----------------------Get a row start!---------------");
		hBaseDemoApi.queryResult(tableName, "tom1", "tom1");
		System.out
				.println("-----------------------Get a row end!---------------");

		hBaseDemoApi.deleteRow(tableName, row1);

		System.out
				.println("-----------------------Get multi-row start!---------------");
		hBaseDemoApi.queryResult(tableName, "tom1", "tom5");
		System.out
				.println("-----------------------Get multi-row end!---------------");

		System.out
				.println("-----------------------Get multi-row start!---------------");
		hBaseDemoApi.scanResult(tableName, -1);
		System.out
				.println("-----------------------Get multi-row end!---------------");
		hBaseDemoApi.dropTable(tableName);

		System.out
				.println("-----------------------insert gdr multi-row start!---------------");
		hBaseDemoApi.insert("gdr", "/gdr/import");
		System.out
				.println("-----------------------insert gdr multi-row end!---------------");

		System.out
				.println("-----------------------Get multi-row start!---------------");
		hBaseDemoApi.scanResult("gdr", 100);
		System.out
				.println("-----------------------Get multi-row end!---------------");*/
		
		try {
			System.out.println("Dir \'/gdr/import\' size:\t" + HDFSUtils.getLength("/gdr/import"));
			System.out.println("Dir \'/gdr\' size:\t" + HDFSUtils.getLength("/gdr"));
			System.out.println("Dir \'/gdr/import/GPRS_GNGDR13102417.000000.141e9ca956f\' size:\t" + HDFSUtils.getLength("/gdr/import/GPRS_GNGDR13102417.000000.141e9ca956f"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
