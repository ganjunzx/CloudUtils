package com.paic.hbasedemo.api.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;

public class HBaseScanner implements RowScanner {
	private ResultScanner resultScanner = null;
	private HTableInterface dataTable;
	private long limit = -1;
	private long count = 0;
	
	public HBaseScanner(String hbaseTableName, byte[] startKey, byte[] stopKey) {
		this(hbaseTableName, startKey, stopKey, -1);
	}
	
	public HBaseScanner(String hbaseTableName, byte[] startKey, byte[] stopKey, long limit) {
		try {
			dataTable = HBaseUtils.getHBaseTableFromPool(hbaseTableName);
			Scan scan = new Scan();
			if (startKey != null) {
				scan.setStartRow(startKey);
			} 
			
			if (stopKey != null) {
				scan.setStopRow(stopKey);
			}
			scan.addColumn(HBaseUtils.HBASE_COLUMN_FAMILY, HBaseUtils.HBASE_COLUMN_QUALIFIER);
			resultScanner = dataTable.getScanner(scan);
			
			this.limit = limit;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Object next() throws IOException {
		// TODO Auto-generated method stub
		if (limit != -1) {
			if (count >= limit) {
				return null;
			} else {
				count++;
				return resultScanner.next();
			}
		} else {
			return resultScanner.next();
		}
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (resultScanner != null) {
			resultScanner.close();
			resultScanner = null;
		}
	}


}
