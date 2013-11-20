package com.paic.hbasedemo.api.core;

import org.apache.hadoop.conf.Configuration;

public class DataProc {
	private Configuration conf;
	int[] rowkeyCols;
	
	public DataProc(Configuration conf) {
		this.conf = conf;
		String strCol = conf.get("hbase.rowkey.columns", "1");
		String[] temps = strCol.split(",");
		rowkeyCols = new int[temps.length];
		for (int i = 0; i < temps.length; i++) {
			rowkeyCols[0] = Integer.parseInt(temps[i].trim()) - 1;
		}
	}
	
	public Row proc(String text) {
		String[] strCols = text.split(",");
		String strKey = "";
		try {
			for (int i = 0; i < rowkeyCols.length; i++) {
				if (i == 0) {
					strKey = strCols[rowkeyCols[i]].trim();
				} else {
					strKey = "|" + strCols[rowkeyCols[i]].trim();
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}
		
		if (strKey.equals("")) {
			return null;
		}
		Row row = new Row(strKey.getBytes(), text.getBytes());
		return row;
	}
}
