package com.paic.hbasedemo.api.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTable {
	private static final Log LOG = LogFactory.getLog(HBaseTable.class);
	private String dbName;
	private String tableName;
	private boolean autoFlush;
	private Configuration conf;
	private long writeBufferSize;
	private List<Row> writeBuffer;
	private long currentWriteBufferSize;
	private HTableInterface dataTable;
	private boolean closed;
	private List<Put> puts;

	public HBaseTable(String dbName, String tableName, Configuration conf) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.conf = conf;
		this.autoFlush = false;
		this.closed = false;

		if (conf == null) {
			this.conf = HBaseUtils.getConf();
		}

		this.autoFlush = false;
		this.writeBuffer = new ArrayList<Row>();
		this.writeBufferSize = conf.getLong(HBaseUtils.TABLE_WRITE_BUFFER_SIZE,
				1024L * 1024L);
		String dataTableName = HBaseUtils.getHBaseTableName(dbName, tableName);
		dataTable = HBaseUtils.getHBaseTableFromPool(dataTableName);
		dataTable.setAutoFlush(true);
		puts = new ArrayList<Put>();
	}

	/**
	 * Get database name.
	 * 
	 * @return database name.
	 */
	public String getDatabaseName() {
		return this.dbName;
	}

	/**
	 * Get table name.
	 * 
	 * @return table name
	 */
	public String getTableName() {
		return this.tableName;
	}

	/**
	 * Set auto-flush flag. When auto-flush flag is on, every modification is
	 * flushed immediately. When doing batch modification, auto-flush should set
	 * to false. Default is false.
	 * 
	 * @param autoFlush
	 *            auto flushing modification to table.
	 */
	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}

	/**
	 * Get auto-flush flag.
	 * 
	 * @return True if auto-flush is on and vice versa.
	 */
	public boolean isAutoFlush() {
		return this.autoFlush;
	}

	/**
	 * Set write buffer size. When auto-flush is off, modification will be
	 * cached in write buffer until write buffer is full.
	 * 
	 * @param bufferSize
	 *            max number of rows which write buffer can hold
	 * @see HBaseTable#setAutoFlush(boolean)
	 */
	public void setWriteBufferSize(long bufferSize) {
		this.writeBufferSize = bufferSize;
	}

	/**
	 * Get write buffer size.
	 * 
	 * @return write buffer size or 0 when auto-flush is on.
	 */
	public long getWriteBufferSize() {
		if (autoFlush)
			return 0;
		else
			return this.writeBufferSize;
	}

	/**
	 * Get current row number in write buffer.
	 * 
	 * @return number of rows that is not flushed yet.
	 */
	public long getCurrentBufferSize() {
		return this.currentWriteBufferSize;
	}

	/**
	 * Clear current write buffer.
	 * 
	 * @return rows that is clear.
	 */
	public List<Row> clearWriteBuffer() {
		List<Row> unFlushedData = this.writeBuffer;
		this.writeBuffer = new ArrayList<Row>();
		this.currentWriteBufferSize = 0;
		puts.clear();
		return unFlushedData;
	}

	protected void checkNotClosed() throws IOException {
		if (this.closed)
			throw new IOException("Table is closed.");
	}

	protected void insertImpl(Row row, boolean copyRow) throws IOException {
		checkNotClosed();

		if (row == null)
			return;

		Row rowCopy = row;
		if (copyRow) {
			rowCopy = row.copyRow();
		}

		Put put = new Put(rowCopy.getKey());
		put.add(HBaseUtils.HBASE_COLUMN_FAMILY,
				HBaseUtils.HBASE_COLUMN_QUALIFIER, rowCopy.getValue());
		puts.add(put);
		this.currentWriteBufferSize = put.heapSize();

		writeBuffer.add(rowCopy);

		if (autoFlush || currentWriteBufferSize >= this.writeBufferSize)
			flush();
	}

	public void flush() throws IOException {
		checkNotClosed();

		List<Row> failedRows = new ArrayList<Row>();
		List<Put> failedPutLists = new ArrayList<Put>();

		long failedBufferSize = 0;

		Object[] results = new Object[puts.size()];

		try {
			if (puts.size() > 0) {
				dataTable.batch((List) puts, results);
			}
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			for (int p = results.length - 1; p >= 0; p--) {
				if (results[p] instanceof Result == false) {
					failedRows.add(writeBuffer.remove(p));
					Put put = puts.remove(p);
					failedBufferSize += put.heapSize();
					failedPutLists.add(put);
				}
			}

			puts.clear();
		}

		this.writeBuffer.clear();

		if (failedRows.size() > 0) {
			this.writeBuffer = failedRows;
			this.puts = failedPutLists;
			this.currentWriteBufferSize = failedBufferSize;
		} else {
			this.currentWriteBufferSize = 0;
		}

		if (writeBuffer.size() > 0) {
			throw new IOException("Failed to flush all record.");
		}
	}

	public void close() throws IOException {
		if (writeBuffer.size() > 0)
			flush();
		
		dataTable.close();
		this.closed = true;
		LOG.info("Table closed.");
	}
}
