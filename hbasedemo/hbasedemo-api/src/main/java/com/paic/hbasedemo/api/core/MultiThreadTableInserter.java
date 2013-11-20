package com.paic.hbasedemo.api.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class MultiThreadTableInserter {
	private static final Log LOG = LogFactory
			.getLog(MultiThreadTableInserter.class);

	private final int threadNum;// 设置线程数
	private final long recordNum;// 设置记录数，当达到这个值后就写一次数据
	private long currentRecordNum;// 记录现在的记录数
	private final ArrayBlockingQueue<HBaseTable> tableQueue;// 阻塞队列，当达到队列容量后，准备插入的值将会阻塞，等待空对列
	private HBaseTable currentTable;// 现在使用的HBaseTable表
	private final ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();// 读写锁，可以并行读，和阻塞写操作
	private final ExecutorService executor;// 启动线程池
	private List<Row> rows;// = new ArrayList<Row>();//待插入的行
	private final List<Row> invalidRows = Collections
			.synchronizedList(new ArrayList<Row>());// 一个线程安全的List,无效的列
	private final List<Row> failedRows = new ArrayList<Row>();// 失败的列
	private final HBaseTable metaTable;
	private final ArrayBlockingQueue<List<Row>> pools;
	private final Configuration conf;

	public MultiThreadTableInserter(Configuration conf, String dbName,
			String tableName, int threadNum,
			long recordNum) throws IOException {
		this.conf = conf;
		this.threadNum = threadNum;
		this.recordNum = recordNum;
		this.executor = Executors.newFixedThreadPool(threadNum);
		this.currentRecordNum = 0;

		int qlen = conf.getInt("hbase.client.queueoflist", threadNum * 6);
		this.pools = new ArrayBlockingQueue<List<Row>>(qlen);
		for (int i = 0; i < qlen; i++) {
			pools.add(new ArrayList<Row>());
		}

		this.tableQueue = new ArrayBlockingQueue<HBaseTable>(threadNum * 2);

		synchronized (MultiThreadTableInserter.class) { // multi-thread risk in
														// object-store access?
			for (int i = 0; i < threadNum; ++i) {
				HBaseTable table = new HBaseTable(dbName, tableName, conf);
				table.setAutoFlush(false);
				table.setWriteBufferSize(Long.MAX_VALUE);
				tableQueue.add(table);
			}
		}

		metaTable = new HBaseTable(dbName, tableName, conf);
	}

	public void insertRow(Row row) throws InterruptedException {
		flushLock.readLock().lock();

		try {
			if (currentTable == null) {
				currentTable = tableQueue.take();
			}

			if (rows == null) {
				rows = pools.take();
			}

			rows.add(row);
			++currentRecordNum;

			if (this.currentRecordNum > this.recordNum) {
				this.flushCurrentTable();
			}
		} finally {
			flushLock.readLock().unlock();
		}
	}

	private void flushCurrentTable() {
		executor.submit(new FlushCall(currentTable, rows));
		rows = null;// new ArrayList<Row>();
		this.currentRecordNum = 0;
		currentTable = null;
	}

	public void close() throws IOException {
		flush();
		executor.shutdownNow();

		ArrayList<HBaseTable> tables = new ArrayList<HBaseTable>();
		tableQueue.drainTo(tables);

		for (HBaseTable table : tables) {
			table.close();
		}
	}

	public void flush() throws IOException {
		flushLock.writeLock().lock();

		try {
			ArrayList<HBaseTable> tables = new ArrayList<HBaseTable>();
			tableQueue.drainTo(tables);
			//往HBASE里插入数据
			if (currentTable != null) {
				flushCurrentTable();
			}
			//flush数据到磁盘
			for (HBaseTable table : tables) {
				executor.submit(new FlushCall(table, null));
			}

			tables.clear();
			//由于线程FlushCall处理完数据后，继续往tableQueue放了table，所以这里可以读取
			try {
				for (int i = 0; i < threadNum; ++i) {
					tables.add(tableQueue.take());
				}
			} catch (InterruptedException ie) {
				throw new IOException("Flush interrupted.", ie);
			}

			boolean flushError = false;

			for (HBaseTable table : tables) {
				List<Row> unFlushedData = table.clearWriteBuffer();

				if (unFlushedData.size() > 0) {
					flushError = true;
				}

				failedRows.addAll(unFlushedData);
				tableQueue.add(table);
			}

			if (flushError) {
				LOG.error("failed flush all records, failed rows = "
						+ failedRows.size());
				throw new IOException("Unable to flush all records");
			}
		} finally {
			flushLock.writeLock().unlock();
		}
	}

	/**
	 * Get invlaid rows. Invalid rows are usually failed to serialized before
	 * inserting to HBase table.
	 * 
	 * @return invalid rows.
	 */
	public List<Row> getInvalidRows() {
		return this.invalidRows;
	}

	/**
	 * Get rows that failed to insert into HBase table.
	 * 
	 * @return failed rows
	 */
	public List<Row> getFailedRows() {
		return this.failedRows;
	}

	private class FlushCall implements Runnable {
		private final HBaseTable table;
		private final List<Row> rows;

		public FlushCall(HBaseTable table, List<Row> rows) {
			this.table = table;
			this.rows = rows;
		}

		@Override
		public void run() {
			try {
				if (rows != null) {
					for (Row row : rows) {
						try {
							table.insertImpl(row, true);
						} catch (IOException ioe) {
							// should never reach this

						}
					}
					rows.clear();
					pools.add(rows);
				}

				table.flush();
			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				tableQueue.add(table);
			}
		}

	}
}
