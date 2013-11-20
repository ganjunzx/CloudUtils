package com.paic.hbasedemo.api.core;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.base.Preconditions;

public class HBaseUtils {
	/*
	 * (一)HTable是HBase客户端与HBase服务端通讯的Java
	 * API对象，客户端可以通过HTable对象与服务端进行CRUD操作（增删改查）。它的创建很简单： Configuration conf =
	 * HBaseConfiguration.create(); HTable table = new HTable(conf,
	 * "tablename"); HTable使用时的一些注意事项： 1. 规避HTable对象的创建开销
	 * 因为客户端创建HTable对象后，需要进行一系列的操作
	 * ：检查.META.表确认指定名称的HBase表是否存在，表是否有效等等，整个时间开销比较重，可能会耗时几秒钟之长
	 * ，因此最好在程序启动时一次性创建完成需要的HTable对象，如果使用Java API，一般来说是在构造函数中进行创建，程序启动后直接重用。 2.
	 * HTable对象不是线程安全的
	 * HTable对象对于客户端读写数据来说不是线程安全的，因此多线程时，要为每个线程单独创建复用一个HTable对象，不同对象间不要共享HTable对象使用
	 * ，特别是在客户端auto flash被置为false时，由于存在本地write buffer，可能导致数据不一致。 3.
	 * HTable对象之间共享Configuration HTable对象共享Configuration对象，这样的好处在于：
	 * 1)共享ZooKeeper的连接：每个客户端需要与ZooKeeper建立连接，查询用户的table
	 * regions位置，这些信息可以在连接建立后缓存起来共享使用；
	 * 2)共享公共的资源：客户端需要通过ZooKeeper查找-ROOT-和.META.表
	 * ，这个需要网络传输开销，客户端缓存这些公共资源后能够减少后续的网络传输开销，加快查找过程速度。 因此，与以下这种方式相比： HTable
	 * table1 = new HTable("table1"); HTable table2 = new HTable("table2");
	 * 下面的方式更有效些： Configuration conf = HBaseConfiguration.create(); HTable
	 * table1 = new HTable(conf, "table1"); HTable table2 = new HTable(conf,
	 * "table2"); 备注：即使是高负载的多线程程序，也并没有发现因为共享Configuration而导致的性能问题；如果你的实际情况中不是如此，
	 * 那么可以尝试不共享Configuration。
	 * 
	 * 
	 * (二)HTablePool可以解决HTable存在的线程不安全问题，同时通过维护固定数量的HTable对象，能够在程序运行期间复用这些HTable资源对象
	 * 。 Configuration conf = HBaseConfiguration.create(); HTablePool pool = new
	 * HTablePool(conf, 10); 1.
	 * HTablePool可以自动创建HTable对象，而且对客户端来说使用上是完全透明的，可以避免多线程间数据并发修改问题。 2.
	 * HTablePool中的HTable对象之间是公用Configuration连接的，能够可以减少网络开销。
	 */

	private static HTablePool pool = null;
	private static HBaseAdmin admin = null;
	private static Configuration conf = HBaseConfiguration.create();

	public static final String HBASE_TABLE_NAME_PREFIX = "PEAK.";
	public static final String PEAK_HBASE_COMPRESSION_TYPE = "peak.hbase.compression.type";
	public static final String PEAK_KEY_UNIQUE="peak.key.unique";
	public static final String PEAK_HBASE_DEFRRED_LOG_FLUSH = "peak.hbase.deferred.log.flush";
	public static final String TABLE_WRITE_BUFFER_SIZE = "table.write.buffer.size";
	public static final byte[] HBASE_COLUMN_FAMILY = { (byte) 'f' };
	public static final byte[] HBASE_COLUMN_QUALIFIER = {(byte)'c'};

	private static Log LOG = LogFactory.getLog(HBaseUtils.class);

	static {
		conf.addResource("hbasedemo-conf.xml");
	}

	public static Configuration getConf() {
		return conf;
	}

	public static Algorithm getHBaseCompressionType() {
		String compressType = conf.get(PEAK_HBASE_COMPRESSION_TYPE, "NONE");
		return Compression.getCompressionAlgorithmByName(compressType);
	}

	public static synchronized HTablePool getHTablePool() {
		if (pool == null) {
			pool = new HTablePool(conf, Integer.MAX_VALUE);
		}
		return pool;
	}

	public static synchronized HTableInterface getHBaseTableFromPool(
			String hbaseTableName) {
		return getHTablePool().getTable(hbaseTableName);
	}

	public static byte[][] getTableStartKeys(String tableName)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		byte[][] startKeys = table.getStartKeys();

		table.close();
		return startKeys;
	}

	public static HTableDescriptor getTableDescriptor(String tableName)
			throws IOException {
		return getHBaseAdmin().getTableDescriptor(Bytes.toBytes(tableName));
	}

	public static synchronized HBaseAdmin getHBaseAdmin() {
		try {
			if (admin == null) {
				admin = new HBaseAdmin(conf);
			}

			return admin;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getHBaseTableName(String dbName, String tableName) {
		if (tableName != null) {
			StringBuilder indexTableNameBuilder = new StringBuilder(
					HBaseUtils.HBASE_TABLE_NAME_PREFIX);

			indexTableNameBuilder.append(dbName);
			indexTableNameBuilder.append('.');
			indexTableNameBuilder.append(tableName);
			String indexTableName = new String(indexTableNameBuilder);
			return indexTableName;
		} else {
			return null;
		}
	}
	
	public static boolean isHBaseKeyUnique() {
		return conf.getBoolean(HBaseUtils.PEAK_KEY_UNIQUE, false);
	}
	
	public static boolean isHBaseDeferredLogFlush() {
		return conf.getBoolean(PEAK_HBASE_DEFRRED_LOG_FLUSH, true);
	}
}
