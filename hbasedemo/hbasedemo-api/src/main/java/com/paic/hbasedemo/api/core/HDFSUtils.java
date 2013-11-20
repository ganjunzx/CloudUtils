package com.paic.hbasedemo.api.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HADOOP HDFS 工具类
 * 
 * @author ganjun
 * 
 */
public class HDFSUtils {
	private static Configuration conf = HBaseConfiguration.create();
	static {
		conf.addResource("hbasedemo-conf.xml");
	}

	public static Configuration getConf() {
		return conf;
	}

	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static boolean deleteHdfsFile(String path) throws IOException {
		// 获取要删除的文件
		Path delefPath = new Path(path);
		FileSystem hdfs = FileSystem.get(conf);
		boolean isDeleted = false;

		// 检查文件是否存在，若存在，递归删除
		if (hdfs.exists(delefPath)) {
			isDeleted = hdfs.delete(delefPath, true);
			// 递归删除
		} else {
			isDeleted = false;
		}
		return isDeleted;
	}

	/**
	 * 将本地文件上传至HDFS
	 * 
	 * @param local
	 *            本地路径
	 * @param hdfs
	 *            hdfs路径
	 * @throws IOException
	 *             IO异常
	 */
	public static void uploadToHdfs(String local, String hdfs)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(URI.create(hdfs), conf);

		/*
		 * // 读取本地文件 FileInputStream fis = new FileInputStream(new File(local));
		 * OutputStream os = fs.create(new Path(hdfs)); // 复制
		 * IOUtils.copyBytes(fis, os, 4096, true);
		 * 
		 * os.close(); fis.close();
		 */
		Path src = new Path(local);
		Path dst = new Path(hdfs);
		fs.copyFromLocalFile(src, dst);
	}

	/**
	 * 读取HDFS文件
	 * 
	 * @param fileName
	 *            源文件路径
	 * @param dest
	 *            写入文件路径
	 * @throws IOException
	 */
	public static void readFromHdfs(String fileName, String dest)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		// 打开文件流
		FSDataInputStream hdfsInStream = fs.open(new Path(fileName));
		// 写入本地文件系统
		OutputStream out = new FileOutputStream(dest);
		byte[] ioBuffer = new byte[1024];
		// 按行读取
		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {
			out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		out.close();
		hdfsInStream.close();
		fs.close();
	}
	
	public static FSDataInputStream openReadHdfsFile(String path)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		Path dfsPath=new Path(path);
		if (!fs.exists(dfsPath)) {
			return null;
		} else {
			FSDataInputStream hdfsInStream = fs.open(dfsPath);
			return hdfsInStream;
		}
		// 打开文件流
	}
	
	public static long getLength(String path)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		Path dfsPath=new Path(path);
		if (!fs.exists(dfsPath)) {
			return 0L;
		} else {
			ContentSummary contentSummary = fs.getContentSummary(dfsPath);
			return contentSummary.getLength();
		}
		// 打开文件流
	}
	
	public static long getFileCount(String path)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		Path dfsPath=new Path(path);
		if (!fs.exists(dfsPath)) {
			return 0L;
		} else {
			ContentSummary contentSummary = fs.getContentSummary(dfsPath);
			return contentSummary.getFileCount();
		}
		// 打开文件流
	}
	
	public static <T> T newInstance(String className)
			throws IOException {
		// 获取HDFS文件系统
		Class srcClass;
		try {
			srcClass = Class.forName(className);
			if (srcClass != null) {
				return (T) ReflectionUtils.newInstance(srcClass, conf);
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static FSDataOutputStream openWriteHdfsFile(String path)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		Path dfsPath=new Path(path);
		if (!fs.exists(dfsPath)) {
			FSDataOutputStream hdfsOutStream = fs.create(dfsPath);
			return hdfsOutStream;
		} else {
			return null;
		}
	}

	/**
	 * 列出HDFS目录
	 * 
	 * @param path
	 *            路径
	 * @throws IOException
	 */
	public static Map<String, Long> lsdir(String path)
			throws IOException {
		Map<String, Long> pathInfo = new HashMap<String, Long>();
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		Path dfsPath=new Path(path);
		if (!fs.exists(dfsPath)) {
			return pathInfo;
		} else {
			// 获取指定路径下的文件
			FileStatus fileList[] = fs.listStatus(dfsPath);
			int size = fileList.length;
			// 循环输出文件
			for (int i = 0; i < size; i++) {
				pathInfo.put(fileList[i].getPath().getName(), fileList[i].getLen());
			}
			fs.close();
			return pathInfo;
		}
		
	}
	
	public static boolean mkdir(String path)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		Path dfsPath=new Path(path);
		if (!fs.exists(dfsPath)) {
			fs.close();
			return false;
		} else {
			fs.close();
			return fs.mkdirs(dfsPath);
		}
	}
	
	public static FileSystem getFileSystem()
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		return fs;
	}
	
	public static boolean exists(String strPath)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		boolean isExist = fs.exists(new Path(strPath));
		fs.close();
		return isExist;
	}
	
	public static boolean isDirectory(String strPath)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		boolean isExist = fs.isDirectory(new Path(strPath));
		fs.close();
		return isExist;
	}
	
	public static boolean isFile(String strPath)
			throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(conf);
		FileStatus fileStatus = fs.getFileStatus(new Path(strPath));
		boolean isFile = fileStatus.isFile();
		fs.close();
		return isFile;
	}
	
	public void main(String[] args) {
		try {
			System.out.println("================== delete \'/test\' start =========================");
			HDFSUtils.deleteHdfsFile("/test");
			System.out.println("================== delete \'/test\' end   =========================");
			
			System.out.println("================== mkdir \'/test\' start =========================");
			HDFSUtils.mkdir("/test");
			System.out.println("================== mkdir \'/test\' end   =========================");
			
			System.out.println("================== write \'/test/file1\' start =========================");
			FSDataOutputStream fout = HDFSUtils.openWriteHdfsFile("/test/file1");
			BufferedWriter out = null;
			out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			out.write("file1");
			out.close();
			System.out.println("================== write \'/test/file1\' end   =========================");
			
			System.out.println("================== read \'/test/file1\' start =========================");
			FSDataInputStream fin = HDFSUtils.openReadHdfsFile("/test/file1");
			BufferedReader in = null;
			in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			String strLine = in.readLine();
			System.out.println("\n" + strLine + "\n");
			out.close();
			System.out.println("================== read \'/test/file1\' end   =========================");
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
