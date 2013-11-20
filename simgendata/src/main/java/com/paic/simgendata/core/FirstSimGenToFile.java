package com.paic.simgendata.core;

/*
 * 模拟生成mmsclog数据，为其它程序作数据准备
 */

import java.io.FileReader;
import java.net.InetAddress;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class FirstSimGenToFile {

	public Configuration simConf;
	private int nodeId = 0;
	public FileSystem fs = null;
	int fileNum = 0;

	static final int[] startMsNum = {135, 136, 137, 138, 139, 155, 156, 157, 158,
			159};
	private final Random random = new Random();

	public static void printUsage() {
		System.out
				.println("arguements: <outDir> <fileRows> <msPerfix> <msRange> <rowsPerSec> <grain> <dispRows>");
		System.out
				.println("simigendata gengdr /gdr 400000 0 0 50000 10000 400000");
		System.out.println("simigendata gengdr /gdr 100 0 0 100 100 100\n");
	}

	public int run(String[] args) throws Exception {
		String hostName = InetAddress.getLocalHost().getHostName();
		this.nodeId = Integer.parseInt(simConf.get(hostName, "0"));

		String outDir = args[0];
		long fileRows = Long.parseLong(args[1]);
		int msPerfix = Integer.parseInt(args[2]);
		int msRange = Integer.parseInt(args[3]);
		int rowsPerSec = Integer.parseInt(args[4]);
		int interRows = Integer.parseInt(args[5]);
		int interMillis = (1000 * interRows) / rowsPerSec + 1;
		int dispRows = Integer.parseInt(args[6]);

		boolean blobflag=false;
		if (args.length > 7) {
      blobflag=(args[7].equalsIgnoreCase("blob"));
    }
		simConf.set("gdr.src.tmp.blob",String.valueOf(blobflag));

		if ((msPerfix <= 0) || (msPerfix > startMsNum.length)) {
			msPerfix = 0; // invalid
		}
		int curMsPerfix = msPerfix;

		if ((msRange <= 0) || (msRange > 1000)) {
			msRange = 0;
		}
		int curMsRange = msRange;

		Configuration hadoopConf = new Configuration();
		if (null == this.fs) {
      this.fs = FileSystem.get(hadoopConf);
    }

		SimGnGdr simGdr = new SimGnGdr();
		Date dateTime = new Date();

		String gdrFileName = SimGnGdr.makeFileName(dateTime, nodeId);
		Path curPathName = new Path(outDir, gdrFileName);
		Path importPath = new Path(outDir, "import");

		if (!this.fs.mkdirs(importPath)) {
			throw new Exception("mkdir dir fail");
		}

		FSDataOutputStream fileOut = null;
		long genGdrRows = 0;

		long beginTimestamp = System.currentTimeMillis();
		long curTimestamp = beginTimestamp;
		long chkSpeedTs1 = beginTimestamp;
		long chkSpeedTs2 = beginTimestamp;

		while (true) {
			curTimestamp = System.currentTimeMillis();
			dateTime.setTime(curTimestamp);

			if (genGdrRows == 0) {
				// start a new file
				gdrFileName = SimGnGdr.makeFileName(dateTime, nodeId);
				curPathName = new Path(outDir, gdrFileName);
				fileOut = fs.create(curPathName);
			}

			if (genGdrRows >= fileRows) {
				if (fileOut != null) {
					fileOut.close(); // close current file
					fs.rename(curPathName, new Path(importPath, gdrFileName));
					fileNum++;
					fileOut = null;

					break;
				}
			}

			if ((msPerfix > 0) && (msPerfix <= startMsNum.length)) {
				curMsPerfix = startMsNum[random.nextInt(msPerfix)];
			}

			simGdr.next(curTimestamp - random.nextInt(1000), gdrFileName,
					curMsPerfix, curMsRange, simConf);
			String gdrTextLine = simGdr.buildTextLine(",");
			/*
			 * CodeReview - Fixed - High possible null pointer dereference of fileOut.
			 * Found by guoleitao, 20100210. Reopen by guoleitao, 20100305. if null,
			 * init all here.
			 */
			if (fileOut == null) {
				genGdrRows = 0;
				gdrFileName = SimGnGdr.makeFileName(dateTime, nodeId);
				curPathName = new Path(outDir, gdrFileName);
				fileOut = fs.create(curPathName);
			}
			fileOut.writeBytes(gdrTextLine);
			genGdrRows++;

			if (0 == (genGdrRows % dispRows)) {
				long runTime = (curTimestamp > beginTimestamp)
						? (curTimestamp - beginTimestamp)
						: 1;
				String row = Long.toString(simGdr.msisdn) + "@" + simGdr.start_time;
				System.out.println(genGdrRows + " latest row= " + row + " rows/s= "
						+ (long) (((double) genGdrRows / (double) runTime) * 1000));
			}

			// control speed
			if (0 == (genGdrRows % interRows)) {
				chkSpeedTs2 = System.currentTimeMillis();
				long deltaMillis = chkSpeedTs2 - chkSpeedTs1;
				if (deltaMillis < interMillis) {
					long sleepLen = Math.abs(random.nextLong())
							% (long) ((interMillis - deltaMillis) * 1.85);
					// long sleepLen = interMillis - deltaMillis;
					Thread.sleep(sleepLen);
				}
				chkSpeedTs1 = System.currentTimeMillis();
			}
		}

		return fileNum;
	}

	public static void main(String[] args) {
		if (args.length < 7) {
			printUsage();
			System.exit(-1);
		}

		FirstSimGenToFile gdrGenerator = new FirstSimGenToFile();
		gdrGenerator.simConf = SimGenDataConf.getConf();
		while (true) {
			try {
				boolean runFlag = Boolean.parseBoolean(gdrGenerator.simConf
						.get("huge.etl.system.run", "true"));
				int maxfilenum = Integer.parseInt(gdrGenerator.simConf
						.get("huge.src.gen.files.maxnum", "100"));
				if (runFlag == false) {
					if (gdrGenerator.fs != null) {
            gdrGenerator.fs.close();
          }
					System.out.println("SimGenToFile Exit Gracefully!");
					return;
				}

				if (gdrGenerator.run(args) >= maxfilenum) {
					System.out.println("done! " + maxfilenum + " generated!");
					break;
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				System.exit(-1);
			}
		}
	}
}

