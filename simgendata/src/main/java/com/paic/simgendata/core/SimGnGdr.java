package com.paic.simgendata.core;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import com.paic.utils.TimeUtils;

public class SimGnGdr {
	public long timeStamp;

	// GDR data fields
	public long start_time;
	public String imsi;
	public long msisdn;
	public int sourceip;
	public int destip;
	public String sourceipblob;
	public String destipblob;
	public int gdrtype;
	public int req_num;
	public String apn;
	public int gdrtime;
	public int result;
	public int gtpver;
	public int remoteno;
	public int frontno;
	public int offset;
	public String fileName;
	public boolean blobflag;

	// local variables
	private Random random = new Random();

	static final int[] startMsNum = {135, 136, 137, 138, 139, 155, 156, 157, 158,
			159};

	static final int srcIpBase = 0xDDB18E01; // 221.177.142.1 ~ 200
	static final int dstIpBase = 0xDDB18F01; // 221.177.143.1 ~ 200
	static final int ipNum = 200;

	static final String[] apnNames = {"cmwap", "cmnet"};

	public static String makeFileName(Date dateTime, int nodeId) {
		return String.format("GPRS_GNGDR%s.%06d.%s",
				TimeUtils.formatDateTime(dateTime, "yyMMddHH"), nodeId,
				Long.toHexString(dateTime.getTime()));
	}

	/**
	 * generate next GNGDR record
	 * 
	 * @param ts
	 * @param gdrFileName
	 * @param fixMsPerfix
	 * @param fixMsRange
	 * @throws Exception
	 */
	public void next(long ts, String inFileName, int fixMsPerfix, int fixMsRange,
			Configuration simConf) throws Exception {
		boolean iscurrenttime = Boolean.parseBoolean(simConf.get(
				"huge.src.record.timestamp.iscurrent", "true"));
		if (iscurrenttime) {

			start_time = ts;

		} else {

			String beginTime = simConf.get(
					"huge.src.record.timestamp.begin").trim();
			String endTime = simConf.get(
					"buge.src.record.timestamp.end").trim();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			// String timemm = sdf.format(ts);
			// long lg = sdf.parse(timemm).getTime();
			// long ra=((long)random.nextInt(28 * 24 * 3600)) * 1000;
			long begintime = sdf.parse(beginTime).getTime();
			long endtime = sdf.parse(endTime).getTime();
			long ra = (long) random.nextInt((int) ((endtime - begintime) / 1000));

			start_time = begintime + ra * 1000;
			// System.out.println("==================333::"+begintime+":"+endtime+":"+ra*1000+"::"+start_time);
		}

		int msPerfix = fixMsPerfix;
		if (msPerfix <= 0) {
			msPerfix = startMsNum[random.nextInt(startMsNum.length)];
		}

		int cityId = fixMsRange;
		if (cityId <= 0) {
			cityId = random.nextInt(30);
		}

		msisdn = msPerfix * 100000000L + cityId * 100000 + random.nextInt(100000);
		imsi = Long.toString(msisdn) + "0000000";

		blobflag = Boolean.parseBoolean(simConf.get(
				"gdr.src.tmp.blob", "false"));

		sourceip = srcIpBase + random.nextInt(ipNum);
		destip = dstIpBase + random.nextInt(ipNum);

		sourceipblob = Base64.encodeBytes(Bytes.toBytes(sourceip));
		destipblob = Base64.encodeBytes(Bytes.toBytes(destip));

		gdrtype = random.nextInt(10) + 1;
		req_num = random.nextInt(10) + 1;

		apn = apnNames[random.nextInt(apnNames.length)];

		gdrtime = random.nextInt(10000);
		result = random.nextInt(2);

		gtpver = random.nextInt(100) + 1;
		remoteno = random.nextInt(1000) + 1;
		frontno = random.nextInt(1000) + 1;
		offset = random.nextInt(1000) + 1;

		fileName = inFileName;
	}

	public String buildTextLine(String delimiter) {
		StringBuilder gdrBuilder = new StringBuilder();

		gdrBuilder.append(start_time);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(imsi);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(msisdn);
		gdrBuilder.append(delimiter);

		if (blobflag) {
			gdrBuilder.append(sourceipblob);
			gdrBuilder.append(delimiter);
			gdrBuilder.append(destipblob);

		} else {
			gdrBuilder.append(sourceip);
			gdrBuilder.append(delimiter);
			gdrBuilder.append(destip);
		}
		gdrBuilder.append(delimiter);
		gdrBuilder.append(gdrtype);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(req_num);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(apn);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(gdrtime);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(result);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(gtpver);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(remoteno);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(frontno);
		gdrBuilder.append(delimiter);
		gdrBuilder.append(offset);

		gdrBuilder.append("\n");
		return gdrBuilder.toString();
	}

	public long getTimestamp() {
		return timeStamp;
	}

	void printValue() {
		System.out.println("start_time=" + start_time);
		System.out.println("imsi=" + imsi);
		System.out.println("msisdn=" + msisdn);
		System.out.println("sourceip=" + sourceip);
		System.out.println("destip=" + destip);
		System.out.println("gdrtype=" + gdrtype);
		System.out.println("req_num=" + req_num);
		System.out.println("apn=" + apn);
		System.out.println("gdrtime=" + gdrtime);
		System.out.println("result=" + result);
		System.out.println("gtpver=" + gtpver);
		System.out.println("remoteno=" + remoteno);
		System.out.println("frontno=" + frontno);
		System.out.println("offset=" + offset);
		System.out.println("fileName=" + fileName);
	}
}