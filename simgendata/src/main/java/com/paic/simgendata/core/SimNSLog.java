package com.paic.simgendata.core;

import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import com.paic.utils.TimeUtils;

public class SimNSLog {
	public static Integer[] httpstatus = {200, 201, 202, 203, 204, 205, 206, 300,
			300, 301, 302, 303, 304, 305, 306, 307, 400, 401, 402, 403, 404, 405,
			406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 500, 501,
			502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 580};

	public String recordtype;// E,H,T only E now.
	public String servertype;// W
	public String runningcounter;// digit
	public String serviceid;// digit
	public String servicetype;// 1,2,3,4
	public String msisdn;
	public String ipaddress;
	public String tsstarttransaction;// time1
	public String tsendtransaction;// time2
	public String tariffclass;// 0-99
	public String uplinkwspheadersvolume;// 0-9999999999
	public String uplinkcontentvolume;// 0-9999999999
	public String downlinkwspheadersvolume;// 0-9999999999
	public String downlinkcontentvolume;// 0-9999999999
	public String successindicator;// 1,2,3
	public String empty1;
	public String empty2;
	public String clientid;// ""
	public String bearertype;// 1,2,4(4 is GPRS),only 4 now
	public String httpstatuscode;// httpstatus
	public String sessiontransactionid;// 0-9999999999
	public String wapstackmode;// 1,2,3,4,5,6
	public String wspabortreasoncode;// ""
	public String nampserveripaddress;//ip
	public String fetchedurl;//
	public String originalurl;
	public String browsertype;// terminalType
	public String pricetag;// ""
	public String empty3;
	public String empty4;
	public String piidentity;// ""
	public String ppgipaddress;// ""
	public String pushrecipientaddress;//""
	public String pushdeliverytype;//""
	public String pushid;//""
	public String pushpriority;//""
	public String pushcontenttype;//""
	public String pushapplicationidentity;//""
	public String pushdeliveryfeatures;//""
	public String pushdelayedtransmission;//""
	public String empty5;
	public String empty6;
	public String tsstartcontentfetching;//time
	public String tsendcontentfetching;//time
	public String tsackorabort;  //time
	public String imsi;//""
	public String ggsnipaddress;//"GGSN"+""0-9999"
	public String chargingid;//""
	public String sgsnipaddress;
	public String mimetyperequestoriginal;//mimeType
	public String mimetyperequestfinal;//mimeType
	public String mimetyperesponseoriginal;//mimeType
	public String mimetyperesponsefinal;//mimeType
	public String httpstatuscodeorig;// status

	//public String host; // 218.200.160.55
	//public String time; // [26/Oct/2009:00:00:00 +0800]
	//public String request; // "GET /s/ch/1000101118/1712?param=315 HTTP/1.0"
	//public String status; // 200 -- 302
	//public String afterstatus;// 10884 -- 9141 -- '-' 10361 4834 8722
	//public String tel;// "8613893705527" "8613820231449" "8613909777890"
	//public String aftertel;// 94 237 1 151 4935
	//public String terminal; // "MAUI_WAP_Browser"
	// "LG-KG70 MIC/1.1.14 MIDP-2.0/CLDC-1.1 UNTRUSTED/1.0"
	// "Jakarta Commons-HttpClient/3.1"
	// "Nokia6300/2.0 (07.21) Profile/MIDP-2.0 Configuration/CLDC-1.1"
	// "MOT-V360i/08.D5.09R MIB/2.2.1 Profile/MIDP-2.0 Configuration/CLDC-1.1"

	public String ipaddrlist; // "10.194.215.216, 211.137.59.23"
	// "10.40.9.212, 211.139.151.10"
	// "10.7.222.196, 211.139.151.74, 218.200.160.29"
	// "10.180.16.162, 211.139.60.19" "10.144.196.12, 211.142.189.202"

	public String fileName;

	// local variables
	private Random random = new Random();

	static final int[] startMsNum = {135, 136, 137, 138, 139, 155, 156, 157, 158,
			159};
	static final long srcIpBase = 0xBAB18C01;
	static final long dstIpBase = 0xDA218F01;
	static final int ipNum = 2000;
	static final String[] terminalType = {"MAUI_WAP_Browser",
			"LG-KG70 MIC/1.1.14 MIDP-2.0/CLDC-1.1 UNTRUSTED/1.0",
			"Jakarta Commons-HttpClient/3.1",
			"Nokia6300/2.0 (07.21) Profile/MIDP-2.0 Configuration/CLDC-1.1",
			"MOT-V360i/08.D5.09R MIB/2.2.1 Profile/MIDP-2.0 Configuration/CLDC-1.1",
			"HTC DIAMOND2", "HTC HD PRO", "NOKIA N97"};
	static final String[] mimeType = {"text/vnd.wap.wml",
			"application/x-www-form-urlencoded", "application/x-www-form-urlencoded",
			"image/jpeg", "application/xml", "text/plain",
			"application/octet-stream", "text/html", "application/vnd.wap.xhtml+xml",
			"application/vnd.wap.xhtml+xml"};
	static final String[] www={".3g.qq.com",".hoopchina.com",".sina.com.cn",".gmail.com",".wap.i139.cn",".icbc.com.cn",".sh.39.net",".gd.10086.cn"};
  static final String[] other={"/pams/s.do?j=l&p=1&c=5142","/MML/Images/Avatars/M3Avatar_29.JPG","/g/s?sid=AGVdplGqBRqyE9wNMWwL+w==s&aid=sendVerify","/game5/game_ad.jsp?funid=71+","/books/bookContentDetail.do?z=AwvUhmhjj4467&bookId=163361&pn=1&chapter=113","/g/s?sid=JWe83W/tsmB1FFiuW97g5g==s&tfor=3g","http_imgload.cgi?/rurl2=240fc5bf79073275a4527512eb967a2b6e9728ef3b1d6e94fa7e0dd2a0988ea08804120c40d0305cc29199fe29961168e60be301d2486275f8551fdc7eec03b9739dfb06a23fb283af91560784cef419483090f1","/g/s?sid=AIhNnAvDNDKCXhizJtFA4g==s&aid=nqqchatMain&on=1","/g/s?sid=1xqdV2WS6SH0ffdPYw4wFA==s&aid=nqqchatMain&on=1"};
	static final String[] oriurl={"http://10.0.0.172:80/HttpSrv1.3/httpInterface/readmessage.aspx","NokiaN73-1/3.0705.1.0.31 Series60/3.0 Profile/MIDP-2.0 Configuration/CLDC-1.1","LENOVO-P708/(2005.07.06)S270/WAP1.2.1 Profile","THL-02A/L6A5502.1.5/WAP2.0 Profile","MOT-ROKR E2/1.0 R564_G_12.01.46P/12.28.2005 Mozilla/4.0 (compatible; MSIE 6.0; Linux; Motorola ROKR E2; 2445) Profile/MIDP-2.0 Configuration/CLDC-1.1 Ope","ra 8.50 [zh-cn]","http://wap.3g.QQ.com/?t=92537"};
  
	public static String[] makeFileName(Date dateTime, int nodeId,int GWnums) {
		Random random = new Random();
		String[] result=new String[GWnums];
		//String GWName = GwNameNum[random.nextInt(GwNameNum.length)];
		for(int i=0;i<GWnums;i++){
			result[i]=String.format("FJFZ-PS-WAP-GW%03d-%06d-%s", i+1, nodeId, TimeUtils
					.formatDateTime(dateTime, "yyyyMMddHHmmssSSS"));
		}
		
		//return String.format("FJFZ-PS-WAP-%s-%06d-%s", GWName, nodeId, TimeUtils
		//		.formatDateTime(dateTime, "yyyyMMddHHmmssSSS"));
		return result;
	}

	private static String long2ip(long ip) {
		int[] b = new int[4];
		b[0] = (int) ((ip >> 24) & 0xff);
		b[1] = (int) ((ip >> 16) & 0xff);
		b[2] = (int) ((ip >> 8) & 0xff);
		b[3] = (int) (ip & 0xff);
		String x;
		/*
		 * CodeReview - Verified - low no need to construct Integer object here. use
		 * Integer.toString(b[]) Found by guoleitao, 20100210. Fixed by
		 * chanceli,20100227 Verified by guoleitao, 20100305
		 */
		// Integer p;
		// p = new Integer(0);
		 x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) +
		 "." + Integer.toString(b[3]);
		//x = Integer.toString(b[0]) + Integer.toString(b[1])
		//		+ Integer.toString(b[2]) + Integer.toString(b[3]);
		return x;
	}

	public String genipaddr() {
		long sourceip = srcIpBase + random.nextInt(ipNum);
		long destip = dstIpBase + random.nextInt(ipNum);
		return long2ip((random.nextInt(10) > 5) ? sourceip : destip);
	}

	/**
	 * generate next accesslog record
	 * 
	 * @param ts
	 * @param gdrFileName
	 * @param fixMsPerfix
	 * @param fixMsRange
	 * @throws Exception
	 */
	public void next(long ts, String inFileName, int fixMsPerfix, int fixMsRange,
			Configuration simConf) throws Exception {
		
		java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS", Locale.US);
		int _interval =Integer.parseInt(simConf.get("huge.src.nokiasimens.gdrinterval","300").trim());

		//Date date = new Date(ts+((long)random.nextInt(300))*1000);

		
		
		int msPerfix = fixMsPerfix;
		if (msPerfix <= 0) {
			msPerfix = startMsNum[random.nextInt(startMsNum.length)];
		}

		int cityId = fixMsRange;
		if (cityId <= 0) {
			cityId = random.nextInt(30);
		}
		
		recordtype  =  "E";             
		servertype  ="W";               
		runningcounter   = Integer.toString(random.nextInt(2147483647));          
		serviceid      = Integer.toString(random.nextInt(2147483647));              
		servicetype       = Integer.toString(random.nextInt(4)+1);           
		msisdn  = "86"+ Long.toString(msPerfix * 100000000L + cityId * 100000
					+ random.nextInt(100000));                
		ipaddress =    genipaddr();           
		tsstarttransaction = df.format(new Date(ts+((long)random.nextInt(_interval))*1000));         
		tsendtransaction    =    df.format(new Date(ts+((long)random.nextInt(_interval))*1000));   
		tariffclass      = Integer.toString(random.nextInt(100));           
		uplinkwspheadersvolume   = Integer.toString(random.nextInt(2147483647)); 
		uplinkcontentvolume    = Integer.toString(random.nextInt(2147483647));      
		downlinkwspheadersvolume    = Integer.toString(random.nextInt(2147483647)); 
		downlinkcontentvolume     = Integer.toString(random.nextInt(2147483647));   
		successindicator      = Integer.toString(random.nextInt(3)+1);       
		empty1     ="";                
		empty2      ="";               
		clientid    ="";               
		bearertype   ="4";              
		httpstatuscode    =   Integer.toString(httpstatus[random.nextInt(httpstatus.length)]);      
		sessiontransactionid  = Integer.toString(random.nextInt(2147483647));     
		wapstackmode        =Integer.toString(random.nextInt(6)+1);        
		wspabortreasoncode     ="";    
		nampserveripaddress  =   genipaddr();     
		fetchedurl        ="http://"+"q"+Integer.toString(random.nextInt(100)+1)+www[random.nextInt(www.length)]+other[random.nextInt(other.length)];         
		originalurl       =oriurl[random.nextInt(oriurl.length)];         
		browsertype       =terminalType[random.nextInt(terminalType.length)];         
		pricetag    ="";               
		empty3      ="";               
		empty4     ="";                
		piidentity  ="";               
		ppgipaddress   ="";            
		pushrecipientaddress     ="";  
		pushdeliverytype     ="";      
		pushid              ="";       
		pushpriority         ="";      
		pushcontenttype       ="";     
		pushapplicationidentity    ="";
		pushdeliveryfeatures      =""; 
		pushdelayedtransmission    ="";
		empty5                 ="";    
		empty6                 ="";    
		tsstartcontentfetching  =df.format(new Date(ts+((long)random.nextInt(_interval))*1000));      
		tsendcontentfetching   =df.format(new Date(ts+((long)random.nextInt(_interval))*1000));       
		tsackorabort           =df.format(new Date(ts+((long)random.nextInt(_interval))*1000));       
		imsi       ="";                
		ggsnipaddress     ="GGSN"+  Integer.toString(random.nextInt(8999)+1000);       
		chargingid      ="";           
		sgsnipaddress    ="";          
		mimetyperequestoriginal =  mimeType[random.nextInt(mimeType.length)]; 
		mimetyperequestfinal   =  mimeType[random.nextInt(mimeType.length)];   
		mimetyperesponseoriginal =  mimeType[random.nextInt(mimeType.length)]; 
		mimetyperesponsefinal   =   mimeType[random.nextInt(mimeType.length)]; 
		httpstatuscodeorig    =   Integer.toString(httpstatus[random.nextInt(httpstatus.length)]);  


		fileName = inFileName;
	}

	public String buildTextLine(String delimiter) {
		StringBuilder gdrBuilder = new StringBuilder();
		gdrBuilder.append(recordtype)                ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(servertype)                ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(runningcounter)            ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(serviceid)                 ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(servicetype)               ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(msisdn)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(ipaddress)                 ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(tsstarttransaction)        ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(tsendtransaction)          ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(tariffclass)               ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(uplinkwspheadersvolume)    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(uplinkcontentvolume)       ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(downlinkwspheadersvolume)  ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(downlinkcontentvolume)     ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(successindicator)          ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(empty1)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(empty2)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(clientid)                  ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(bearertype)                ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(httpstatuscode)            ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(sessiontransactionid)      ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(wapstackmode)              ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(wspabortreasoncode)        ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(nampserveripaddress)       ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(fetchedurl)                ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(originalurl)               ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(browsertype)               ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pricetag)                  ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(empty3)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(empty4)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(piidentity)                ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(ppgipaddress)              ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushrecipientaddress)      ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushdeliverytype)          ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushid)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushpriority)              ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushcontenttype)           ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushapplicationidentity)   ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushdeliveryfeatures)      ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(pushdelayedtransmission)   ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(empty5)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(empty6)                    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(tsstartcontentfetching)    ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(tsendcontentfetching)      ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(tsackorabort)              ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(imsi)                      ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(ggsnipaddress)             ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(chargingid)                ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(sgsnipaddress)             ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(mimetyperequestoriginal)   ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(mimetyperequestfinal)      ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(mimetyperesponseoriginal)  ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(mimetyperesponsefinal)     ;
		gdrBuilder.append(delimiter)                 ;
		gdrBuilder.append(httpstatuscodeorig)        ;

		gdrBuilder.append("\n");
		return gdrBuilder.toString();
	}

	void printValue() {
	}
}
