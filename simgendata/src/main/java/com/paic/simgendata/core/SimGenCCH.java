package com.paic.simgendata.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

import junit.framework.Test;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.glassfish.grizzly.filterchain.NextAction;

public class SimGenCCH {
	static final int[] startMsNum = { 135, 136, 137, 138, 139, 155, 156, 157,
			158, 159 };
	static final int[] startIDNum = { 510222, 510223, 510224, 510227, 510228,
			510230, 510231, 510232, 510281, 510282, 510283, 510301, 510302,
			510303, 510304, 510311, 510321, 510322, 510401, 510402 };// 20
	private final Random random = new Random();
	private final int[] numRandom = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	private final String[] firstName = { "王", "李", "张", "刘", "陈", "杨", "赵",
			"黄", "周", "吴", "徐", "孙", "胡", "朱", "高", "林", "何", "郭", "马", "罗",
			"梁", "宋", "郑", "谢", "韩", "唐", "冯", "于", "董", "萧", "程", "柴", "袁",
			"邓", "许", "傅", "沈", "曾", "彭", "吕", "苏", "卢", "蒋", "蔡", "贾", "丁",
			"魏", "薛", "叶", "阎", "余", "潘", "杜", "戴", "夏", "钟", "汪", "田", "任",
			"姜", "范", "方", "石", "姚", "谭" };// 65
	private final String[] secondName = { "的", "一", "是", "在", "不", "了", "有",
			"和", "人", "这", "中", "大", "为", "上", "个", "国", "我", "以", "要", "他",
			"时", "来", "用", "们", "生", "到", "作", "地", "于", "出", "就", "分", "对",
			"成", "会", "可", "主", "发", "年", "动", "同", "工", "也", "能", "下", "过",
			"子", "说", "产", "种", "面", "而", "方", "后", "多", "定", "行", "学", "法",
			"所", "民", "得", "经", "十", "三", "之", "进着", "等", "部", "度", "家", "电",
			"力", "里", "如", "水", "化", "高", "自", "二", "理", "起", "小", "物", "现",
			"实", "加", "量", "都", "两", "体", "制", "机", "当", "使", "点", "从", "业",
			"本", "去", "把", "性", "好", "应", "开", "它", "合", "还", "因", "由", "其",
			"些", "然", "前", "外", "天", "政", "四", "日", "那", "社", "义", "事", "平",
			"形", "相", "全", "表", "间", "样", "与", "关", "各", "重", "新", "线", "内",
			"数", "正", "心", "反", "你", "明", "看", "原", "又", "么", "利", "比", "或",
			"但", "质", "气", "第", "向", "道", "命", "此", "变", "条", "只", "没", "结",
			"解", "问", "意", "建", "月", "公", "无", "系", "军", "很", "情", "者", "最",
			"立", "代", "想", "已", "通", "并", "提", "直", "题", "党", "程", "展", "五",
			"果", "料", "象", "员", "革", "位", "入", "常", "文", "总", "次", "品", "式",
			"活", "设", "及", "管", "特", "件", "长", "求", "老", "头", "基", "资", "边",
			"流", "路", "级", "少", "图", "山", "统", "接", "知", "较", "将", "组", "见",
			"计", "别", "她", "手", "角", "期", "根", "论", "运", "农", "指", "几", "九",
			"区", "强", "放", "决", "西", "被", "干", "做", "必", "战", "先", "回", "则",
			"任", "取", "据", "处", "队", "南", "给", "色", "光", "门", "即", "保", "治",
			"北", "造", "百", "规", "热", "领", "七", "海", "口", "东", "导", "器", "压",
			"志", "世", "金", "增", "争", "济", "阶", "油", "思", "术", "极", "交", "受",
			"联", "什", "认", "六", "共", "权", "收", "证", "改", "清", "己", "美", "再",
			"采", "转", "更", "单", "风", "切", "打", "白", "教", "速", "花", "带", "安",
			"场", "身", "车", "例", "真", "务", "具", "万", "每", "目", "至", "达", "走",
			"积", "示", "议", "声", "报", "斗", "完", "类", "八", "离", "华", "名", "确",
			"才", "科", "张", "信", "马", "节", "话", "米", "整", "空", "元", "况", "今",
			"集", "温", "传", "土", "许", "步", "群", "广", "石", "记", "需", "段", "研",
			"界", "拉", "林", "律", "叫", "且", "究", "观", "越", "织", "装", "影", "算",
			"低", "持", "音", "众", "书", "布", "复", "容", "儿", "须", "际", "商", "非",
			"验", "连", "断", "深", "难", "近", "矿", "千", "周", "委", "素", "技", "备",
			"半", "办", "青", "省", "列", "习", "响", "约", "支", "般", "史", "感", "劳",
			"便", "团", "往", "酸", "历", "市", "克", "何", "除", "消", "构", "府", "称",
			"太", "准", "精", "值", "号", "率", "族", "维", "划", "选", "标", "写", "存",
			"候", "毛", "亲", "快", "效", "斯", "院", "查", "江", "型", "眼", "王", "按",
			"格", "养", "易", "置", "派", "层", "片", "始", "却", "专", "状", "育", "厂",
			"京", "识", "适", "属", "圆", "包", "火", "住", "调", "满", "县", "局", "照",
			"参", "红", "细", "引", "听", "该", "铁", "价", "严", "龙", "飞" };

	private final char[] english = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
			'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
			'v', 'w', 'x', 'y', 'z' };

	private String next() {
		String partyNo = "" + numRandom[random.nextInt(10)];
		for (int i = 0; i < 11; ++i) {
			partyNo += numRandom[random.nextInt(10)];
		}
		String chineseName = (random.nextInt(2) == 0) ? firstName[random
				.nextInt(65)] + secondName[random.nextInt(65)]
				: firstName[random.nextInt(65)]
						+ secondName[random.nextInt(65)]
						+ secondName[random.nextInt(65)];
		String englishName = (random.nextInt(2) == 0) ? ""
				: ("" + english[random.nextInt(26)]
						+ english[random.nextInt(26)]
						+ english[random.nextInt(26)]
						+ english[random.nextInt(26)]
						+ english[random.nextInt(26)]
						+ english[random.nextInt(26)] + english[random
						.nextInt(26)]);
		int idType = 1;
		String idNum = "" + startIDNum[random.nextInt(20)]
				+ (1950 + random.nextInt(64)) + (random.nextInt(12) + 1);
		int date = random.nextInt(31);
		if (date < 10) {
			idNum += 0;
			idNum += date;
		} else {
			idNum += date;
		}

		for (int i = 0; i < 4; i++) {
			idNum += numRandom[random.nextInt(10)];
		}
		int sex = random.nextInt(2);
		java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS", Locale.US);
		long curTimestamp = System.currentTimeMillis();
		String LCD = df.format(new Date(curTimestamp - random.nextInt(1000)
				+ ((long) random.nextInt(300)) * 1000));
		String LCU = chineseName;

		return partyNo + "," + chineseName + "," + englishName + "," + idType
				+ "," + idNum + "," + sex + "," + "" + "," + ""
				+ "," + "" + "," + "" + "," + "" + "," + "" + "," + "" + ","
				+ "" + "," + "" + "," + "" + "," + "" + "," + "" + "," + ""
				+ "," + "" + "," + "" + "," + LCD + "," + LCU + "" + "," + ""
				+ "," + "" + "," + "" + "," + "" + "," + "" + "," + "" + ","
				+ "" + "," + "" + "," + "" + "," + "" + "," + "" + "," + ""
				+ "," + "" + "," + "" + "," + "" + "," + "" + "," + "" + ","
				+ "" + "," + "";
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			printHelp();
			System.exit(1);
		}

		SimGenCCH simGenCCH = new SimGenCCH();
		long numRecord = Long.parseLong(args[0]);
		long fileRecordNum = Long.parseLong(args[1]);
		String dirString = args[2];
		long currentRecordNum = 0;
		long tempRecordNum = 0;
		long files = 0;
		String record = "";
		BufferedWriter reader;
		try {
			reader = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(dirString + "/" + "test"
							+ files)), "UTF-8"));
			System.out.println("Will generate " + numRecord + " records");
			for (int i = 0; i < numRecord; i++) {
				if (tempRecordNum == fileRecordNum) {
					System.out.println("generate "
							+ (dirString + "/" + "test" + (files)) + ": "
							+ currentRecordNum + " record!");
					tempRecordNum = 0;
					files++;
					reader.close();
					reader = null;
					reader = new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(new File(dirString + "/"
									+ "test" + files)), "UTF-8"));
				}
				tempRecordNum++;
				currentRecordNum++;
				record = simGenCCH.next();
				if (i != (numRecord-1)) {
					reader.write(record + "\n");
				}
				reader.write(record);
			}
			reader.close();
			reader = null;
			System.out.println("generate "
					+ (dirString + "/" + "test" + (files)) + ": "
					+ currentRecordNum + " record!");
			System.out.println("finished generate " + numRecord + " records");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void printHelp() {
		System.out.println("./simigendatachh nums fileRecordNum dir");
	}

}
