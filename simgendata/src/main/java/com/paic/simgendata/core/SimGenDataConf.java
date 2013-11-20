package com.paic.simgendata.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class SimGenDataConf {

	private static Configuration conf = new Configuration();
	private static Log LOG = LogFactory.getLog(SimGenDataConf.class);
	
	static {
		conf.addResource("simgendata-conf.xml");
	}
	
	public static Configuration getConf() {
		return conf;
	}
	
}
