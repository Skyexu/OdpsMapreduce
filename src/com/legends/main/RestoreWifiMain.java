package com.legends.main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.legends.mr.RestoreWifiMap;
import com.legends.mr.RestoreWifiReduce;

public class RestoreWifiMain {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("rowid:String,user_id:String,shop_id:String,time_stamp:String,user_lon:double,user_lat:double"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("bssid:String,strength:Double,connect:String"));

		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName("shop_behavior_out").build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName("shop_restore").build(), job);

		// TODO: specify a mapper
		job.setMapperClass(RestoreWifiMap.class);
		// TODO: specify a reducer
		job.setReducerClass(RestoreWifiReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
