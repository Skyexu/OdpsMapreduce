package com.legends.main;


import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import com.legends.mr.RestoreWifiTrainMap;
import com.legends.mr.RestoreWifiTrainReduce;

public class RestoreWifiTrainMain {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("append_id:string,row_id:string,user_id:string,shop_id:string,mall_id:string,time_stamp:string,date:string,hour:string,dayofweek:BIGINT,user_lon:DOUBLE,user_lat:DOUBLE"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("bssid:string,assi:DOUBLE,connect:string"));

		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName("shop_rows_in").build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName("shop_rows_out").build(), job);

		// TODO: specify a mapper
		job.setMapperClass(RestoreWifiTrainMap.class);
		// TODO: specify a reducer
		job.setReducerClass(RestoreWifiTrainReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
