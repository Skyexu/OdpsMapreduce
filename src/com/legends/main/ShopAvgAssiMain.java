package com.legends.main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.legends.mr.ShopAvgAssiMap;
import com.legends.mr.ShopAvgAssiReduce;

public class ShopAvgAssiMain {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("append_id:string,row_id:string,user_id:string,shop_id:string,mall_id:string,time_stamp:string,date:string,hour:string,dayofweek:string,user_lon:double,user_lat:double,bssid:string,strength:double,connect:string,bssid_appear_num:bigint,assi:double"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("shop_id:string,assi:double"));

		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName("shop_assi_in").build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName("shop_assi_out").build(), job);

		// TODO: specify a mapper
		job.setMapperClass(ShopAvgAssiMap.class);
		// TODO: specify a reducer
		job.setReducerClass(ShopAvgAssiReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
