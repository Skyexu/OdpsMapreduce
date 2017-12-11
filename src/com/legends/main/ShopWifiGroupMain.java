package com.legends.main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.legends.mr.ShopWifiGroupMap;
import com.legends.mr.ShopWifiGroupReduce;

public class ShopWifiGroupMain {

	public static void main(String[] args) throws OdpsException{
		// TODO Auto-generated method stub
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("shop_id:String"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("bssid:String"));

		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName("shop_wifi_group_in").build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName("shop_wifi_group_out").build(), job);

		// TODO: specify a mapper
		job.setMapperClass(ShopWifiGroupMap.class);
		// TODO: specify a reducer
		job.setReducerClass(ShopWifiGroupReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
