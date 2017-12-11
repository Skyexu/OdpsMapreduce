package com.legends.main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.legends.mr.ShopAppearNumMap;
import com.legends.mr.ShopAppearNumReduce;

public class ShopAppearNumMain {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("append_id:string,user_id:string,shop_id:string,mall_id:string,time_stamp:string,date:string,hour:string,dayofweek:string,user_lon:double,user_lat:double,shop_lon:double,shop_lat:double,category_id:string,price:double"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("shop_id:string,mall_id:string,count:bigint"));

		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName("shop_no_wifi_in").build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName("shop_no_wifi_out").build(), job);

		// TODO: specify a mapper
		job.setMapperClass(ShopAppearNumMap.class);
		// TODO: specify a reducer
		job.setReducerClass(ShopAppearNumReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
