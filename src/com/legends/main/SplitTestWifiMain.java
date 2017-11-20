package com.legends.main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.legends.mr.SplitTestWifiOnlyMap;

public class SplitTestWifiMain {

	public static void main(String[] args) throws OdpsException {
		// TODO Auto-generated method stub
		JobConf job = new JobConf();
		job.setMapperClass(SplitTestWifiOnlyMap.class);
		job.setNumReduceTasks(0);
		InputUtils.addTable(TableInfo.builder().tableName("shop_test").build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName("shop_test_out").build(), job);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
