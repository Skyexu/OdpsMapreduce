package com.legends.main;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.legends.mr.SplitBehaviorWifiOnlyMap;
import com.legends.mr.SplitTestWifiOnlyMap;
import com.legends.mr.SplitWifiOnlyMap;

public class SplitWifiMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf job = new JobConf();
	    job.setMapperClass(SplitWifiOnlyMap.class);
	    job.setNumReduceTasks(0);
	    InputUtils.addTable(TableInfo.builder().tableName("hy_in").build(), job);
	    OutputUtils.addTable(TableInfo.builder().tableName("hy_out").build(), job);

	    RunningJob rj = JobClient.runJob(job);
	    rj.waitForCompletion();
	}

}