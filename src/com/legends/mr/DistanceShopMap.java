package com.legends.mr;

import java.io.IOException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
/**
 * 输入两个经纬度，计算最小距离
 * @author Skye
 *
 */
public class DistanceShopMap extends MapperBase{

	private Record key;
	private Record value;

	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		
		// 按照数据库表的字段索引获取数据并写入 key 和 value中
		key.setString("row_id", record.getString(0));
		
		value.setString("shop_id", record.getString(1));
		value.setDouble("along", record.getDouble(3));
		value.setDouble("alat", record.getDouble(4));
		value.setDouble("blong", record.getDouble(5));
		value.setDouble("blat", record.getDouble(6));
		
		context.write(key, value);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
		
	}

}
