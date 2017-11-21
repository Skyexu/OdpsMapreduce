package com.legends.mr;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;
/**
 * 将 多行 wifi 变为 一行的 map 函数
 * 
 * 对应表 wifi_train  字段  
 * append_id:STRING,row_id:STRING,user_id:STRING:shop_id:STRING,mall_id:STRING,time_stamp:STRING,date:STRING,hour:STRING,dayofweek:BIGINT,
 * user_lon:DOUBLE,user_lat:DOUBLE,bssid:STRING,assi:DOUBLE,connect:STRING
 * 
 * @author Skye
 *
 */
public class RestoreWifiTrainMap extends MapperBase {

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
		key.setString(0, record.getString(0));
		key.setString(1, record.getString(1));
		key.setString(2, record.getString(2));
		key.setString(3, record.getString(3));
		key.setString(4, record.getString(4));
		key.setString(5, record.getString(5));
		key.setString(6, record.getString(6));
		key.setString(7, record.getString(7));
		key.setBigint(8, record.getBigint(8));
		key.setDouble(9, record.getDouble(9));
		key.setDouble(10, record.getDouble(10));
		//bssid:String,strength:Double,connect:String
		value.setString(0, record.getString(11));
		value.setDouble(1, record.getDouble(12));
		value.setString(2, record.getString(13));
		context.write(key, value);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
		
	}

}
