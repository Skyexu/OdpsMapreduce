package com.legends.mr;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

/**
 * 将 多行 wifi 变为 一行的 map 函数
 * @author Skye
 *
 */
public class RestoreWifiMap extends MapperBase {
	private Record key;
	private Record value;

	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		//int index = 0;
//		for (int i = 0; i < record.getColumnCount() - 3; i++) {
//			key.set(i, record.get(i));
//			if (i >= record.getColumnCount() -4) {
//				value.set(index++, record.get(i));
//			}
//		}
//		key.setString("rowid", record.getString(0));
//		key.setString("user_id", record.getString(1));
//		key.setString("shop_id", record.getString(2));
//		key.setString("time_stamp", record.getString(3));
//		key.setDouble("longitude", record.getDouble(4));
//		key.setDouble("latitude", record.getDouble(5));
//		//bssid:String,strength:Double,connect:String
//		value.setString("bssid", record.getString(6));
//		value.setString("strengt", record.getString(7));
//		value.setString("connect", record.getString(8));
		//System.out.println(record.getColumnCount());
		// 按照数据库表的字段索引获取数据并写入 key 和 value中
		key.setString(0, record.getString(0));
		key.setString(1, record.getString(1));
		key.setString(2, record.getString(2));
		key.setString(3, record.getString(3));
		key.setDouble(4, record.getDouble(4));
		key.setDouble(5, record.getDouble(5));
		//bssid:String,strength:Double,connect:String
		value.setString(0, record.getString(6));
		value.setDouble(1, record.getDouble(7));
		value.setString(2, record.getString(8));
		context.write(key, value);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
		
	}

}
