package com.legends.mr;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
/**
 * 计算 历史 shop 的 wifi 平均强度
 * 
 * @author Skye
 *
 */
public class ShopAvgAssiMap extends MapperBase {
	private Record key;
	private Record value;

	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String shop_id = record.getString("shop_id");
		double assi = record.getDouble("assi");
		
		key.set("shop_id",shop_id);
		value.set("assi", assi);
		context.write(key, value);
		
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
