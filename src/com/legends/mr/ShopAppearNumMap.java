package com.legends.mr;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;
/**
 * 计算 shop 人流量
 * @author Skye
 *
 */
public class ShopAppearNumMap extends MapperBase {
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
		String mall_id = record.getString("mall_id");
		
		
		key.set("shop_id",shop_id);
		key.set("mall_id", mall_id);
		//这里其实随便传个值就可以，reduce 直接计数即可
		value.set("count", new Long(1));
		context.write(key, value);
		
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
		
	}
}
