package com.legends.mr;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class ShopAppearNumReduce extends ReducerBase {
	private Record result = null;
	@Override
	public void setup(TaskContext context) throws IOException {
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		
		
		long sum = 0;
		while (values.hasNext()) {
			Record value = values.next();
			// 也可以不获取，直接计数
			long count = value.getBigint("count");
			sum += count;
			
		}

		result.set("shop_id",key.get("shop_id"));
		result.set("mall_id",key.get("mall_id"));
		result.set("shop_appear_num",sum);
		
		context.write(result);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
