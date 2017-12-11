package com.legends.mr;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class ShopAvgAssiReduce extends ReducerBase {
	private Record result = null;
	@Override
	public void setup(TaskContext context) throws IOException {
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		
		int count = 0;
		double sumAssi = 0;
		while (values.hasNext()) {
			Record value = values.next();
			double assi = value.getDouble("assi");
			sumAssi += assi;
			count ++;
		}
		//System.out.println(count);
		//System.out.println(sumAssi);
		result.set("shop_id",key.get("shop_id"));
		result.set("shop_mean_assi",sumAssi/count);
		
		context.write(result);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
