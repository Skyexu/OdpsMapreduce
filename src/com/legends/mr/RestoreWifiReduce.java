package com.legends.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

/**
 * 将 多行 wifi 变为 一行的 reduce 函数
 * 
 * @author Skye
 *
 */
public class RestoreWifiReduce extends ReducerBase {
	
	private Record result = null;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		// 此 句若加在这里，会出现上一行的结果保留问题，目测是odps本地插件的问题
		//result = context.createOutputRecord();
		
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		int index = 0;
		
		result = context.createOutputRecord();
		
		// 写 wifi 相关信息之前的列，共六列
		for (int i = 0; i < key.getColumnCount(); i++) {
			result.set(index++, key.get(i));
			
		}
		
		int indexB = index;
		int indexS = index + 10;
		int indexC = index + 20;
		// wifiInfoMap 保存 wifi 信息
		Map<String, Double> wifiInfoMap = new HashMap<>();
		while (values.hasNext()) {
			Record value = values.next();
			String bssid = value.getString(0);
			double strength = value.getDouble(1);
			String connect = value.getString(2);
			wifiInfoMap.put(bssid + "," + connect, strength);
			
		}

		// 对 wifiInfoMap 按 strength 降序
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(wifiInfoMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		
		for (Map.Entry<String, Double> mapping : list) {
			String[] bssidConnect = mapping.getKey().split(",");
			double strength = mapping.getValue();
			result.set(indexB++,bssidConnect[0]);
			result.set(indexS++,strength);
			result.set(indexC++,bssidConnect[1]);
			
		}
		
		context.write(result);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
