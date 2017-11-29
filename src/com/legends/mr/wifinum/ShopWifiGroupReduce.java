package com.legends.mr.wifinum;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
/**
 * 输入 shop_id , bssid
 * 
 * 输出 shop_id,(b1|b2|b3....)
 * @author Skye
 *
 */
public class ShopWifiGroupReduce extends ReducerBase {
	private Record result = null;
	
	@Override
	public void setup(TaskContext context) throws IOException {
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		
		Set<String> wifiSet = new HashSet<>();
		while (values.hasNext()) {
			Record value = values.next();
			String bssid = value.getString("bssid");
			wifiSet.add(bssid);
		}
		StringBuffer stringBuffer = new StringBuffer();
		int count = 0;
		for (String wifiString : wifiSet) {
			stringBuffer.append(wifiString);
			count ++;
			if (count != wifiSet.size()) {
				stringBuffer.append("|");
			}
		}
		String wifis = stringBuffer.toString();
		
		//System.out.println(key.get("shop_id").toString() + " : " + wifis);
		result.set(0,key.get("shop_id").toString());
		result.set(1, wifis);
		context.write(result);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
