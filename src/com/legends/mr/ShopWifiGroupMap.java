package com.legends.mr;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
/**
 * 输入 shop_id , wifi_infos
 * 
 * 输出 shop_id , bssid
 * @author Skye
 *
 */
public class ShopWifiGroupMap extends MapperBase {
	private Record key;
	private Record value;
	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		
		String shop_id = record.get(0).toString();
		String[] wifi_infos = record.get(1).toString().split(";");
		for(String wifi:wifi_infos){
			//bssid:String,strength:String,connect:String
			String[] info_splits = wifi.split("\\|");
			key.set("shop_id",shop_id);
			value.set("bssid", info_splits[0]);
			context.write(key, value);
		}	
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
