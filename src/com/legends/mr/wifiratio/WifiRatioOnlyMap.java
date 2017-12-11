package com.legends.mr.wifiratio;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class WifiRatioOnlyMap extends MapperBase {
	private Record key;
	private Record value;
	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
        
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		//append_id:String,b_0:String,b_1:String,b_2:String,b_3:String,b_4:String,b_5:String,b_6:String,b_7:String,b_8:String,
		// shop_id:String,wifi_bssids:String
		
		Record result = context.createOutputRecord();
		
		String append_id = record.get("append_id").toString();
		String shop_id = record.get("shop_id").toString();
		
		if (record.isNull("wifi_bssids")) {
			result.set("append_id",append_id);
			result.set("shop_id",shop_id);
			result.set("wifi_ratio",999.0);
			result.set("same_wifi_num",0);
			context.write(result);
			//System.out.println("Skye: " + shop_id + " has not shown before");
		}else {
			String wifi_bssids = record.get("wifi_bssids").toString();
			//System.out.println("wifi_bssids: " + wifi_bssids);
			String[] bssids = wifi_bssids.split("\\|");
			
			Set<String> shopWifiBssids = new HashSet<>();
			for(String bssid: bssids){
				shopWifiBssids.add(bssid);
			}	
			
			int sameNum = 0;
			int wifiCount = 0;
			for(int i = 1;i < 11;i++){
				
				if (record.isNull(i)) {
					continue;
				}
				String bssid = record.get(i).toString();
				wifiCount ++;
				if (shopWifiBssids.contains(bssid)) {
					sameNum ++;
				}
			}
			//System.out.println("wifiCount: " + wifiCount);
			double wifiRatio = sameNum * 1.0 / wifiCount;
			
			result.set("append_id",append_id);
			result.set("shop_id",shop_id);
			result.set("wifi_ratio",wifiRatio);
			result.set("same_wifi_num",sameNum);
			
			context.write(result);
		}
		
		
		
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
