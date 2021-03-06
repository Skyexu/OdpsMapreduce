package com.legends.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

/**
 * 切分 user_shop_behavior 表的   wifi_info 信息，一行变多行
 * 
 * @author Skye
 *
 */
public class SplitBehaviorWifiOnlyMap extends MapperBase {
	// 计数器，用于对 rowid 计数
	Counter gCnt;
	
	private String convertConnect(String connect){
		if (connect.equalsIgnoreCase("false")) 
			return "0";
		else
			return "1";
	}
	
//	private double convertStrength(String strength){
//		if (strength == "null"){
//			return "null"; //???????????
//		}else{
//			double s = Double.parseDouble(strength);
//			return (s + 113) / 2;
//		}
//	}
	
	private String convertDate(String time){
		SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMdd");
		Date date = null;
		try {
			date = formatter1.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return formatter2.format(date);
	}
	
	private String convertHour(String time){
		SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		SimpleDateFormat formatter2 = new SimpleDateFormat("HH");
		Date hour = null;
		try {
			hour = formatter1.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
		return formatter2.format(hour);
	}
	
	private int convertWeek(String time){
		SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		SimpleDateFormat formatter2 = new SimpleDateFormat("EEE",Locale.ENGLISH);
		Date date = null;
		// Mon Tues Wed Thur Fri Sat Sun
		Map<String, Integer> weekMap = new HashMap<>();
		weekMap.put("Sun", 0);
		weekMap.put("Mon", 1);
		weekMap.put("Tue", 2);
		weekMap.put("Wed", 3);
		weekMap.put("Thu", 4);
		weekMap.put("Fri", 5);
		weekMap.put("Sat", 6);
		try {
			date = formatter1.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		String week = formatter2.format(date);
		int weekInt = weekMap.get(week);
		return weekInt;
	}
	
	@Override
	public void setup(TaskContext context) throws IOException {
		// 获取计数器
		gCnt = context.getCounter("MyCounters", "global_counts");
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		//获取输入表字段
		String user_id = record.get(0).toString();
		String shop_id = record.get(1).toString();
		String time_stamp = record.get(2).toString();
		Double longitude = (Double)record.get(3);
		Double latitude = (Double)record.get(4);
		//对输入字段进行自定义操作 
		
		String[] wifi_infos = record.get(5).toString().split(";");
		
		//写到输出表 使用的输出Output 按照字段填入(多字段输出)
		Record result_record = context.createOutputRecord();
		//columns=row_id:String,user_id:String,mall_id:String,time_stamp:String,longitude:Double,latitude:Double,wifi_infos:String
		// 每读一行计数器加1
		result_record.set("rowid", gCnt.getValue()+"");
		gCnt.increment(1);
		result_record.set("user_id", user_id);
		result_record.set("shop_id", shop_id);
		result_record.set("time_stamp", time_stamp);
		result_record.set("date", convertDate(time_stamp));
		result_record.set("hour", convertHour(time_stamp));
		result_record.set("dayofweek", convertWeek(time_stamp));
		result_record.set("user_lon", longitude);
		result_record.set("user_lat", latitude);
		
		// 输出 wifi_info 信息
		// wifi_infos 为空 则
//		if (wifi_infos[0].equals("")) {
//			result_record.set("bssid", "");
//			result_record.set("strength", "");
//			result_record.set("connect", "");
//			context.write(result_record);
//		}
		for(String wifi:wifi_infos){
			//bssid:String,strength:String,connect:String
			String[] info_splits = wifi.split("\\|");
			result_record.set("bssid", info_splits[0]);
			result_record.set("strength", info_splits[1]);
			result_record.set("connect", convertConnect(info_splits[2]));
			context.write(result_record);
		}	
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
