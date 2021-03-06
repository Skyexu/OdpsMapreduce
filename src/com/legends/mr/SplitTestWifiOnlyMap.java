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
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class SplitTestWifiOnlyMap extends MapperBase {
	// 计数器，用于对 row_id 计数
		Counter gCnt;
		
		private String convertConnect(String connect){
			if (connect.equalsIgnoreCase("false")) 
				return "0";
			else
				return "1";
			
		}
		
		private double convertStrength(String strength){
			double s = Double.parseDouble(strength);
			
			return (s + 113) / 2;
		}
		
		private String convertTime(String time){
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMdd");
			Date date = null;
			
			try {
				date = formatter.parse(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
			
			
			return formatter2.format(date);
		}
		
		private int convertWeek(String time){
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			
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
				date = formatter.parse(time);
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
			String row_id = record.get(0).toString();
			String user_id = record.get(1).toString();
			String mall_id = record.get(2).toString();
			String time_stamp = record.get(3).toString();
			Double longitude = (Double)record.get(4);
			Double latitude = (Double)record.get(5);
			//对输入字段进行自定义操作 
			String[] wifi_infos = record.get(6).toString().split(";");
			//写到输出表 使用的输出Output 按照字段填入(多字段输出)
			Record result_record = context.createOutputRecord();
			//columns=row_id:String,user_id:String,mall_id:String,time_stamp:String,longitude:Double,latitude:Double,wifi_infos:String
			// 每读一行计数器加1
			result_record.set("rowid", gCnt.getValue()+"");
			gCnt.increment(1);
			result_record.set("row_id", row_id);
			result_record.set("user_id", user_id);
			result_record.set("mall_id", mall_id);
			result_record.set("time_stamp", convertTime(time_stamp));
			result_record.set("user_lon", longitude);
			result_record.set("user_lat", latitude);
			
			// 输出 wifi_info 信息
			for(String wifi:wifi_infos){
				//bssid:String,strength:String,connect:String
				//System.out.println(wifi);
				String[] info_splits = wifi.split("\\|");
				result_record.set("bssid", info_splits[0]);
				result_record.set("strength", convertStrength(info_splits[1]));
				result_record.set("connect", convertConnect(info_splits[2]));
				context.write(result_record);
			}
			
		}

		@Override
		public void cleanup(TaskContext context) throws IOException {
		}

}
