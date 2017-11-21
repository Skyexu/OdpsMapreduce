package com.legends.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.xml.crypto.Data;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

/**
 * �з� user_shop_behavior ���   wifi_info ��Ϣ��һ�б����
 * 
 * @author Skye
 *
 */
public class SplitWifiOnlyMap extends MapperBase {

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
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		//��ȡ������ֶ�
		String append_id = record.get(0).toString();
		String row_id = record.get(1).toString();
		String user_id = record.get(2).toString();
		String shop_id = record.get(3).toString();
		String mall_id = record.get(4).toString();
		String time_stamp = record.get(5).toString();
		Double longitude = (Double)record.get(6);
		Double latitude = (Double)record.get(7);
		//�������ֶν����Զ������ 
		String[] wifi_infos = record.get(8).toString().split(";");
		//д������� ʹ�õ����Output �����ֶ�����(���ֶ����)
		Record result_record = context.createOutputRecord();
		
		result_record.set("append_id", append_id);
		result_record.set("rowid", row_id);
		result_record.set("user_id", user_id);
		result_record.set("shop_id", shop_id);
		result_record.set("mall_id", mall_id);
		result_record.set("time_stamp", time_stamp);
		result_record.set("date", convertDate(time_stamp));
		result_record.set("hour", convertHour(time_stamp));
		result_record.set("dayofweek", convertWeek(time_stamp));
		result_record.set("user_lon", longitude);
		result_record.set("user_lat", latitude);
		
		// ��� wifi_info ��Ϣ
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
