package com.legends.mr;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class DistanceShopReduce extends ReducerBase {
	private Record result = null;
	
	private final double EARTH_RADIUS = 6378.137;
	
	private double rad(double d)  
    {  
       return d * Math.PI / 180.0;  
    }  
	
	private double distance(double lon1,double lat1,double lon2, double lat2){
		double radLat1 = rad(lat1);
	    double radLat2 = rad(lat2);
	    double a = radLat1 - radLat2;
	    double b = rad(lon1) - rad(lon2);
	    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2)+Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2))); 
	    s = s * EARTH_RADIUS * 1000; 
	    
	    return s;
	}
	@Override
	public void setup(TaskContext context) throws IOException {
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		result.set("row_id", key.get(0));
		
		String shop_id = "";
		double minDistance = Double.MAX_VALUE;
		int count = 0;
		while (values.hasNext()) {
			Record value = values.next();
			if (count == 0 ) {
				shop_id = value.getString("shop_id");
			}
			
			double distance = distance(value.getDouble("along"),value.getDouble("alat"),value.getDouble("blong"),value.getDouble("blat"));
			if (distance < minDistance) {
				minDistance = distance;
				shop_id = value.getString("shop_id");
			}
			count++;
		}
		
		result.set("shop_id", shop_id);
		context.write(result);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
