package com.legends.udf;

import com.aliyun.odps.udf.UDF;

public class SpherDistance extends UDF {
	
	private final double EARTH_RADIUS = 6378.137;
	
	private double rad(double d)  
    {  
       return d * Math.PI / 180.0;  
    }  
	
	
	/**
	 * 计算球面距离
	 * @return
	 */
	public Double evaluate(Double lon1,Double lat1,Double lon2, Double lat2){
		double radLat1 = rad(lat1);
	    double radLat2 = rad(lat2);
	    double a = radLat1 - radLat2;
	    double b = rad(lon1) - rad(lon2);
	    Double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2)+Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2))); 
	    s = s * EARTH_RADIUS * 1000; 
	    
	    return s;
	}

}
