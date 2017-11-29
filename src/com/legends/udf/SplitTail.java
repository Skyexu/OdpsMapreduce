package com.legends.udf;

import com.aliyun.odps.udf.UDF;

public class SplitTail extends UDF {

	// TODO define parameters and return type, e.g., public Long evaluate(Long a, Long b)
	public Long evaluate(String str){
		String[] strings = str.split("_");
		return Long.parseLong(strings[1]);
	}

}
