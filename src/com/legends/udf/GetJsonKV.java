package com.legends.udf;

import java.util.Map.Entry;

import com.aliyun.odps.udf.ExecutionContext;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
@Resolve({"string,string->string,bigint,double"})
public class GetJsonKV extends UDTF {

	public void setup(ExecutionContext ctx) throws UDFException {
	}

	public void process(Object[] args) throws UDFException {
		String row_id = (String) args[0];
	    String jsonString = (String)args[1];
	    JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
	    
	    for(Entry<String, JsonElement> entry : jsonObject.entrySet()){
	    	Long shop_id = Long.parseLong(entry.getKey());
	    	double pre = entry.getValue().getAsDouble();
	    	forward(row_id,shop_id, pre);
	    }
	    
	}

	public void close() throws UDFException {
	}

}
