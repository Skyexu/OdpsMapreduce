package Test;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class TestMapper extends MapperBase {

	@Override
	public void setup(TaskContext context) throws IOException {
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		//获取输入表字段
		String key = record.get(0).toString() + record.get(1).toString() +
				record.get(2).toString() + record.get(3).toString() +
				record.get(4).toString() + record.get(5).toString();
		String value = record.get(1).toString();
		//定义reduce的key和value 使用的是MapOutputKey MapOutputValue
		Record word = context.createMapOutputKeyRecord();
		Record  one = context.createMapOutputValueRecord();
		word.set(new Object[] {key});
		one.set(new Object[] {1L});
		context.write(word,one);
		String[] values=value.split("_");
		for (String val : values) {
			word.set(new Object[] {val});
			one.set(new Object[] {1L});
			context.write(word,one);
		}
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
