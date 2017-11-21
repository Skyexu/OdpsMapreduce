package Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TestDate {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String time = "2017-11-25 10:10";
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
		System.out.println(week);
		int weekInt = weekMap.get(week);
		System.out.println(weekInt);
	}

}
