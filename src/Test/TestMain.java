package Test;

import com.legends.udf.SpherDistance;

public class TestMain {

	public static void main(String[] args) throws Exception {
		SpherDistance spherDistance = new SpherDistance();
		double dis = spherDistance.evaluate(31.86, 117.27, 30.26, 120.19);
		System.out.println(dis);
	}

}
