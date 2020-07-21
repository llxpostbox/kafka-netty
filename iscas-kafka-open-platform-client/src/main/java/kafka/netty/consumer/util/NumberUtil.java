package kafka.netty.consumer.util;

import java.util.Random;

/**
 * @author 遇见清晨
 * @date 创建时间： 2020年5月28日 下午4:53:06
 * @version 1.0
 * @user: Lenovo
 */
public class NumberUtil {
	
	public static int getRandomInt(int num) {
		int number = (int)(Math.random()*num);
		return  number == 0?1:number;
	}

	public static void main(String[] args) {
		Random random = new Random();
		for (int i=0; i<100; i++){
			System.out.println(random.nextInt(5));
		}
	}
}
