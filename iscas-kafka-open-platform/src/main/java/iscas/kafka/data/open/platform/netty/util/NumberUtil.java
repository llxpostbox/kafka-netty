package iscas.kafka.data.open.platform.netty.util;
/**
 * @author 遇见清晨
 * @date 创建时间： 2020年5月28日 下午4:53:06
 * @version 1.0  111
 * @user: Lenovo  22
 */
public class NumberUtil {
	
	public static int getRandomInt(int num) {
		int number = (int)(Math.random()*num);
		return  number == 0?1:number;
	}
}
