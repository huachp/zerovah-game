package org.zerovah.servercore.util;

public class MathUtil {

	/**
	 * 位运算：判断value中第pos位是否为1
	 * pos从0开始
	 */
	public static boolean isSelected(int value, int pos) {
		if(pos < 0)return false;
		
        int d = 0b1;
        d = d<<pos;
        int data = d & value;
        return (data>0);
	}

    /**
	 * 选中指定位置的项：判断value中第pos位是否为1
	 * pos从0开始
	 */
	public static int selected(int setting, int pos) {
        if(pos < 0)
            return setting;
        int d = 0b1;
        d = d<<pos;
        int data = d | setting;
        return data;
	}
	
}
