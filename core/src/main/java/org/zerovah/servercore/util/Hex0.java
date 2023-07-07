package org.zerovah.servercore.util;

import java.io.ByteArrayOutputStream;

/**
 * 十六进制转换工具类
 * 
 * @author huachp
 */
public class Hex0 {
	
	static final byte[] HEX_DIGITS = new byte[] { 
			(byte) '0', (byte) '1', (byte) '2', (byte) '3', 
			(byte) '4', (byte) '5', (byte) '6', (byte) '7', 
			(byte) '8', (byte) '9', (byte) 'A', (byte) 'B', 
			(byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F' 
	};
	
	
	public static String bytesToHex(byte[] x) {
		if (x == null || x.length == 0) {
			return null;
		}
		// Send as hex
		ByteArrayOutputStream bOut = new ByteArrayOutputStream((x.length * 2) + 2);
		bOut.write('0');
		bOut.write('x');

		for (int i = 0; i < x.length; i++) {
			int lowBits = (x[i] & 0xff) / 16;
			int highBits = (x[i] & 0xff) % 16;

			bOut.write(HEX_DIGITS[lowBits]);
			bOut.write(HEX_DIGITS[highBits]);
		}
		
		return bOut.toString();
	}
	
	
}
