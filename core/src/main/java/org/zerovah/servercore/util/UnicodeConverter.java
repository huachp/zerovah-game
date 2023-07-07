package org.zerovah.servercore.util;

import io.netty.util.CharsetUtil;

import java.util.HashMap;

public class UnicodeConverter {

	private static final char[] hexDigit = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'A', 'B', 'C', 'D', 'E', 'F'
	};

	private static char toHex(int nibble) {

		return hexDigit[(nibble & 0xF)];

	}

	/**
	 * 
	 * 将字符串编码成 Unicode 形式的字符串. 如 "黄" to "\u9EC4"
	 * 
	 * Converts unicodes to encoded \\uxxxx and escapes
	 * 
	 * special characters with a preceding slash
	 * 
	 * 
	 * 
	 * @param theString
	 * 
	 *            待转换成Unicode编码的字符串。
	 * 
	 * @param escapeSpace
	 * 
	 *            是否忽略空格，为true时在空格后面是否加个反斜杠。
	 * 
	 * @return 返回转换后Unicode编码的字符串。
	 */
	public static String toEncodedUnicode(String theString, boolean escapeSpace) {

		int len = theString.length();

		int bufLen = len * 2;

		if (bufLen < 0) {

			bufLen = Integer.MAX_VALUE;

		}

		StringBuilder outBuffer = new StringBuilder(bufLen);

		for (int x = 0; x < len; x++) {

			char aChar = theString.charAt(x);

			// Handle common case first, selecting largest block that

			// avoids the specials below

			if ((aChar > 61) && (aChar < 127)) {

				if (aChar == '\\') {

					outBuffer.append('\\');

					outBuffer.append('\\');

					continue;

				}

				outBuffer.append(aChar);

				continue;

			}

			switch (aChar) {

			case ' ':

				if (x == 0 || escapeSpace)
					outBuffer.append('\\');

				outBuffer.append(' ');

				break;

			case '\t':

				outBuffer.append('\\');

				outBuffer.append('t');

				break;

			case '\n':

				outBuffer.append('\\');

				outBuffer.append('n');

				break;

			case '\r':

				outBuffer.append('\\');

				outBuffer.append('r');

				break;

			case '\f':

				outBuffer.append('\\');

				outBuffer.append('f');

				break;

			case '=': // Fall through

			case ':': // Fall through

			case '#': // Fall through

			case '!':

				outBuffer.append('\\');

				outBuffer.append(aChar);

				break;

			default:

				if ((aChar < 0x0020) || (aChar > 0x007e)) {

					// 每个unicode有16位，每四位对应的16进制从高位保存到低位

					outBuffer.append('\\');

					outBuffer.append('u');

					outBuffer.append(toHex((aChar >> 12) & 0xF));

					outBuffer.append(toHex((aChar >> 8) & 0xF));

					outBuffer.append(toHex((aChar >> 4) & 0xF));

					outBuffer.append(toHex(aChar & 0xF));

				} else {

					outBuffer.append(aChar);

				}

			}

		}

		return outBuffer.toString();

	}

	/**
	 * 
	 * 从 Unicode 形式的字符串转换成对应的编码的特殊字符串。 如 "\u9EC4" to "黄".
	 * 
	 * Converts encoded \\uxxxx to unicode chars
	 * 
	 * and changes special saved chars to their original forms
	 * 
	 * 
	 * 
	 * @param in
	 * 
	 *            Unicode编码的字符数组。
	 * 
	 * @param off
	 * 
	 *            转换的起始偏移量。
	 * 
	 * @param len
	 * 
	 *            转换的字符长度。
	 * 
	 *            转换的缓存字符数组。
	 * 
	 * @return 完成转换，返回编码前的特殊字符串。
	 */
	public static String fromEncodedUnicode(char[] in, int off, int len) {

		char aChar;

		char[] out = new char[len]; // 只短不长

		int outLen = 0;

		int end = off + len;

		while (off < end) {

			aChar = in[off++];

			if (aChar == '\\') {

				aChar = in[off++];

				if (aChar == 'u') {

					// Read the xxxx

					int value = 0;

					for (int i = 0; i < 4; i++) {

						aChar = in[off++];

						switch (aChar) {

						case '0':

						case '1':

						case '2':

						case '3':

						case '4':

						case '5':

						case '6':

						case '7':

						case '8':

						case '9':

							value = (value << 4) + aChar - '0';

							break;

						case 'a':

						case 'b':

						case 'c':

						case 'd':

						case 'e':

						case 'f':

							value = (value << 4) + 10 + aChar - 'a';

							break;

						case 'A':

						case 'B':

						case 'C':

						case 'D':

						case 'E':

						case 'F':

							value = (value << 4) + 10 + aChar - 'A';

							break;

						default:

							throw new IllegalArgumentException(
									"Malformed \\uxxxx encoding.");

						}

					}

					out[outLen++] = (char) value;

				} else {

					if (aChar == 't') {

						aChar = '\t';

					} else if (aChar == 'r') {

						aChar = '\r';

					} else if (aChar == 'n') {

						aChar = '\n';

					} else if (aChar == 'f') {

						aChar = '\f';

					}

					out[outLen++] = aChar;

				}

			} else {

				out[outLen++] = (char) aChar;

			}

		}

		return new String(out, 0, outLen);

	}

	/**
	 * 
	 * Function name:getRequestUriValue Description: 解析uri字串，获得数据
	 * 
	 * @param uri：字串
	 * @return HashMap: 数据对
	 */
	public static HashMap<String, String> getRequestUriValue(String uri) {
		HashMap<String, String> attributes = new HashMap<>();
		int beginIndex = uri.indexOf("?");
		if (beginIndex != -1) {
			String body = uri.substring(beginIndex + 1);
			String[] strs = body.split("&");
			for (String keyValues : strs) {
				String[] attribute = keyValues.split("=");
				if (attribute.length > 2) {
					continue;
				}
				if (attribute.length == 1) {
					String key = attribute[0];
					String value = "";
					attributes.put(key, value);
				}
				if (attribute.length == 2) {
					String key = attribute[0];
					String value = attribute[1];
					attributes.put(key, value);
				}
			}
		}
		return attributes;
	}
	
	
	
	public static void main(String[] args) throws Exception {

		String s = "\u5e7f\u4e1c\u7701\u60e0\u5dde\u5e02";

		try {
			System.out.println("Original:\t\t" + s);
		} catch (Exception e) {
			e.printStackTrace();
		}

		s = toEncodedUnicode(s, true);

		System.out.println("to unicode:\t\t" + s + ", 占用字节:" + s.getBytes(CharsetUtil.UTF_8).length);

		s = fromEncodedUnicode(s.toCharArray(), 0, s.length());

		System.out.println("from unicode:\t" + s);


	}

}
