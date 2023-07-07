package org.zerovah.servercore.util;

import com.alibaba.fastjson2.JSON;

public class MessageOutput {
	
	private Object messageLite; // 2.6 GeneratedMessageLite
	
	public static MessageOutput create(Object messageLite) {
		MessageOutput output = new MessageOutput();
		output.messageLite = messageLite;
		return output;
	}
	
	public static String toString(Object messageLite) {
		return JSON.toJSONString(messageLite, ProtobufPropertyFilter.INST);
	}
	
	@Override
	public String toString() {
		return toString(messageLite);
	}
}
