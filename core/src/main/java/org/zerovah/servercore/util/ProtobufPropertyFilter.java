package org.zerovah.servercore.util;

import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.filter.PropertyPreFilter;

public class ProtobufPropertyFilter implements PropertyPreFilter {
	
	static ProtobufPropertyFilter INST = new ProtobufPropertyFilter();
	
	@Override
	public boolean process(JSONWriter writer, Object source, String name) {
		if (name.equals("defaultInstanceForType")) {
			return false;
		}
		if (name.equals("initialized")) {
			return false;
		}
		if (name.equals("parserForType")) {
			return false;
		}
		if (name.equals("serializedSize")) {
			return false;
		}
		if (name.equals("unknownFields")) {
			return false;
		}
		if (name.contains("Bytes")) {
			return false;
		}
		if (name.contains("OrBuilderList")) {
			return false;
		}
		return true;
	}
}
