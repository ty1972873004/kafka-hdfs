package com.hncy58.kafka;
import java.util.Arrays;

import org.apache.kudu.Common.DataType;
import org.apache.kudu.Type;

public class Test {
	
	public static void main(String[] args) {
		
		Arrays.asList("a,b c,c d, d , e, ".split(" *, *")).forEach(s -> System.out.println("---" + s + "---"));
		
		StringBuffer buf = new StringBuffer("_1234");
		System.out.println(buf.delete(0, 1).length());
		
		Arrays.asList(Type.values()).forEach(t -> {
			System.out.println(t);
			System.out.println(t.getSize());
			System.out.println(t.getName());
			System.out.println(t.getDataType());
		});
		System.out.println("----------------------");
		System.out.println(Type.valueOf("BOOL"));
		System.out.println(DataType.valueOf("UNIXTIME_MICROS"));
	}
}
