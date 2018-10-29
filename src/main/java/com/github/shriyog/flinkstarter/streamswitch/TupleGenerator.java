package com.github.shriyog.flinkstarter.streamswitch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TupleGenerator implements MapFunction<String, Tuple2<Integer, String>> {

	private static final long serialVersionUID = 5939149759678724940L;

	@Override
	public Tuple2<Integer, String> map(String value) throws Exception {
		String[] arr = value.split(" ");
		return new Tuple2<Integer, String>(Integer.parseInt(arr[1]), arr[0]);
	}
}
