package com.github.shriyog.flinkstarter.streamswitch;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DownstreamProcessor implements WindowFunction<Tuple2<Integer, String>, String, Tuple, TimeWindow> {

	private static final long serialVersionUID = 1885064882422568354L;

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<Integer, String>> input, Collector<String> out)
			throws Exception {
		System.out.println("Evicting buffer for id " + key.getField(0));
		for (Tuple2<Integer, String> val : input) {
			System.out.println("id: " + val.f0 + " command: " + val.f1);
		}
	}

}
