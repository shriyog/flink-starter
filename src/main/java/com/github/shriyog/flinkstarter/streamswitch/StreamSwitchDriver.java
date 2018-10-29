package com.github.shriyog.flinkstarter.streamswitch;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamSwitchDriver {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> inputStream = env.addSource(new CommandSource());
		SingleOutputStreamOperator<Tuple2<Integer, String>> commandStream = inputStream.map(new TupleGenerator());
		
		commandStream.keyBy(0)
					 .timeWindow(Time.of(1, TimeUnit.DAYS))
					 .trigger(new CommandTrigger())
					 .apply(new DownstreamProcessor());

		env.execute();
	}
}
