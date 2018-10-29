package com.github.shriyog.flinkstarter.streamswitch;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class CommandSource extends RichSourceFunction<String> {

	private static final long serialVersionUID = 800369242890273441L;

	@Override
	public void run(SourceContext<String> context) throws Exception {
		List<String> commands = new ArrayList<String>();
		commands.add("process 1");
		commands.add("process 2");

		commands.add("pause 1");
		commands.add("pause 2");

		commands.add("process 1");
		commands.add("process 2");
		commands.add("process 1");
		commands.add("process 2");

		commands.add("resume 1");
		commands.add("resume 2");

		commands.add("process 1");
		commands.add("process 2");

		for (String command : commands) {
			context.collect(command);
			Thread.sleep(100);
		}
	}

	@Override
	public void cancel() {

	}

}
