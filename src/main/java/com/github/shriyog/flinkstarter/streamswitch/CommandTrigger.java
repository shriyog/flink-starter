package com.github.shriyog.flinkstarter.streamswitch;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class CommandTrigger extends Trigger<Tuple2<Integer, String>, Window> {
	private static final long serialVersionUID = 6019394441745229260L;
	private transient boolean paused = false;

	/**
	 * We trigger the window processing as per command inside the record. The
	 * process records are buffered when a pause record is received and the
	 * buffer is evicted once resume record is received. If no pause record is
	 * received earlier, then for each process record the buffer is evicted.
	 */
	@Override
	public TriggerResult onElement(Tuple2<Integer, String> element, long timestamp, Window window,
			TriggerContext context) throws Exception {
		if (element.f1.equals("pause")) {
			paused = true;
			return TriggerResult.CONTINUE;
		} else if (element.f1.equals("resume")) {
			paused = false;
			return TriggerResult.FIRE_AND_PURGE;
		} else if (paused)
			return TriggerResult.CONTINUE;
		return TriggerResult.FIRE_AND_PURGE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, Window window, TriggerContext context) throws Exception {
		return null;
	}

	@Override
	public TriggerResult onEventTime(long time, Window window, TriggerContext context) throws Exception {
		return null;
	}

	@Override
	public void clear(Window window, TriggerContext context) throws Exception {

	}

}
