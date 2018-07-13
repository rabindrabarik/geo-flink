package org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

public class SchedulingDecisionWriter {
	private static SchedulingDecisionWriter instance = new SchedulingDecisionWriter();

	private CsvOutputFormat<SchedulingDecision> writer;

	public SchedulingDecisionWriter() {
		writer = new CsvOutputFormat<SchedulingDecision>(new Path("schedulingDecisions"));
	}

	public SchedulingDecisionWriter(String pathString) {
		writer = new CsvOutputFormat<SchedulingDecision>(new Path(pathString));
	}

	public static SchedulingDecisionWriter getInstance() {
		return instance;
	}

	public void write(SchedulingDecision decision) {
		try {
			writer.writeRecord(decision);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
