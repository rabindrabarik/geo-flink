package org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework;

import org.junit.Before;
import org.junit.Test;

public class SchedulingDecisionWriterTest {

	private SchedulingDecisionWriter writer;

	@Before
	public void setup() {
		writer = new SchedulingDecisionWriter("test-csv.csv");
	}

	@Test
	public void write() {
		writer.write(new SchedulingDecision(0.0, 0.0, "aScheduler", "aJobGraph", "anInstanceSet"));
	}
}
