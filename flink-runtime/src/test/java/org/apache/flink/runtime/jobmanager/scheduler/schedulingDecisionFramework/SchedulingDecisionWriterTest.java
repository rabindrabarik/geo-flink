package org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework;

import org.junit.Before;
import org.junit.Test;

public class SchedulingDecisionWriterTest {

	private TestOutputWriter writer;

	@Before
	public void setup() {
		writer = new TestOutputWriter("test-csv.csv");
	}

	@Test
	public void write() {
		writer.write(new SchedulingDecision(0.0, 0.0, "aScheduler", "aJobGraph", "anInstanceSet", 1L));
	}
}
