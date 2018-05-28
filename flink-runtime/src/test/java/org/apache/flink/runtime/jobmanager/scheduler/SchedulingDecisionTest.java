package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.instance.AckingDummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.TestJobGraphs.SimpleJobGraph;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class SchedulingDecisionTest extends TestLogger {

	private final static Logger log = LoggerFactory.getLogger(SchedulingDecisionTest.class);

	private final static int
		INSTANCE_NUMBER = 2,
		NUM_SLOTS = 4;

	@Test
	public void simpleJobGraphTest() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.queuedActionExecutionContext());

		for (int i = 0; i < INSTANCE_NUMBER; i++) {
			scheduler.newInstanceAvailable(SchedulerTestUtils.getRandomInstance(NUM_SLOTS, AckingDummyActorGateway.INSTANCE));
		}

		JobGraph jobGraph = new JobGraph(new SimpleJobGraph().getVertices());
		ExecutionGraph executionGraph = ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			new Configuration(),
			TestingUtils.queuedActionExecutionContext(),
			TestingUtils.queuedActionExecutionContext(),
			scheduler,
			ClassLoader.getSystemClassLoader(),
			null,
			Time.seconds(1L),
			null,
			new UnregisteredMetricsGroup(),
			new VoidBlobWriter(),
			Time.seconds(1L),
			log);

		executionGraph.scheduleForExecution();
		System.out.println("job state: " + executionGraph.getState().toString() + "\n\n");

		for (Instance i : scheduler.getInstancesByHost().values().stream().reduce((a, b) -> {
			a.addAll(b);
			return a;
		}).orElse(new ArrayList<>())) {
			System.out.println("instance: " + i.getId());
			System.out.println("free slots: " + i.getNumberOfAvailableSlots());
			System.out.println("allocated slots: " + i.getNumberOfAllocatedSlots());
			System.out.println();
		}
	}
}
