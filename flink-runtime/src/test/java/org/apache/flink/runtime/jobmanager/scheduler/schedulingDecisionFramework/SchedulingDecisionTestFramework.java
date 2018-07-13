package org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.jobGraphs.SimpleJobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.spies.SchedulingDecisionSpy;
import org.apache.flink.runtime.jobmanager.scheduler.spies.SpyableFlinkScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.spies.SpyableGeoScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.spies.SpyableScheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.makeExecutionGraph;

/**
 * This tests logs the slot usages after scheduling a {@link SimpleJobGraph}.
 * */
@RunWith(Parameterized.class)
@Ignore
public abstract class SchedulingDecisionTestFramework extends TestLogger {

	private final static Logger log = LoggerFactory.getLogger(SchedulingDecisionTestFramework.class);

	@Parameterized.Parameters(name = "scheduling via: {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{ new SpyableGeoScheduler(TestingUtils.defaultExecutor()) }, { new SpyableFlinkScheduler(TestingUtils.defaultExecutor()) }
		});
	}

	@Parameterized.Parameter(0)
	public Scheduler scheduler;

	/** @return the JobGraph to schedule */
	protected abstract JobGraph jobGraph();

	/** @return the Set of Instance to schedule on */
	protected abstract Set<Instance> instanceSet();

	/** @return the vertices that are already placed in a geolocation */
	protected Map<JobVertex, GeoLocation> placedVertices() {
		return null;
	}

	@Test
	public void test() throws Exception {

		for (Instance i : instanceSet()) {
			scheduler.newInstanceAvailable(i);
		}

		ExecutionGraph executionGraph = makeExecutionGraph(
			jobGraph().getVerticesSortedTopologicallyFromSources().toArray(new JobVertex[0]),
			log,
			scheduler,
			placedVertices(),
			null);


		SchedulingDecisionSpy spy = new SchedulingDecisionSpy(executionGraph, placedVertices());

		((SpyableScheduler) scheduler).addSchedulingDecisionSpy(spy);

		executionGraph.setScheduleMode(ScheduleMode.EAGER);
		executionGraph.scheduleForExecution();

		for (Instance i : scheduler.getInstancesByHost().values().stream().reduce((a, b) -> {
			a.addAll(b);
			return a;
		}).orElse(new ArrayList<>())) {
			System.out.println("instance: " + i.getId() + " at " + i.getTaskManagerLocation().getGeoLocation());
			System.out.println("free slots: " + i.getNumberOfAvailableSlots());
			System.out.println("allocated slots: " + i.getNumberOfAllocatedSlots());
			System.out.println();
		}
		System.out.println("network cost: " + spy.calculateNetworkCost());
		System.out.println("execution speed: " + spy.calculateExecutionSpeed());
		System.out.println("\n\n");
		System.out.println(spy.getAssignementsString());
	}
}
