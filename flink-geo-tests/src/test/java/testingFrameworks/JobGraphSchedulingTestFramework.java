package testingFrameworks;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spies.SchedulingDecisionSpy;
import spies.SpyableFlinkScheduler;
import spies.SpyableGeoScheduler;
import spies.SpyableScheduler;
import testOutputWriter.SchedulingDecision;
import testOutputWriter.TestOutputWriter;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.makeExecutionGraph;

/**
 * This tests logs the slot usages after scheduling a {@link TestJobGraph}.
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class JobGraphSchedulingTestFramework extends TestLogger {

	private final static Logger log = LoggerFactory.getLogger(JobGraphSchedulingTestFramework.class);

	private final static TestOutputWriter writer = new TestOutputWriter();

	@Parameterized.Parameters(name = "scheduling via: {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{new SpyableGeoScheduler(TestingUtils.defaultExecutor())}, {new SpyableFlinkScheduler(TestingUtils.defaultExecutor())}
		});
	}

	@Parameterized.Parameter(0)
	public Scheduler scheduler;

	/**
	 * @return the JobGraph to schedule
	 */
	protected abstract TestJobGraph jobGraph();

	/**
	 * @return the Set of Instance to schedule on
	 */
	protected abstract TestInstanceSet instanceSet();

	/**
	 * @return the vertices that are already placed in a geolocation
	 */
	protected Map<JobVertex, GeoLocation> placedVertices() {
		return null;
	}

	@Test
	public void test() throws Exception {

		long initialTime = System.currentTimeMillis();

		SpyableScheduler spyableScheduler = ((SpyableScheduler) scheduler);

		for (Instance i : instanceSet().getInstances()) {
			scheduler.newInstanceAvailable(i);
		}

		ExecutionGraph executionGraph = makeExecutionGraph(
			jobGraph().getJobGraph().getVerticesSortedTopologicallyFromSources().toArray(new JobVertex[0]),
			log,
			scheduler,
			placedVertices(),
			null);


		SchedulingDecisionSpy spy = new SchedulingDecisionSpy(executionGraph, placedVertices());

		spyableScheduler.addSchedulingDecisionSpy(spy);

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

		double networkCost = spy.calculateNetworkCost();
		double executionSpeed = spy.calculateExecutionSpeed();

		System.out.println("network cost: " + networkCost);
		System.out.println("execution speed: " + executionSpeed);
		System.out.println("\n\n");
		System.out.println(spy.getAssignementsString());

		writer.write(new SchedulingDecision(
			networkCost,
			executionSpeed,
			scheduler.getClass().getSimpleName(),
			jobGraph().getClassNameString(),
			instanceSet().getClassNameString(),
			System.currentTimeMillis() - initialTime));
	}
}
