package testingFrameworks;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
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
import testOutputWriter.TestOutputImpl;
import testOutputWriter.TestOutputWriter;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.makeExecutionGraph;
import static testingFrameworks.SchedulingTestFrameworkUtils.writeTestOutcome;

/**
 * This tests logs the slot usages after scheduling a {@link TestJobGraph}.
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class JobGraphSchedulingTestFramework extends TestLogger {

	private final static Logger log = LoggerFactory.getLogger(JobGraphSchedulingTestFramework.class);

	private final TestOutputWriter<TestOutputImpl> writer = new TestOutputWriter<>(this.getClass().getSimpleName() + ".csv");

	@Parameterized.Parameters(name = "scheduling via: {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{new SpyableGeoScheduler(TestingUtils.defaultExecutor())}, {new SpyableFlinkScheduler(TestingUtils.defaultExecutor())}
		});
	}

	@Parameterized.Parameter(0)
	public Scheduler scheduler;

	private ExecutionGraph executionGraph;
	private SchedulingDecisionSpy spy;

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
		return new HashMap<>();
	}

	@Before
	public void setup() {
		SpyableScheduler spyableScheduler = ((SpyableScheduler) scheduler);

		for (Instance i : instanceSet().getInstances()) {
			scheduler.newInstanceAvailable(i);
		}

		spy = new SchedulingDecisionSpy();
		spy.addPlacedVertices(placedVertices());
		spyableScheduler.addSchedulingDecisionSpy(spy);
	}

	@Test
	public void test() throws Exception {
		executionGraph = makeExecutionGraph(
			jobGraph().getJobGraph().getVerticesSortedTopologicallyFromSources().toArray(new JobVertex[0]),
			log,
			scheduler,
			placedVertices(),
			null);


		executionGraph.setScheduleMode(ScheduleMode.EAGER);
		executionGraph.scheduleForExecution();

	}

	@After
	public void teardown() {
		writeTestOutcome(executionGraph, spy, scheduler, writer, jobGraph().getClassNameString(), instanceSet().getClassNameString());
	}


}
