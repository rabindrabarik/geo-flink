package testingFrameworks;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.OptimisationModelParameters;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.StaticBandwidthProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.types.TwoKeysMultiMap;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spies.SchedulingDecisionSpy;
import spies.SpyableGeoScheduler;
import spies.SpyableScheduler;
import testOutputWriter.TestOutputImpl;
import testOutputWriter.TestOutputWriter;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.Map;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.makeExecutionGraph;
import static testingFrameworks.SchedulingTestFrameworkUtils.writeTestOutcome;

@RunWith(Parameterized.class)
@Ignore
public abstract class ModelRuntimeTestFramework {
	private final static Logger log = LoggerFactory.getLogger(ModelRuntimeTestFramework.class);

	protected final TestOutputWriter<TestOutputImpl> writer = new TestOutputWriter<>(this.getClass().getSimpleName() + ".csv");

	protected SchedulingDecisionSpy spy;

	protected abstract TestJobGraph jobGraph();

	protected abstract TestInstanceSet instanceSet();

	/**
	 * @return the vertices that are already placed in a geolocation
	 */
	protected abstract Map<JobVertex, GeoLocation> placedVertices();

	private ExecutionGraph executionGraph;

	private Scheduler scheduler;

	@Before
	public void setup() {
		LogManager.getRootLogger().setLevel(Level.OFF);
		scheduler = new SpyableGeoScheduler(TestingUtils.defaultExecutor());

		SpyableScheduler spyableScheduler = ((SpyableScheduler) scheduler);

		for (Instance i : instanceSet().getInstances()) {
			scheduler.newInstanceAvailable(i);
		}

		jobGraph().getJobGraph().setOptimisationModelParameters(new OptimisationModelParameters(0.5, 0.5, 10, true));


		spy = new SchedulingDecisionSpy();
		spy.addPlacedVertices(placedVertices());
		spyableScheduler.addSchedulingDecisionSpy(spy);
	}

	@Test
	public void test() throws Exception {
		if (scheduler instanceof GeoScheduler) {
			jobGraph().getJobGraph().solveOptimisationModel(new StaticBandwidthProvider(new TwoKeysMultiMap<>()), ((GeoScheduler) scheduler).calculateAvailableSlotsByGeoLocation());
		}

		if (scheduler instanceof SpyableGeoScheduler) {
			((SpyableGeoScheduler) scheduler).addGraphSolution(jobGraph().getJobGraph().getJobID(), jobGraph().getJobGraph().getSolution());
		}
	}

	@After
	public void teardown() {
		writeTestOutcome(jobGraph().getJobGraph().getJobID(), spy, scheduler, writer, jobGraph().getClassNameString(), instanceSet().getClassNameString());
	}
}
