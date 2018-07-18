package testingFrameworks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GeoSchedulerTestingUtilsOptions;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * This tests logs the slot usages after scheduling a DataStream
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class DataStreamSchedulingTestFramework extends TestLogger {

	private final static Logger log = LoggerFactory.getLogger(DataStreamSchedulingTestFramework.class);

	private final TestOutputWriter<TestOutputImpl> writer = new TestOutputWriter<>(this.getClass().getSimpleName() + ".csv");

	@Parameterized.Parameters(name = "scheduling via: {0}")
	public static Collection<Object[]> data() {
		SpyableGeoScheduler spyableGeoScheduler = new SpyableGeoScheduler(TestingUtils.defaultExecutor(), new SchedulingDecisionSpy());
		SpyableFlinkScheduler spyableFlinkScheduler = new SpyableFlinkScheduler(TestingUtils.defaultExecutor(), new SchedulingDecisionSpy());

		return Arrays.asList(new Object[][]{
			{spyableGeoScheduler, MiniClusterResource.MiniClusterType.LEGACY_SPY_GEO},
			{spyableFlinkScheduler, MiniClusterResource.MiniClusterType.LEGACY_SPY_FLINK}
		});
	}

	public static Configuration makeLocationSlotConfiguration(Map<String, Integer> geoLocationSlotMap) {
		Configuration configuration = new Configuration();
		int taskManagerIndex = 0;
		for (Map.Entry<String, Integer> locationAndSlots : geoLocationSlotMap.entrySet()) {
			configuration.setInteger(GeoSchedulerTestingUtilsOptions.slotsForTaskManagerAtIndex(taskManagerIndex), locationAndSlots.getValue());
			configuration.setString(GeoSchedulerTestingUtilsOptions.geoLocationForTaskManagerAtIndex(taskManagerIndex), locationAndSlots.getKey());
			taskManagerIndex++;
		}
		return configuration;
	}

	public SpyableScheduler scheduler;

	public MiniClusterResource.MiniClusterType miniClusterType;

	public int numberTaskManagers;

	public int numberSlotsPerTaskManager;

	public SchedulerInjectingMiniClusterResource miniClusterResource;

	public String jobName;

	public String instanceSetName;

	public DataStreamSchedulingTestFramework(SpyableScheduler scheduler, MiniClusterResource.MiniClusterType miniClusterType) {
		this.scheduler = scheduler;
		this.miniClusterType = miniClusterType;
		this.numberSlotsPerTaskManager = getSlotAverage(getGeoLocationSlotMap());
		this.numberTaskManagers = getGeoLocationSlotMap().isEmpty() ? 1 : getGeoLocationSlotMap().size();
	}

	private int getSlotAverage(Map<String, Integer> geoLocationSlotMap) {
		int sum = 0;

		if(geoLocationSlotMap.isEmpty()) {
			return 1;
		}

		for (Integer slots : geoLocationSlotMap.values()) {
			sum += slots;
		}
		return sum / geoLocationSlotMap.size();
	}

	/**
	 * @return a map from a geo location name to the number of slots to provide in that location. For performance reasons,
	 * make this a getter of a cached field, instead of recalculating it.
	 */
	public abstract Map<String, Integer> getGeoLocationSlotMap();

	@Rule
	public SchedulerInjectingMiniClusterResource makeMiniClusterResource() {
		return new SchedulerInjectingMiniClusterResource(
			new MiniClusterResource.MiniClusterResourceConfiguration(makeLocationSlotConfiguration(getGeoLocationSlotMap()), numberTaskManagers, numberSlotsPerTaskManager),
			scheduler,
			miniClusterType);
	}

	protected TestEnvironment getEnvironment() {
		return miniClusterResource.getTestEnvironment();
	}

	@After
	public void teardown() {
		if(scheduler.getSpies().size() != 1) {
			throw new RuntimeException("Shouldn't have more than 1 spy");
		}

		SchedulingDecisionSpy spy = scheduler.getSpies().iterator().next();

		if(spy.getGraphs().size() != 1) {
			throw new RuntimeException("Shouldn't have more than 1 graph");
		}

		ExecutionGraph executionGraph = spy.getGraphs().iterator().next();

		SchedulingTestFrameworkUtils.writeTestOutcome(executionGraph, spy, (Scheduler) scheduler, writer, jobName, instanceSetName);
	}
}
