package testingFrameworks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GeoSchedulerTestingUtilsOptions;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.StaticBandwidthProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.types.TwoKeysMap;
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
import writableTypes.TestGeoLocationAndBandwidths;

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

	public Configuration makeLocationSlotConfiguration(Map<GeoLocation, Integer> geoLocationSlotMap) {
		Configuration configuration = new Configuration();
		int taskManagerIndex = 0;
		int totalSlots = 0;
		for (Map.Entry<GeoLocation, Integer> locationAndSlots : geoLocationSlotMap.entrySet()) {
			int taskManager1Slots = locationAndSlots.getValue() / 2;
			int taskManager2Slots = locationAndSlots.getValue() - taskManager1Slots;

			configuration.setInteger(GeoSchedulerTestingUtilsOptions.slotsForTaskManagerAtIndex(taskManagerIndex), taskManager1Slots);
			configuration.setString(GeoSchedulerTestingUtilsOptions.geoLocationForTaskManagerAtIndex(taskManagerIndex), locationAndSlots.getKey().getKey());
			taskManagerIndex++;
			totalSlots += taskManager1Slots;

			if(taskManager2Slots > 0) {
				configuration.setInteger(GeoSchedulerTestingUtilsOptions.slotsForTaskManagerAtIndex(taskManagerIndex), taskManager2Slots);
				configuration.setString(GeoSchedulerTestingUtilsOptions.geoLocationForTaskManagerAtIndex(taskManagerIndex), locationAndSlots.getKey().getKey());
				taskManagerIndex++;
				totalSlots += taskManager2Slots;
			}

		}
		this.numberSlotsPerTaskManager = totalSlots / taskManagerIndex;
		this.numberTaskManagers = taskManagerIndex;
		return configuration;
	}

	@Parameterized.Parameter(0)
	public SpyableScheduler scheduler;

	@Parameterized.Parameter(1)
	public MiniClusterResource.MiniClusterType miniClusterType;

	public int numberTaskManagers;

	public int numberSlotsPerTaskManager;

	public String jobName;

	public DataStreamSchedulingTestFramework() {
			 if(scheduler instanceof  SpyableGeoScheduler) {
			 ((SpyableGeoScheduler) scheduler).setBandwidthProvider(new StaticBandwidthProvider(getTestGeoLocationAndBandwidths().getBandwidths()));
		 } else if (scheduler instanceof  SpyableFlinkScheduler) {
			 ((SpyableFlinkScheduler) scheduler).setBandwidthProvider(new StaticBandwidthProvider(getTestGeoLocationAndBandwidths().getBandwidths()));
		 }
	}

	/**
	 * @return a map from a geo location name to the number of slots to provide in that location. For performance reasons,
	 * make this a getter of a cached field, instead of recalculating it.
	 */
	public abstract TestGeoLocationAndBandwidths getTestGeoLocationAndBandwidths();


	@Rule
	public SchedulerInjectingMiniClusterResource makeMiniClusterResource() {
		return new SchedulerInjectingMiniClusterResource(
			new MiniClusterResource.MiniClusterResourceConfiguration(makeLocationSlotConfiguration(getTestGeoLocationAndBandwidths().getGeoLocationSlotMap()), numberTaskManagers, numberSlotsPerTaskManager),
			scheduler,
			miniClusterType);
	}

	protected StreamExecutionEnvironment getEnvironment() {
		return StreamExecutionEnvironment.getExecutionEnvironment();
	}

	@After
	public void teardown() {
		if(scheduler.getSpies().size() != 1) {
			throw new RuntimeException("Should have 1 spy");
		}

		SchedulingDecisionSpy spy = scheduler.getSpies().iterator().next();

		if(spy.getGraphs().size() != 1) {
			throw new RuntimeException("Should have 1 graph");
		}

		ExecutionGraph executionGraph = spy.getGraphs().iterator().next();

		SchedulingTestFrameworkUtils.writeTestOutcome(executionGraph, spy, (Scheduler) scheduler, writer, jobName, getTestGeoLocationAndBandwidths().getClassNameString());
	}
}
