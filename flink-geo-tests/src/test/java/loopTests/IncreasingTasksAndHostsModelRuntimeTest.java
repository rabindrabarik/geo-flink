package loopTests;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.StaticBandwidthProvider;
import org.apache.flink.types.TwoKeysMultiMap;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import spies.SchedulingDecisionSpy;
import testOutputWriter.TestOutputImpl;
import testOutputWriter.TestOutputWriter;
import testingFrameworks.ModelRuntimeTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.makeExecutionGraph;
import static testingFrameworks.SchedulingTestFrameworkUtils.writeTestOutcome;

public class IncreasingTasksAndHostsModelRuntimeTest extends ModelRuntimeTestFramework {
	//remember this is times numberOfMapTasksIncrements
	private final static int NUM_TESTS = 66;

	private static int initialEdgeClouds = 150;
	private static int edgeCloudsIncrement = 5;

	private static int initialCentralSlots = 192;
	private static int centralSlotsIncrement = 10;

	private static int initialEachEdgeSlots = 584;
	private static int eachEdgeSlotsIncrement = 20;

	private static int numberOfMapTasksIncrements = 3;

	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} centralSlots: {2} eachEdgeSlots: {3} mapTasks: {4}")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		int hundredsIndex = 0;
		for (int i = 0; i < 1; i++) {
			for (int test = 0; test < NUM_TESTS; test++) {

				Object[] params = new Object[4];

				//adding slots and geoscheduling
				params[0] = initialEdgeClouds + test * edgeCloudsIncrement;
				params[1] = initialCentralSlots + test * centralSlotsIncrement;
				params[2] = initialEachEdgeSlots + test * eachEdgeSlotsIncrement;

				//max map tasks a bit more than locations
				int maxMapTasks = ((int) params[0] * 2);

				//map tasks increment (min 1)
				int mapTasksIncrement = maxMapTasks / numberOfMapTasksIncrements;
				if (mapTasksIncrement == 0) {
					mapTasksIncrement = 1;
				}

				//adds to data all the tests with increasing map tasks
				addAllMapTasks(data, params, maxMapTasks, mapTasksIncrement);

				//adds the last test
				params[3] = maxMapTasks;
				data.add(params);
			}
		}

		return data;
	}

	private static void addAllMapTasks(Collection<Object[]> data, Object[] params, int maxMapTasks, int mapTasksIncrement) {
		for (int mapTasks = mapTasksIncrement; mapTasks < maxMapTasks; mapTasks += mapTasksIncrement) {
			Object[] paramsClone = params.clone();
			paramsClone[3] = mapTasks;
			data.add(paramsClone);
		}
	}

	@Parameterized.Parameter(0)
	public int edgeClouds;

	@Parameterized.Parameter(1)
	public int centralSlots;

	@Parameterized.Parameter(2)
	public int eachEdgeSlots;

	@Parameterized.Parameter(3)
	public int mapTasks;

	public CentralAndEdgeInstances instances;

	public SimpleJobGraph jobGraph;

	@Override
	@Before
	public void setup() {
		instances = new CentralAndEdgeInstances(edgeClouds, centralSlots, eachEdgeSlots);
		int parallelism = (edgeClouds * eachEdgeSlots + centralSlots) / mapTasks;
		jobGraph = new SimpleJobGraph(mapTasks, parallelism);
		super.setup();
	}


	@Override
	protected TestJobGraph jobGraph() {
		return jobGraph;
	}

	@Override
	protected TestInstanceSet instanceSet() {
		return instances;
	}

	/**
	 * @return the vertices that are already placed in a geolocation
	 */
	@Override
	protected Map<JobVertex, GeoLocation> placedVertices() {
		return new HashMap<>();
	}

}
