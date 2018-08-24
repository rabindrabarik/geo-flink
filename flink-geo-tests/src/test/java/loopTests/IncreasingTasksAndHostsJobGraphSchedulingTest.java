package loopTests;

import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Before;
import org.junit.runners.Parameterized;
import spies.SpyableFlinkScheduler;
import spies.SpyableGeoScheduler;
import testingFrameworks.JobGraphSchedulingTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.ArrayList;
import java.util.Collection;

public class IncreasingTasksAndHostsJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {
	//remember this is times numberOfMapTasksIncrements
	private final static int NUM_TESTS = 100;

	private static int initialEdgeClouds = 4;
	private static int edgeCloudsIncrement = 5;

	private static int initialCentralSlots = 4;
	private static int  centralSlotsIncrement = 10;

	private static int initialEachEdgeSlots = 4;
	private static int eachEdgeSlotsIncrement = 20;

	private static int numberOfMapTasksIncrements = 15;

	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} centralSlots: {2} eachEdgeSlots: {3} mapTasks: {4}")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		int hundredsIndex = 0;

		for (int test = 0; test < NUM_TESTS; test++) {

			Object[] params = new Object[5];

			//adding slots and geoscheduling
			params[0] = true;
			params[1] = initialEdgeClouds + test * edgeCloudsIncrement;
			params[2] = initialCentralSlots + test * centralSlotsIncrement;
			params[3] = initialEachEdgeSlots + test * eachEdgeSlotsIncrement;

			//max map tasks a bit more than locations
			int maxMapTasks = (int) ((int) params[1] * 2);

			//map tasks increment (min 1)
			int mapTasksIncrement = maxMapTasks / numberOfMapTasksIncrements;
			if(mapTasksIncrement == 0) {
				mapTasksIncrement = 1;
			}

			//adds to data all the tests with increasing map tasks
			addAllMapTasks(data, params, maxMapTasks, mapTasksIncrement);

			//adds the last test
			params[4] = maxMapTasks;
			data.add(params);
		}

		return data;
	}

	private static void addAllMapTasks(Collection<Object[]> data, Object[] params, int maxMapTasks, int mapTasksIncrement) {
		for(int mapTasks = mapTasksIncrement; mapTasks < maxMapTasks; mapTasks += mapTasksIncrement ) {
			Object[] paramsClone = params.clone();
			paramsClone[4] = mapTasks;
			data.add(paramsClone);
		}
	}

	@Parameterized.Parameter(1)
	public int edgeClouds;

	@Parameterized.Parameter(2)
	public int centralSlots;

	@Parameterized.Parameter(3)
	public int eachEdgeSlots;

	@Parameterized.Parameter(4)
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
}
