package loopTests;

import org.junit.Before;
import org.junit.runners.Parameterized;
import testingFrameworks.JobGraphSchedulingTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.ArrayList;
import java.util.Collection;

public class IncreasingTasksAndSlotsJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {
	private final static int NUM_TESTS = 150;

	private static int initialEdgeClouds = 4;

	private static int initialCentralSlots = 4;
	private static int[] centralSlotsIncrement = {1, 5};

	private static int initialEachEdgeSlots = 4;
	private static int[] eachEdgeSlotsIncrement = {2, 10};


	private static int initialMapTasks = 4;
	private static int[] mapTasksIncrement = {2,10};


	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} centralSlots: {2} eachEdgeSlots: {3} mapTasks: {4}")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		for (int test = 0; test < NUM_TESTS; test++) {
			int hundredsIndex = test / 100;
			int index = test - hundredsIndex * 100;

			Object[] params = new Object[5];

			params[0] = true;
			params[1] = initialEdgeClouds;
			params[2] = initialCentralSlots + index * centralSlotsIncrement[hundredsIndex];
			params[3] = initialEachEdgeSlots + index * eachEdgeSlotsIncrement[hundredsIndex];
			params[4] = initialMapTasks + index * mapTasksIncrement[hundredsIndex];

			data.add(params);

			params = new Object[5];

			params[0] = false;
			params[1] = initialEdgeClouds;
			params[2] = initialCentralSlots + index * centralSlotsIncrement[hundredsIndex];
			params[3] = initialEachEdgeSlots + index * eachEdgeSlotsIncrement[hundredsIndex];
			params[4] = initialMapTasks + index * mapTasksIncrement[hundredsIndex];

			data.add(params);

			if((test + 1) % 100 == 0) {
				initialCentralSlots = (int) params[2];
				initialEachEdgeSlots = (int) params[3];
				initialMapTasks = (int) params[4];
			}
		}

		return data;
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
