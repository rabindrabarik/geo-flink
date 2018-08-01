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
	private final static int NUM_TESTS = 200;

	private static int initialEdgeClouds = 4;
	private static int[] edgeCloudsIncrement = {1, 5};

	private static int initialCentralSlots = 4;
	private static int[] centralSlotsIncrement = {5, 10};

	private static int initialEachEdgeSlots = 4;
	private static int[] eachEdgeSlotsIncrement = {1, 20};

	private static int initialMapTasks = 4;

	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} centralSlots: {2} eachEdgeSlots: {3} mapTasks: {4}")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		for (int test = 0; test < NUM_TESTS; test++) {
			int hundredsIndex = test / 100;
			int index = test - hundredsIndex * 100;

			Object[] params = new Object[5];

			params[0] = true;
			params[1] = initialEdgeClouds + index * edgeCloudsIncrement[hundredsIndex];
			params[2] = initialCentralSlots + index * centralSlotsIncrement[hundredsIndex];
			params[3] = initialEachEdgeSlots + index * eachEdgeSlotsIncrement[hundredsIndex];
			params[4] = initialMapTasks + index * ((int) params[1] * edgeCloudsIncrement[test / 100] + ((int) params[1] + edgeCloudsIncrement[test / 100]));

			data.add(params);

			params = new Object[5];

			params[0] = false;
			params[1] = initialEdgeClouds + index * edgeCloudsIncrement[hundredsIndex];
			params[2] = initialCentralSlots + index * centralSlotsIncrement[hundredsIndex];
			params[3] = initialEachEdgeSlots + index * eachEdgeSlotsIncrement[hundredsIndex];
			params[4] = initialMapTasks + index * ((int) params[1] * edgeCloudsIncrement[test / 100] + ((int) params[1] + edgeCloudsIncrement[test / 100]));

			data.add(params);
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
		jobGraph = new SimpleJobGraph(mapTasks);
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
