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

public class IncreasingParallelismAndHostsJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {
	private final static int NUM_TESTS = 150;

	private static int initialEdgeClouds = 4;
	private static int[] edgeCloudsIncrement = {1, 5};

	private static int initialCentralSlots = 4;

	private static int initialEachEdgeSlots = 4;

	private static int initialMapTasks = 4;

	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} parallelism: {2} ")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		for (int test = 0; test < NUM_TESTS; test++) {
			int hundredsIndex = test / 100;
			int index = test - hundredsIndex * 100;

			Object[] params = new Object[3];

			params[0] = true;
			params[1] = initialEdgeClouds + index * edgeCloudsIncrement[hundredsIndex];
			params[2] = ((int) params[1] * initialEachEdgeSlots + initialCentralSlots) / initialMapTasks;

			data.add(params);

			params = new Object[3];

			params[0] = false;
			params[1] = initialEdgeClouds + index * edgeCloudsIncrement[hundredsIndex];
			params[2] = ((int) params[1] * initialEachEdgeSlots + initialCentralSlots) / initialMapTasks;

			data.add(params);

			if((test + 1) % 100 == 0) {
				initialEdgeClouds = (int) params[1];
			}
		}

		return data;
	}

	@Parameterized.Parameter(1)
	public int edgeClouds;

	@Parameterized.Parameter(2)
	public int maxParallelism;



	public CentralAndEdgeInstances instances;

	public SimpleJobGraph jobGraph;

	@Override
	@Before
	public void setup() {
		instances = new CentralAndEdgeInstances(edgeClouds, initialCentralSlots, initialEachEdgeSlots);
		jobGraph = new SimpleJobGraph(initialMapTasks, maxParallelism < 1 ? 1 : maxParallelism);
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
