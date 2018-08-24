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
	private final static int MAX_EDGE_CLOUDS = 700;

	private static int initialEdgeClouds = 4;
	private static int edgeCloudsIncrement = 5;

	private static int initialCentralSlots = 4;

	private static int initialEachEdgeSlots = 4;

	private static int initialMapTasks = 4;

	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} parallelism: {2} ")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		for (int test = 0; test < MAX_EDGE_CLOUDS / edgeCloudsIncrement; test++) {

			Object[] params = new Object[3];

			params[0] = true;
			params[1] = initialEdgeClouds + test * edgeCloudsIncrement;
			params[2] = ((int) params[1] * initialEachEdgeSlots + initialCentralSlots) / initialMapTasks;

			data.add(params);
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
