package layeredTests;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.junit.Before;
import org.junit.runners.Parameterized;
import testingFrameworks.ModelRuntimeTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IncreasingHostsStepTasksModelRuntimeTest extends ModelRuntimeTestFramework {
	private final static int MAX_EDGE_CLOUDS = 700;

	private final static int[] MAP_TASKS_VALUES = {10, 50, 100, 500, 700};

	private static int initialEdgeClouds = 4;
	private static int edgeCloudsIncrement = 5;

	private static int initialCentralSlots = 4;

	private static int initialEachEdgeSlots = 4;

	@Parameterized.Parameters(name = " edgeClouds: {0} parallelism: {1} mapTasks: {2}")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		for (int mapTasks : MAP_TASKS_VALUES) {
			for (int test = 0; test < MAX_EDGE_CLOUDS / edgeCloudsIncrement; test++) {
				Object[] params = new Object[3];

				params[0] = initialEdgeClouds + test * edgeCloudsIncrement;
				params[1] = ((int) params[0] * initialEachEdgeSlots + initialCentralSlots) / mapTasks;
				params[2] = mapTasks;

				data.add(params);
			}
		}

		return data;
	}

	@Parameterized.Parameter(0)
	public int edgeClouds;

	@Parameterized.Parameter(1)
	public int maxParallelism;

	@Parameterized.Parameter(2)
	public int mapTasks;

	public CentralAndEdgeInstances instances;

	public SimpleJobGraph jobGraph;

	@Override
	@Before
	public void setup() {
		instances = new CentralAndEdgeInstances(edgeClouds, initialCentralSlots, initialEachEdgeSlots);
		jobGraph = new SimpleJobGraph(mapTasks, maxParallelism < 1 ? 1 : maxParallelism);
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

	@Override
	protected Map<JobVertex, GeoLocation> placedVertices() {
		return new HashMap<>();
	}

}
