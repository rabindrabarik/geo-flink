package loopTests;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
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

public class IncreasingParallelismAndHostsModelRuntimeTest extends ModelRuntimeTestFramework {
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

			Object[] params = new Object[2];

			params[0] = initialEdgeClouds + test * edgeCloudsIncrement;
			params[1] = ((int) params[0] * initialEachEdgeSlots + initialCentralSlots) / initialMapTasks;

			data.add(params);
		}

		return data;
	}

	@Parameterized.Parameter(0)
	public int edgeClouds;

	@Parameterized.Parameter(1)
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

	@Override
	protected Map<JobVertex, GeoLocation> placedVertices() {
		return new HashMap<>();
	}

}
