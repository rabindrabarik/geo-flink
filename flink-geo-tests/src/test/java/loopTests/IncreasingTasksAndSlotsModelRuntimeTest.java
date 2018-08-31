package loopTests;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.runners.Parameterized;
import testingFrameworks.JobGraphSchedulingTestFramework;
import testingFrameworks.ModelRuntimeTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IncreasingTasksAndSlotsModelRuntimeTest extends ModelRuntimeTestFramework {
	private final static int MAX_TASKS = 700;

	private static int initialEdgeClouds = 4;

	private static int initialCentralSlots = 4;
	private static int centralSlotsIncrement = 5;

	private static int initialEachEdgeSlots = 4;
	private static int eachEdgeSlotsIncrement = 10;


	private static int initialMapTasks = 4;
	private static int mapTasksIncrement = 10;


	@Parameterized.Parameters(name = " geoScheduling?: {0} edgeClouds: {1} centralSlots: {2} eachEdgeSlots: {3} mapTasks: {4}")
	public static Collection<Object[]> data() {
		Collection<Object[]> data = new ArrayList<>();

		for (int test = 0; test < MAX_TASKS / mapTasksIncrement; test++) {

			Object[] params = new Object[4];

			params[0] = initialEdgeClouds;
			params[1] = initialCentralSlots + test * centralSlotsIncrement;
			params[2] = initialEachEdgeSlots + test * eachEdgeSlotsIncrement;
			params[3] = initialMapTasks + test * mapTasksIncrement;

			data.add(params);
		}

		return data;
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
