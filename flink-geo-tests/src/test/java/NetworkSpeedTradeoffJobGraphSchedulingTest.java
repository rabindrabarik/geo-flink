import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import testingFrameworks.JobGraphSchedulingTestFramework;
import writableTypes.CentralAndEdgeInstances;
import writableTypes.SimpleJobGraph;
import writableTypes.TestInstanceSet;
import writableTypes.TestJobGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NetworkSpeedTradeoffJobGraphSchedulingTest extends JobGraphSchedulingTestFramework {
	private final SimpleJobGraph jobGraph = new SimpleJobGraph(4);
	private final CentralAndEdgeInstances instances = new CentralAndEdgeInstances(4, 20, 2);
	private final Map<JobVertex, GeoLocation> placedVertices = new HashMap<>();

	public NetworkSpeedTradeoffJobGraphSchedulingTest() {
		fillPlacedVertices();
	}

	private void fillPlacedVertices() {
		Iterator<GeoLocation> geoLocationIterator = instances.getEdgeClouds().keySet().iterator();

		List<JobVertex> inputs = jobGraph.getInputs();
		for (int i = inputs.size() - 1; i >= 0; i--) {
			JobVertex input = inputs.get(i);
			if (geoLocationIterator.hasNext()) {
				placedVertices.put(input, geoLocationIterator.next());
			}
		}
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
		return placedVertices;
	}
}
