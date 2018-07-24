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
	private final CentralAndEdgeInstances instances = new CentralAndEdgeInstances(4, 10, 20);
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
				GeoLocation location = geoLocationIterator.next();
				input.setGeoLocationKey(location.getKey());
				placedVertices.put(input, location);
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
