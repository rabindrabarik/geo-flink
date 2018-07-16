package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.instanceSets.CentralAndEdgeInstanceSet;
import org.apache.flink.runtime.jobmanager.scheduler.instanceSets.InstanceSet;
import org.apache.flink.runtime.jobmanager.scheduler.schedulingDecisionFramework.SchedulingDecisionTestFramework;
import org.apache.flink.runtime.jobmanager.scheduler.testJobGraphs.SimpleJobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.testJobGraphs.TestJobGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NetworkSpeedTradeoffSchedulingDecisionTest extends SchedulingDecisionTestFramework {
	private final SimpleJobGraph jobGraph = new SimpleJobGraph(4);
	private final CentralAndEdgeInstanceSet instances = new CentralAndEdgeInstanceSet(4, 20, 2);
	private final Map<JobVertex, GeoLocation> placedVertices = new HashMap<>();

	public NetworkSpeedTradeoffSchedulingDecisionTest() {
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
	protected InstanceSet instanceSet() {
		return instances;
	}

	@Override
	protected Map<JobVertex, GeoLocation> placedVertices() {
		return placedVertices;
	}
}
