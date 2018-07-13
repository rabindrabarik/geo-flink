package org.apache.flink.runtime.jobmanager.scheduler.TestInstanceSets;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.instance.AckingDummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CentralAndEdgeInstanceSet {
	private final Set<Instance> instanceSet = new HashSet<>();
	private final Map<GeoLocation, Instance> edgeClouds = new HashMap<>();
	private final Instance centralCloud;

	public CentralAndEdgeInstanceSet(int numEdgeClouds, int centralSlots, int eachEdgeSlots) {
		for (int i = 0; i < numEdgeClouds; i++) {
			GeoLocation edgeLocation = new GeoLocation("edge_cloud_" + i);
			Instance edgeInstance = SchedulerTestUtils.getRandomInstance(eachEdgeSlots, AckingDummyActorGateway.INSTANCE, edgeLocation);
			instanceSet.add(edgeInstance);
			edgeClouds.put(edgeLocation, edgeInstance);
		}
		Instance centralInstance = SchedulerTestUtils.getRandomInstance(centralSlots, AckingDummyActorGateway.INSTANCE, new GeoLocation("central_cloud"));
		instanceSet.add(centralInstance);
		centralCloud = centralInstance;
	}

	public Set<Instance> getInstanceSet() {
		return instanceSet;
	}

	public Map<GeoLocation, Instance> getEdgeClouds() {
		return edgeClouds;
	}

	public Instance getCentralCloud() {
		return centralCloud;
	}
}
