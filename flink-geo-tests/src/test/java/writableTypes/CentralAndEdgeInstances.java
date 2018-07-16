package writableTypes;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.instance.AckingDummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CentralAndEdgeInstances extends TestInstanceSet {
	private final Set<Instance> instanceSet = new HashSet<>();
	private final Map<GeoLocation, Instance> edgeClouds = new HashMap<>();
	private final Instance centralCloud;

	public CentralAndEdgeInstances(int numEdgeClouds, int centralSlots, int eachEdgeSlots) {
		params = new Object[3];
		params[0] = numEdgeClouds;
		params[1] = centralSlots;
		params[2] = eachEdgeSlots;

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

	public Set<Instance> getInstances() {
		return instanceSet;
	}

	public Map<GeoLocation, Instance> getEdgeClouds() {
		return edgeClouds;
	}

	public Instance getCentralCloud() {
		return centralCloud;
	}
}
