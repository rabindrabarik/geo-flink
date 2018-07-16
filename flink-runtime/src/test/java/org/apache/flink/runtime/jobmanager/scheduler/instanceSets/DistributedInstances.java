package org.apache.flink.runtime.jobmanager.scheduler.instanceSets;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.instance.AckingDummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;

import java.util.HashSet;
import java.util.Set;

public class DistributedInstances extends InstanceSet {
	private Set<Instance> instanceSet = new HashSet<>();

	public DistributedInstances(int howMany) {
		params = new Object[1];
		params[0] = howMany;
		for(int i = 0; i < howMany; i ++) {
			instanceSet.add(SchedulerTestUtils.getRandomInstance(4, AckingDummyActorGateway.INSTANCE, new GeoLocation("location_" + i)));
		}
	}

	@Override
	public Set<Instance> getInstances() {
		return instanceSet;
	}
}
