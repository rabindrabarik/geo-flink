package writableTypes;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.instance.AckingDummyActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;

import java.util.HashSet;
import java.util.Set;

public class DistributedInstances extends TestInstanceSet {
	private Set<Instance> instanceSet = new HashSet<>();

	public DistributedInstances(int howMany) {
		this(howMany, 4);
	}

	public DistributedInstances(int howMany, int numSlots) {
		params = new Object[2];
		params[0] = howMany;
		params[1] = numSlots;
		for(int i = 0; i < howMany; i ++) {
			instanceSet.add(SchedulerTestUtils.getRandomInstance(numSlots, AckingDummyActorGateway.INSTANCE, new GeoLocation("location_" + i)));
		}
	}

	@Override
	public Set<Instance> getInstances() {
		return instanceSet;
	}
}
