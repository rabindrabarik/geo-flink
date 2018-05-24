package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.taskmanager.GeoTaskManagerLocation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * This scheduler allows Flink to make better allocation decisions when the task managers are executed in geo-distributed data centers
 * */
public class GeoScheduler extends Scheduler {

	private Map<GeoLocation, Set<Instance>> allInstancesByGeoLocation = new HashMap<>();

	/**
	 * Creates a new scheduler.
	 *
	 * @param executor
	 */
	public GeoScheduler(Executor executor) {
		super(executor);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time timeout) {
		return null;
	}

	@Override
	public void newInstanceAvailable(Instance instance) {
		super.newInstanceAvailable(instance);

		GeoLocation instanceLocation;

		if(instance.getTaskManagerLocation() instanceof GeoTaskManagerLocation) {
			instanceLocation = ((GeoTaskManagerLocation) instance.getTaskManagerLocation()).getGeoLocation();
		} else {
			instanceLocation = GeoLocation.UNKNOWN;
		}

		Set<Instance> instanceSet = allInstancesByGeoLocation.getOrDefault(instanceLocation, new HashSet<>());
		instanceSet.add(instance);
		allInstancesByGeoLocation.put(instanceLocation, instanceSet);
	}

	@Override
	public void instanceDied(Instance instance) {
		if(instance == null) {
			throw new NullPointerException();
		}

		GeoLocation instanceLocation;
		if(instance.getTaskManagerLocation() instanceof GeoTaskManagerLocation) {
			instanceLocation = ((GeoTaskManagerLocation) instance.getTaskManagerLocation()).getGeoLocation();
		} else {
			instanceLocation = GeoLocation.UNKNOWN;
		}

		Set<Instance> instanceSet = allInstancesByGeoLocation.get(instanceLocation);
		if (instanceSet != null) {
			instanceSet.remove(instance);
			if (instanceSet.isEmpty()) {
				allInstancesByGeoLocation.remove(instanceLocation);
			}
		}

		super.instanceDied(instance);
	}

	@Override
	public void shutdown() {
		super.shutdown();
		allInstancesByGeoLocation.clear();
	}
}
