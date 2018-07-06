package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.OptimisationProblemSolution;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.taskmanager.GeoTaskManagerLocation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * This scheduler allows Flink to make better allocation decisions when the task managers are executed in geo-distributed data centers
 */
public class GeoScheduler extends Scheduler {

	private Map<GeoLocation, Set<Instance>> allInstancesByGeoLocation = new HashMap<>();
	private Map<ExecutionGraph, OptimisationProblemSolution> solutions = new HashMap<>();

	/**
	 * Creates a new scheduler.
	 *
	 * @param executor            the executor to run futures on
	 */
	public GeoScheduler(Executor executor) {
		super(executor);
	}

	@Override
	public void newInstanceAvailable(Instance instance) {
		super.newInstanceAvailable(instance);

		GeoLocation instanceLocation = getInstanceLocation(instance);

		Set<Instance> instanceSet = allInstancesByGeoLocation.getOrDefault(instanceLocation, new HashSet<>());
		instanceSet.add(instance);
		allInstancesByGeoLocation.put(instanceLocation, instanceSet);
	}

	@Override
	public void instanceDied(Instance instance) {
		if (instance == null) {
			throw new NullPointerException();
		}

		GeoLocation instanceLocation = getInstanceLocation(instance);

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
		allInstancesByGeoLocation.clear();
		super.shutdown();
	}

	private Iterable<GeoLocation> getAllGeoLocations() {
		return this.allInstancesByGeoLocation.keySet();
	}

	private GeoLocation getInstanceLocation(Instance instance) {
		if (instance.getTaskManagerLocation() instanceof GeoTaskManagerLocation) {
			return ((GeoTaskManagerLocation) instance.getTaskManagerLocation()).getGeoLocation();
		} else {
			//TODO: maybe throw exception here?
			return GeoLocation.UNKNOWN;
		}
	}

	/**
	 * Note that, due to asynchrony, the value returned may not be completely accurate.
	 */
	public Map<GeoLocation, Integer> calculateAvailableSlotsByGeoLocation() {
		Map<GeoLocation, Integer> out = new HashMap<>();

		for (Map.Entry<GeoLocation, Set<Instance>> locationAndInstances : allInstancesByGeoLocation.entrySet()) {
			int slots = 0;

			for (Instance instance : locationAndInstances.getValue()) {
				slots += instance.getNumberOfAvailableSlots();
			}

			out.put(locationAndInstances.getKey(), slots);
		}

		return out;
	}


	public Map<GeoLocation, Set<Instance>> getAllInstancesByGeoLocation() {
		return allInstancesByGeoLocation;
	}


	public void provideGraphSolution(ExecutionGraph executionGraph, OptimisationProblemSolution solution) {
		solutions.put(executionGraph, solution);
	}
}
