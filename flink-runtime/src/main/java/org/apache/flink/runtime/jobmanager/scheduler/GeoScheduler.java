package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.OptimisationModelSolution;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * This scheduler allows Flink to make better allocation decisions when the task managers are executed in geo-distributed data centers
 */
public class GeoScheduler extends Scheduler {

	private Map<GeoLocation, Set<Instance>> allInstancesByGeoLocation = new HashMap<>();
	private Map<ExecutionGraph, OptimisationModelSolution> solutions = new HashMap<>();
	private BandwidthProvider bandwidthProvider;

	/**
	 * Creates a new scheduler.
	 *
	 * @param executor            the executor to run futures on
	 */
	public GeoScheduler(Executor executor) {
		super(executor);
	}

	/**
	 * Creates a new scheduler.
	 *
	 * @param executor            the executor to run futures on
	 * @param bandwidthProvider	  the bandwidth provider that will tell this scheduler the bandwidths between
	 *                            {@link GeoLocation}
	 */
	public GeoScheduler(Executor executor, BandwidthProvider bandwidthProvider) {
		super(executor);
		this.bandwidthProvider = bandwidthProvider;
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
			return instance.getTaskManagerLocation().getGeoLocation();
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

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time allocationTimeout) {
		ExecutionGraph graph = task.getTaskToExecute().getVertex().getExecutionGraph();
		OptimisationModelSolution solution = solutions.get(graph);
		if(solution == null) {
			throw new IllegalArgumentException("Please solve the placement problem for this graph first");
		}

		JobVertex jobVertex = task.getTaskToExecute().getVertex().getJobVertex().getJobVertex();
		GeoLocation whereToPlace = solution.getPlacement(jobVertex);
		if(whereToPlace == null) {
			throw new IllegalArgumentException("The placement for this job vertex was not found. This should never happen");
		}

		if(!allInstancesByGeoLocation.containsKey(whereToPlace)) {
			return FutureUtils.completedExceptionally(new NoResourceAvailableException("The geo location specified in " +
				"the placement problem's solution is unknown to this scheduler"));
		}

		ArrayList<TaskManagerLocation> locationsToPlaceIn = new ArrayList<>();

		for (Instance instance : allInstancesByGeoLocation.get(whereToPlace)) {
			locationsToPlaceIn.add(instance.getTaskManagerLocation());
		}

		SimpleSlot slotToUse = super.getFreeSlotForTask(task.getTaskToExecute().getVertex(), locationsToPlaceIn, true);

		if(slotToUse == null) {
			//we weren't able to schedule respecting the model solution, delegate to standard scheduler
			return super.allocateSlot(slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
		}

		return CompletableFuture.completedFuture(slotToUse);
	}

	public Map<GeoLocation, Set<Instance>> getAllInstancesByGeoLocation() {
		return allInstancesByGeoLocation;
	}


	public void addGraphSolution(ExecutionGraph executionGraph, OptimisationModelSolution solution) {
		solutions.put(executionGraph, solution);
	}

	public BandwidthProvider getBandwidthProvider() {
		return bandwidthProvider;
	}

	public void setBandwidthProvider(BandwidthProvider bandwidthProvider) {
		this.bandwidthProvider = bandwidthProvider;
	}
}
