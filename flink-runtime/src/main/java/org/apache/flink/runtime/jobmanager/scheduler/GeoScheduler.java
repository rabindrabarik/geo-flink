package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.taskmanager.GeoTaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import java.util.*;
import java.util.concurrent.Executor;

/**
 * This scheduler allows Flink to make better allocation decisions when the task managers are executed in geo-distributed data centers
 */
public class GeoScheduler extends Scheduler {

	private Map<GeoLocation, Set<Instance>> allInstancesByGeoLocation = new HashMap<>();

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
	public Map<GeoLocation, Integer> getAvailableSlotsByGeoLocation() {
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

	/**
	 * Prepare for scheduling an {@link ExecutionGraph} for execution. Each request to allocateSlot for an {@link ExecutionVertex}
	 * in this {@link ExecutionGraph} will return the decided slot.
	 */
	public void prepareForScheduling(ExecutionGraph graph) {
		Collection<JobVertex> vertices = new ArrayList<>();
		Collection<JobEdge> edges = new ArrayList<>();

		for (JobVertex vertex : vertices) {

		}

		for (JobEdge edge : edges) {

		}

		gatherJobVerticesAndEdges(graph.getVerticesTopologically(), vertices, edges);
	}

	private void gatherJobVerticesAndEdges(
		Iterable<ExecutionJobVertex> executionJobVertices,
		Collection<JobVertex> vertices,
		Collection<JobEdge> edges) {
		for (ExecutionJobVertex executionJobVertex : executionJobVertices) {
			JobVertex vertex = executionJobVertex.getJobVertex();
			vertices.add(vertex);
			edges.addAll(vertex.getInputs());
		}
	}
}
