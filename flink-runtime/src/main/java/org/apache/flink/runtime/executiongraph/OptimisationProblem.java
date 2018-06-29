package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;

import java.util.Map;
import java.util.Set;

public class OptimisationProblem {
	private Set<JobVertex> vertices;
	private Set<GeoLocation> locations;
	private Map<GeoLocation, Map<GeoLocation, Double>> bandwidths;
	private Map<GeoLocation, Integer> slots;

	public OptimisationProblem(Set<JobVertex> vertices,
							   Map<GeoLocation, Map<GeoLocation, Double>> bandwidths,
							   GeoScheduler scheduler) {

		this.vertices = vertices;
		this.locations = scheduler.getAllInstancesByGeoLocation().keySet();
		this.slots = scheduler.getAvailableSlotsByGeoLocation();
		this.bandwidths = bandwidths;

	}

}
