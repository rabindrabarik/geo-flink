package org.apache.flink.runtime.executiongraph;

import gurobi.GRBException;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class OptimisationProblem {
	private Set<JobVertex> vertices;
	private Set<GeoLocation> locations;
	Map<GeoLocation, Map<GeoLocation, Double>> bandwidths;
	private Map<GeoLocation, Integer> slots;
	private OptimisationModel model;

	public OptimisationProblem(Set<JobVertex> vertices,
							   Map<GeoLocation, Map<GeoLocation, Double>> bandwidths,
							   GeoScheduler scheduler) {

		this.vertices = vertices;
		this.locations = scheduler.getAllInstancesByGeoLocation().keySet();
		this.slots = scheduler.calculateAvailableSlotsByGeoLocation();
		this.bandwidths = bandwidths;

		try {
			initialiseModel();
		} catch (GRBException e) {
			e.printStackTrace();
		}

	}

	private void initialiseModel() throws GRBException {
		model = new OptimisationModel(vertices, locations, bandwidths, slots);
	}

	public CompletableFuture<OptimisationproblemSolution> solve() {
		return null;
	}

}
