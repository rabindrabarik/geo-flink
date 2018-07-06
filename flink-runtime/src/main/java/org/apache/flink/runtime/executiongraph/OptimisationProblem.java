package org.apache.flink.runtime.executiongraph;

import gurobi.GRBException;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.types.TwoKeysMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class OptimisationProblem {
	private Set<JobVertex> vertices;
	private Set<GeoLocation> locations;
	TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths;
	private Map<GeoLocation, Integer> slots;
	private OptimisationModel model;
	private GeoScheduler scheduler;

	public OptimisationProblem(Set<JobVertex> vertices,
							   TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths,
							   GeoScheduler scheduler) {

		this.vertices = vertices;
		this.locations = scheduler.getAllInstancesByGeoLocation().keySet();
		this.slots = scheduler.calculateAvailableSlotsByGeoLocation();
		this.bandwidths = bandwidths;
		this.scheduler = scheduler;

		try {
			initialiseModel();
		} catch (GRBException e) {
			e.printStackTrace();
		}

	}

	private void initialiseModel() throws GRBException {
		model = new OptimisationModel(vertices, locations, new HashMap<>(), bandwidths, slots, scheduler, 0.5d, 0.5d);
	}

	public CompletableFuture<OptimisationproblemSolution> solve() {
		return null;
	}

}
