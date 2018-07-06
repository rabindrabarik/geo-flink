package org.apache.flink.runtime.executiongraph;

import gurobi.*;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OptimisationModel {
	private final GeoScheduler scheduler;

	private GRBEnv grbEnv;
	private GRBModel model;

	// Constants //
	private final Set<JobVertex> vertices;
	private final Set<GeoLocation> locations;
	private final TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths;
	private final Map<GeoLocation, Integer> slots;

	// Variables //
	private final Map<JobVertex, GRBVar> parallelism = new HashMap<>();
	private final TwoKeysMap<JobVertex, GeoLocation, GRBVar> placement = new TwoKeysMultiMap<>();

	public OptimisationModel(Set<JobVertex> vertices,
							 Set<GeoLocation> locations,
							 TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths,
							 Map<GeoLocation, Integer> slots,
							 GeoScheduler scheduler) throws GRBException {
		this.scheduler = scheduler;

		this.grbEnv = new GRBEnv();
		this.model = new GRBModel(grbEnv);

		this.vertices = vertices;
		this.locations = locations;
		this.bandwidths = bandwidths;

		this.slots = scheduler.calculateAvailableSlotsByGeoLocation();

		addPlacementVariables();
		addParallelismVariables();
	}


	private void addPlacementVariables() throws GRBException {
		for(JobVertex jv : vertices) {
			for (GeoLocation gl : locations) {
				placement.put(jv, gl, model.addVar(0,1, 0.0, GRB.BINARY, "placement_" + jv + "_" + gl));
			}
		}
	}

	private void addParallelismVariables() throws GRBException {
		for(JobVertex jv : vertices) {
			parallelism.put(jv, model.addVar(1d / jv.getMaxParallelism(), 1d, 0.0, GRB.CONTINUOUS, "splitting_" + jv));
		}
	}

}
