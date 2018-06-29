package org.apache.flink.runtime.executiongraph;

import gurobi.*;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OptimisationModel {
	private GRBEnv grbEnv;
	private GRBModel model;

	private Set<JobVertex> vertices;
	private Set<GeoLocation> locations;
	Map<GeoLocation, Map<GeoLocation, Double>> bandwidths;
	Map<GeoLocation, Integer> slots;

	private Map<JobVertex, GRBVar> parallelism = new HashMap<>();
	private Map<JobVertex, Map<GeoLocation, GRBVar>> placementByVertex = new HashMap<>();
	private Map<GeoLocation, Map<JobVertex, GRBVar>> placementByLocation = new HashMap<>();

	public OptimisationModel(Set<JobVertex> vertices,
							 Set<GeoLocation> locations,
							 Map<GeoLocation, Map<GeoLocation, Double>> bandwidths,
							 Map<GeoLocation, Integer> slots) throws GRBException {
		grbEnv = new GRBEnv();
		model = new GRBModel(grbEnv);

		this.vertices = vertices;
		this.locations = locations;
		this.bandwidths = bandwidths;

		addPlacementVariables();
	}

	private void addPlacementVariables() throws GRBException {
		for(JobVertex v : vertices) {
			Map<GeoLocation, GRBVar> placementByVertexMap;
			if(placementByVertex.containsKey(v)) {
				placementByVertexMap = placementByVertex.get(v);
			} else {
				placementByVertexMap = new HashMap<>();
				placementByVertex.put(v, placementByVertexMap);
			}
			for(GeoLocation l : locations) {
				GRBVar placementVar = model.addVar(0, 1, 0.0, GRB.BINARY, "placement_" + v + "_" + l);
				placementByVertexMap.put(l, placementVar);

				Map<JobVertex, GRBVar> placementByLocationMap;
				if(placementByLocation.containsKey(l)) {
					placementByLocationMap = placementByLocation.get(l);
				} else {
					placementByLocationMap = new HashMap<>();
					placementByVertex.put(v, placementByVertexMap);
				}

				placementByLocationMap.put(v, placementVar);
			}
		}
	}


}
