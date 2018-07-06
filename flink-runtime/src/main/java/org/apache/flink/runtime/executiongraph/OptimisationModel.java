package org.apache.flink.runtime.executiongraph;

import gurobi.*;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.GeoScheduler;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OptimisationModel {
	private final GeoScheduler scheduler;

	// Constants //
	private final Set<JobVertex> vertices;
	private final Set<GeoLocation> locations;
	private final TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths;
	private final Map<GeoLocation, Integer> slots;
	private final Map<JobVertex, GeoLocation> placedVertices;


	// Variables //
	private final Map<JobVertex, GRBVar> parallelism = new HashMap<>();
	private final TwoKeysMap<JobVertex, GeoLocation, GRBVar> placement = new TwoKeysMultiMap<>();
	private GRBEnv grbEnv;
	private GRBModel model;


	// Objective //
	private GRBVar networkCost;
	private GRBVar executionTime;

	// Weights //
	private double networkCostWeight;
	private double executionTimeWeight;

	public OptimisationModel(Set<JobVertex> vertices,
							 Set<GeoLocation> locations,
							 Map<JobVertex, GeoLocation> placedVertices,
							 TwoKeysMap<GeoLocation, GeoLocation, Double> bandwidths,
							 Map<GeoLocation, Integer> slots,
							 GeoScheduler scheduler,
							 double networkCostWeight,
							 double executionTimeWeight) throws GRBException {
		this.scheduler = Preconditions.checkNotNull(scheduler);

		this.grbEnv = new GRBEnv();
		this.model = new GRBModel(grbEnv);

		this.vertices = Preconditions.checkNotNull(vertices);
		this.locations = Preconditions.checkNotNull(locations);
		this.bandwidths = Preconditions.checkNotNull(bandwidths);
		this.placedVertices = Preconditions.checkNotNull(placedVertices);

		this.slots = scheduler.calculateAvailableSlotsByGeoLocation();

		this.networkCostWeight = networkCostWeight;
		this.executionTimeWeight = executionTimeWeight;

		addPlacementVariables();
		addParallelismVariables();

		addNetworkCostVariable();
		addExecutionTimeVariable();

		addTaskAllocationConstraint();
		addSlotOverflowConstraint();
	}

	private void addPlacementVariables() throws GRBException {
		for (JobVertex jv : vertices) {
			boolean isPlaced = placedVertices.containsKey(jv);
			for (GeoLocation gl : locations) {
				if (isPlaced) {
					//placed vertices' placement variables are static: 1 or 0
					if (placedVertices.get(jv).equals(gl)) {
						placement.put(jv, gl, model.addVar(1, 1, 0.0, GRB.BINARY, "placement_" + jv + "_" + gl));
					} else {
						placement.put(jv, gl, model.addVar(0, 0, 0.0, GRB.BINARY, "placement_" + jv + "_" + gl));
					}
				} else {
					//non-placed vertices' placement variables are free
					placement.put(jv, gl, model.addVar(0, 1, 0.0, GRB.BINARY, "placement_" + jv + "_" + gl));
				}
			}
		}
	}

	private void addParallelismVariables() throws GRBException {
		for (JobVertex jv : vertices) {
			parallelism.put(jv, model.addVar(1d / jv.getMaxParallelism(), 1d, 0.0, GRB.CONTINUOUS, "splitting_" + jv));
		}
	}

	private void addNetworkCostVariable() throws GRBException {
		networkCost = model.addVar(0, GRB.INFINITY, networkCostWeight, GRB.CONTINUOUS, "network_cost");
		model.addQConstr(networkCost, GRB.EQUAL, makeNetworkCostExpression(), "network_cost");
	}

	private void addExecutionTimeVariable() throws GRBException {
		executionTime = model.addVar(-GRB.INFINITY, 0, executionTimeWeight, GRB.CONTINUOUS, "execution_time");
		model.addConstr(executionTime, GRB.EQUAL, makeExecutionTimeExpression(), "execution_time");
	}

	/**
	 * The sum of all the allocation variables of each
	 * vertex equals 1. So each vertex is allocated once and
	 * only once
	 */
	private void addTaskAllocationConstraint() throws  GRBException {
		for (JobVertex vertex : vertices) {
			String name = "sum_allocated_partitions_" + vertex;
			GRBLinExpr lhs = new GRBLinExpr();
			for (GeoLocation location : locations) {
				lhs.addTerm(1d, placement.get(vertex, location));
			}
			model.addConstr(lhs, GRB.EQUAL, 1d, name);
		}
	}

	/**
	 * The number of vertices placed at each location does
	 * not exceed the slots available there
	 */
	private void addSlotOverflowConstraint () throws GRBException {
		for (GeoLocation location : locations) {
			String name = "allocated_subtasks_" + location;
			GRBQuadExpr lhs = new GRBQuadExpr();
			for (JobVertex vertex : vertices) {
				lhs.addTerm(vertex.getMaxParallelism(), placement.get(vertex, location), parallelism.get(vertex));
			}
			model.addQConstr(lhs, GRB.LESS_EQUAL, slots.get(location), name);
		}
	}

	private GRBQuadExpr makeNetworkCostExpression() {
		GRBQuadExpr rhs = new GRBQuadExpr();

		for (JobVertex vertex : vertices) {
			if (!vertex.isInputVertex()) {
				for (JobEdge jobEdge : vertex.getInputs()) {
					//for all the edges
					for (GeoLocation locationFrom: locations) {
						for (GeoLocation locationTo : locations) {
							//add to the network cost the edge, if source and destinations are not placed in the same site
							if(!locationFrom.equals(locationTo)) {
								//TODO: add edge weight
								rhs.addTerm((1 / bandwidths.get(locationFrom, locationTo)), placement.get(vertex, locationTo), placement.get(jobEdge.getSource().getProducer(), locationFrom));
							}
						}
					}
				}
			}
		}

		return rhs;
	}

	private GRBLinExpr makeExecutionTimeExpression() {
		GRBLinExpr rhs = new GRBLinExpr();
		for (JobVertex vertex : vertices) {
			//TODO add execution time
			rhs.addTerm(vertex.getMaxParallelism(), parallelism.get(vertex));
		}
		return rhs;
	}

}
