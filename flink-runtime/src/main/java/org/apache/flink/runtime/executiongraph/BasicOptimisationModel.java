package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBException;
import gurobi.GRBLinExpr;
import gurobi.GRBQuadExpr;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.BandwidthProvider;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class BasicOptimisationModel extends OptimisationModel {
	public BasicOptimisationModel(Collection<JobVertex> vertices, Set<GeoLocation> locations, Map<JobVertex, GeoLocation> placedVertices, BandwidthProvider bandwidthProvider, Map<GeoLocation, Integer> slots, OptimisationModelParameters parameters) throws GRBException {
		super(vertices, locations, placedVertices, bandwidthProvider, slots, parameters);
	}

	public void init() throws GRBException{
		addPlacementVariables();
		addParallelismVariables();

		addNetworkCostVariable();
		addExecutionSpeedVariable();

		addTaskAllocationConstraint();

		if(parameters.isSlotSharingEnabled()) {
			addSharingSlotOverflowConstraint();
		} else {
			addNoSharingSlotOverflowConstraint();
		}
	}

	private void addPlacementVariables() throws GRBException {
		for (JobVertex jv : vertices) {
			boolean isPlaced = placedVertices.containsKey(jv);
			for (GeoLocation gl : locations) {
				if (isPlaced) {
					//placed vertices' placement variables are static: 1 or 0
					if (placedVertices.get(jv).equals(gl)) {
						placement.put(jv, gl, model.addVar(1, 1, 0.0, GRB.BINARY, getVariableString("placement",jv, gl)));
					} else {
						placement.put(jv, gl, model.addVar(0, 0, 0.0, GRB.BINARY, getVariableString("placement",jv, gl)));
					}
				} else {
					//non-placed vertices' placement variables are free
					placement.put(jv, gl, model.addVar(0, 1, 0.0, GRB.BINARY, getVariableString("placement",jv, gl)));
				}
			}
		}
	}

	private void addParallelismVariables() throws GRBException {
		for (JobVertex jv : vertices) {
			parallelism.put(jv, model.addVar(1d, getMaxParallelism(jv), 0.0, GRB.INTEGER, getVariableString("placement",jv)));
		}
	}

	private void addNetworkCostVariable() throws GRBException {
		networkCost = model.addVar(0, GRB.INFINITY, parameters.getNetworkCostWeight(), GRB.CONTINUOUS, "network_cost");
		model.addQConstr(networkCost, GRB.EQUAL, makeNetworkCostExpression(), "network_cost");
	}

	private void addExecutionSpeedVariable() throws GRBException {
		executionSpeed = model.addVar(-GRB.INFINITY, 0, parameters.getExecutionSpeedWeight(), GRB.CONTINUOUS, "execution_speed");
		model.addConstr(executionSpeed, GRB.EQUAL, makeExecutionSpeedExpression(), "execution_speed");
	}

	/**
	 * The sum of all the allocation variables of each
	 * vertex equals 1. So each vertex is allocated once and
	 * only once
	 */
	private void addTaskAllocationConstraint() throws  GRBException {
		for (JobVertex vertex : vertices) {
			String name = getVariableString("sum_allocated_partitions_", vertex);
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
	private void addSharingSlotOverflowConstraint() throws GRBException {
		for (GeoLocation location : locations) {
			for (JobVertex vertex : vertices) {
				String name = getVariableString("allocated_subtasks_", vertex, location);
				GRBQuadExpr lhs = new GRBQuadExpr();
				lhs.addTerm(1d, placement.get(vertex, location), parallelism.get(vertex));
				model.addQConstr(lhs, GRB.LESS_EQUAL, slots.get(location), name);
			}
		}
	}

	/**
	 * The sum of all task vertices placed at each location does
	 * not exceed the slots available there
	 */
	private void addNoSharingSlotOverflowConstraint() throws GRBException {
		for (GeoLocation location : locations) {
			String name = getVariableString("allocated_subtasks_", location);
			GRBQuadExpr lhs = new GRBQuadExpr();
			for (JobVertex vertex : vertices) {
				lhs.addTerm(1d, placement.get(vertex, location), parallelism.get(vertex));
			}
			model.addQConstr(lhs, GRB.LESS_EQUAL, slots.get(location), name);
		}
	}

	private GRBQuadExpr makeNetworkCostExpression() {
		GRBQuadExpr expr = new GRBQuadExpr();

		for (JobVertex vertex : vertices) {
			if (!vertex.isInputVertex()) {
				for (JobEdge jobEdge : vertex.getInputs()) {
					//for all the edges
					for (GeoLocation locationFrom: locations) {
						for (GeoLocation locationTo : locations) {
							//add to the network cost the edge, if source and destinations are not placed in the same site
							if(!locationFrom.equals(locationTo)) {
								double bandwidth;
								if(bandwidthProvider.hasBandwidth(locationFrom, locationTo)) {
									bandwidth = bandwidthProvider.getBandwidth(locationFrom, locationTo);
								} else {
									bandwidth = 1;
								}
								expr.addTerm((1 / bandwidth) * jobEdge.getWeight(), placement.get(vertex, locationTo), placement.get(jobEdge.getSource().getProducer(), locationFrom));
							}
						}
					}
				}
			}
		}

		return expr;
	}

	private GRBLinExpr makeExecutionSpeedExpression() {
		GRBLinExpr expr = new GRBLinExpr();
		for (JobVertex vertex : vertices) {
			expr.addTerm(- vertex.getWeight(), parallelism.get(vertex));
		}
		return expr;
	}
}
