package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBEnv;
import gurobi.GRBException;
import gurobi.GRBLinExpr;
import gurobi.GRBModel;
import gurobi.GRBQuadExpr;
import gurobi.GRBVar;
import org.apache.flink.runtime.clusterframework.types.GeoLocation;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.BandwidthProvider;
import org.apache.flink.runtime.jobmanager.scheduler.StaticBandwidthProvider;
import org.apache.flink.runtime.util.GRBUtils;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OptimisationModel {
	// Constants //
	private final Iterable<JobVertex> vertices;
	private final Set<GeoLocation> locations;
	private final BandwidthProvider bandwidthProvider;
	private final Map<GeoLocation, Integer> slots;
	private final Map<JobVertex, GeoLocation> placedVertices;


	// Variables //
	private final Map<JobVertex, GRBVar> parallelism = new HashMap<>();
	private final TwoKeysMap<JobVertex, GeoLocation, GRBVar> placement = new TwoKeysMultiMap<>();
	private GRBEnv grbEnv;
	private GRBModel model;


	// Objective //
	private GRBVar networkCost;
	private GRBVar executionSpeed;

	// Weights //
	private OptimisationModelParameters parameters;

	public OptimisationModel(Collection<JobVertex> vertices,
							 Set<GeoLocation> locations,
							 Map<JobVertex, GeoLocation> placedVertices,
							 BandwidthProvider bandwidthProvider,
							 Map<GeoLocation, Integer> slots,
							 OptimisationModelParameters parameters) throws GRBException {

		this.grbEnv = new GRBEnv();
		this.model = new GRBModel(grbEnv);

		this.vertices = Preconditions.checkNotNull(vertices);
		this.locations = Preconditions.checkNotNull(locations);

		if(bandwidthProvider != null) {
			this.bandwidthProvider = bandwidthProvider;
		} else {
			this.bandwidthProvider = new StaticBandwidthProvider(new TwoKeysMultiMap<>());
		}

		this.placedVertices = Preconditions.checkNotNull(placedVertices);

		this.slots = slots;

		this.parameters = parameters;

		//double the time spent on heuristics
		model.set(GRB.DoubleParam.Heuristics, 0.1);

		model.set(GRB.DoubleParam.TimeLimit, this.parameters.getTimeForEachTaskBeforeHeuristicSolution() * vertices.size());

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
		executionSpeed = model.addVar(0, GRB.INFINITY, parameters.getExecutionSpeedWeight(), GRB.CONTINUOUS, "execution_speed");
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

	private String getVariableString(String variableName, Object... params) {
		StringBuilder out = new StringBuilder(variableName);
		for (Object param : params) {
			out.append("_");
			if (param.toString().length() >= 100) {
				out.append(param.toString(), 0, 100);
			} else {
				out.append(param.toString());
			}

		}
		return out.toString();
	}

	/**
	 * max parallelism is -1f for unbounded values. changing to GRB.Infinity to solve the model
	 */
	private double getMaxParallelism(JobVertex vertex) {
		double maxParallelism = vertex.getMaxParallelism();
		if(maxParallelism <= 0) {
			maxParallelism = vertex.getParallelism();
		}
		return maxParallelism;
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
			expr.addTerm(vertex.getWeight(), parallelism.get(vertex));
		}
		return expr;
	}

	public OptimisationModelSolution optimize() throws GRBException {
		model.optimize();
		return OptimisationModelSolution.fromSolvedModel(model, placement, parallelism, executionSpeed, networkCost);
	}

	public String solutionString() {
		try {
			if (!isSolved()) {
				return "Solve the model first";
			}
		} catch (GRBException e) {
			e.printStackTrace();
		}

		String out = "";
		try {
			out += "\n------ SPLITTING ARRAY ------\n";
			out += GRBUtils.mapToString(model, parallelism);

			out += "\n\n------ PLACEMENT MATRIX ------";
			out += "\nproblem[tasks][sites] =\n\n";
			out += GRBUtils.twoKeysMapToString(model, placement);

			out += "\n------ EXECUTION SPEED ------ \n";
			out += executionSpeed.get(GRB.DoubleAttr.X);

			out += "\n\n------ NETWORK COST ------ \n";
			out += networkCost.get(GRB.DoubleAttr.X);

			out += "\n\n------ OBJECTIVE ------ \n";
			out += executionSpeed.get(GRB.DoubleAttr.X) + " * " + parameters.getExecutionSpeedWeight() + " + " + networkCost.get(GRB.DoubleAttr.X) + " * " + parameters.getNetworkCostWeight() + " = " + model.get(GRB.DoubleAttr.ObjVal);

			out += "\n\n------ MODEL RUNTIME ------ \n";
			out += model.get(GRB.DoubleAttr.Runtime) + " s";

		} catch (GRBException e) {
			e.printStackTrace();
		}

		return out;
	}

	public boolean isSolved() throws GRBException {
		return GRBUtils.isSolved(this.model);
	}
}
