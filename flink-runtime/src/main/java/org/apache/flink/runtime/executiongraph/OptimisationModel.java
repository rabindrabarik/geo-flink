package org.apache.flink.runtime.executiongraph;

import gurobi.GRB;
import gurobi.GRBEnv;
import gurobi.GRBException;
import gurobi.GRBExpr;
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

public abstract class OptimisationModel {
	// Constants //
	protected final Iterable<JobVertex> vertices;
	protected final Set<GeoLocation> locations;
	protected final BandwidthProvider bandwidthProvider;
	protected final Map<GeoLocation, Integer> slots;
	protected final Map<JobVertex, GeoLocation> placedVertices;


	// Variables //
	protected final Map<JobVertex, GRBVar> parallelism = new HashMap<>();
	protected final TwoKeysMap<JobVertex, GeoLocation, GRBVar> placement = new TwoKeysMultiMap<>();
	protected GRBEnv grbEnv;
	protected GRBModel model;


	// Objective //
	protected GRBVar networkCost;
	protected GRBVar executionSpeed;

	// Weights //
	protected OptimisationModelParameters parameters;

	/**
	 * This constructor:
	 * <ul>
	 *     <li>Creates the model</li>
	 *     <li>Initialises vertices, locations, placedVertices, bandwidthProvider, slots and parameters</li>
	 *     <li>Creates variables for placement and splitting</li>
	 *     <li>Creates the variables for network cost and execution speed as specified in the {@link #addNetworkCostVariable()} and {@link #addExecutionSpeedVariable()} abstract methods</li>
	 * </ul>
	 * All the rest is left to implementers.
	 * */
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

		if (bandwidthProvider != null) {
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

		init();
	}

	protected void addPlacementVariables() throws GRBException {
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

	protected void addParallelismVariables() throws GRBException {
		for (JobVertex jv : vertices) {
			parallelism.put(jv, model.addVar(1d, getMaxParallelism(jv), 0.0, GRB.INTEGER, getVariableString("placement",jv)));
		}
	}

	public abstract void init() throws GRBException;

	protected String getVariableString(String variableName, Object... params) {
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
	protected double getMaxParallelism(JobVertex vertex) {
		double maxParallelism = vertex.getMaxParallelism();
		if(maxParallelism <= 0) {
			maxParallelism = vertex.getParallelism();
		}
		return maxParallelism;
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

	private void addNetworkCostVariable() throws GRBException {
		networkCost = model.addVar(0, GRB.INFINITY, parameters.getNetworkCostWeight(), GRB.CONTINUOUS, "network_cost");

		double automaticNetworkCostWeight = maxExecutionTime() / maxNetworkCost();

		GRBQuadExpr weightedNetworkCostExpression = new GRBQuadExpr();

		weightedNetworkCostExpression.multAdd(automaticNetworkCostWeight, makeNetworkCostExpression());

		model.addQConstr(networkCost, GRB.EQUAL, weightedNetworkCostExpression, "network_cost");
	}

	protected double maxExecutionTime() {
		double out = 0;
		for (JobVertex vertex : vertices) {
			out += vertex.getWeight() * getMaxParallelism(vertex);
		}
		return out;
	}

	protected double maxNetworkCost() {
		double out = 0;
		for (JobVertex destination : vertices) {
			for (JobEdge jobEdge : destination.getInputs()) {
				out += jobEdge.getWeight() * locations.size();
			}
		}
		return out;
	}

	private void addExecutionSpeedVariable() throws GRBException {
		executionSpeed = model.addVar(-GRB.INFINITY, 0, parameters.getExecutionSpeedWeight(), GRB.CONTINUOUS, "execution_speed");
		model.addConstr(executionSpeed, GRB.EQUAL, makeExecutionSpeedExpression(), "execution_speed");
	}

	protected abstract GRBQuadExpr makeNetworkCostExpression() throws GRBException;

	protected abstract GRBLinExpr makeExecutionSpeedExpression() throws GRBException;
}
